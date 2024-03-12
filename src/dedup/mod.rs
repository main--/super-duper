use std::collections::BinaryHeap;
use std::collections::hash_map::Entry;
use std::ffi::OsString;
use std::{mem, iter};
use std::path::{PathBuf, Path};

use blake3::Hash;
use color_eyre::eyre::OptionExt;
use fnv::{FnvHashMap, FnvHashSet};
use indicatif::{ProgressBar, ProgressStyle};
use petgraph::Direction;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::{EdgeRef, IntoNodeReferences};
use rusqlite::Connection;

#[derive(Debug)]
enum Node {
    File {
        name: OsString,
        size: u64,
        hash: Hash,
    },
    Dir {
        name: OsString,
        cumulative_size: u64,
    },
    Tombstone,
    MergedDir {
        names: Vec<OsString>,
        dedup_cumulative_size: u64,
        dup_cumulative_size: u64,
    },
}
impl Node {
    fn take_dir_names(&mut self) -> Box<dyn Iterator<Item=OsString> + '_> {
        match self {
            Node::Dir { name, .. } => Box::new(iter::once(mem::take(name))),
            Node::MergedDir { names, .. } => Box::new(names.drain(..)),
            _ => Box::new(iter::empty()),
        }
    }
    fn dedup_size(&self) -> u64 {
        match self {
            &Node::MergedDir { dedup_cumulative_size, .. } => dedup_cumulative_size,
            &Node::Dir { cumulative_size, .. } => cumulative_size,
            _ => panic!("called dedup_size on non-dir"),
        }
    }
    fn dup_size(&self) -> u64 {
        match self {
            &Node::MergedDir { dup_cumulative_size, .. } => dup_cumulative_size,
            &Node::Dir { cumulative_size, .. } => cumulative_size,
            _ => panic!("called dup_size on non-dir"),
        }
    }
}
#[derive(Debug, Clone, Copy)]
enum Edge {
    Contains,
    ContainsTransitively,
}

#[derive(Default)]
struct Deduper {
    dirs: FnvHashMap<String, FnvHashMap<PathBuf, NodeIndex>>,
    files: FnvHashMap<Hash, NodeIndex>,
    graph: DiGraph<Node, Edge>,
}

impl Deduper {
    fn import(&mut self, conn: &mut Connection) -> color_eyre::Result<()> {
        let mut stmt = conn.prepare("SELECT prefix, path, size, hash FROM files")?;
        let mut  rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let prefix = row.get_ref(0)?.as_str()?;
            let path = row.get_ref(1)?.as_str()?;
            let size: u64 = row.get(2)?;
            let hash: [u8; blake3::OUT_LEN] = row.get(3)?;
            let hash = Hash::from_bytes(hash);

            let path = &Path::new(path);
            let dir = match path.parent() {
                Some(x) if !x.as_os_str().is_empty() => x,
                _ => {
                    tracing::debug!(path = %path.display(), "ignoring top-level file");
                    continue;
                }
            };
            let dir_node = self.mkdir_p(prefix, dir)?;

            let file_node = match self.files.entry(hash) {
                Entry::Occupied(x) => *x.get(),
                Entry::Vacant(v) => {
                    let id = self.graph.add_node(Node::File { name: path.file_name().ok_or_eyre("empty filename")?.to_owned(), size, hash });
                    v.insert(id);
                    id
                }
            };

            create_transitive_edges(&mut self.graph, file_node, dir_node);
        }

        // calculate cumulative sizes
        for ni in self.graph.node_indices() {
            if let &Node::File { size, .. } = &self.graph[ni] {
                let mut neighbors = self.graph.neighbors(ni).detach();
                while let Some(e) = neighbors.next_edge(&mut self.graph) {
                    if matches!(self.graph[e], Edge::ContainsTransitively) {
                        let (_, target) = self.graph.edge_endpoints(e).unwrap();
                        match &mut self.graph[target] {
                            Node::Dir { cumulative_size, .. } => {
                                *cumulative_size += size;
                            }
                            _ => unreachable!(),
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// like "mkdir -p" this creates a directory and parent directories as needed
    fn mkdir_p(&mut self, prefix: &str, dir: &Path) -> color_eyre::Result<NodeIndex> {
        let map = match self.dirs.get_mut(prefix) {
            // fast path
            Some(x) => x,
            None => self.dirs.entry(prefix.to_owned()).or_default(),
        };

        mkdir_p(map, &mut self.graph, prefix, dir)
    }

    fn dedup(&mut self) -> color_eyre::Result<()> {
        // basic idea:
        // - visit all nodes
        // - if a node has multiple incoming edges, see if we can merge the parent directories
        // - merge by replacing the first parent with the merged node, and the second parent with a tombstone (rerouting all edges)
        // - iterate until all nodes have been visited

        // start out by making this list to visit all nodes at least once
        // if they're not ready to merge yet on our first visit, no problem - we will make sure to visit them again
        let mut nodes_to_visit: Vec<_> = self.graph.node_indices().collect();

        let progress = ProgressBar::new(nodes_to_visit.len() as u64);
        progress.set_style(ProgressStyle::with_template("{wide_bar} {human_pos}/{human_len} ETA {eta}")?);

        let mut unmergeable_pairs = FnvHashMap::default();
        while let Some(node) = nodes_to_visit.pop() {
            progress.inc(1);

            let contains_edges = self.graph.edges(node).filter(|e| matches!(e.weight(), Edge::Contains)).map(|e| e.target());

            if contains_edges.clone().count() <= 1 {
                // nothing to merge here
                continue;
            }

            // try all pairs of parents (quadratic, but amortizes over files in a dir)
            let parents: FnvHashSet<_> = contains_edges.collect();
            for &p1 in &parents {
                if matches!(self.graph[p1], Node::Tombstone) {
                    // p1 was merged in a previous iteration
                    continue;
                }

                for &p2 in &parents {
                    if p2 <= p1 {
                        // we arbitrarily decide that the lower index should be the left hand side here (i.e. p1)
                        continue;
                    }
                    if matches!(self.graph[p2], Node::Tombstone) {
                        // p2 was merged in a previous iteration
                        continue;
                    }
                    if self.graph.contains_edge(p1, p2) || self.graph.contains_edge(p2, p1) {
                        // these dirs are nested inside each other, so we cannot merge them
                        continue;
                    }

                    let unmergeable = unmergeable_pairs.entry((p1, p2));
                    match unmergeable {
                        Entry::Occupied(_) => {
                            // we tried this already, so no need to try again
                            continue;
                        }
                        Entry::Vacant(v) => {
                            let p1_children = self.get_children_set(p1);
                            let p2_children = self.get_children_set(p2);

                            // TODO: fuzzy matching
                            if p1_children == p2_children {
                                // mergeable!!

                                let mut p1_node = mem::replace(&mut self.graph[p1], Node::Tombstone);
                                let mut p2_node = mem::replace(&mut self.graph[p2], Node::Tombstone);

                                self.graph[p1] = Node::MergedDir {
                                    names: p1_node.take_dir_names().chain(p2_node.take_dir_names()).collect(),
                                    dedup_cumulative_size: p1_node.dedup_size(),
                                    dup_cumulative_size: p1_node.dup_size() + p2_node.dup_size(),
                                };

                                let p2_incoming: Vec<_> = self.graph.edges_directed(p2, Direction::Incoming).map(|e| (e.id(), e.source(), *e.weight())).collect();
                                let p2_outgoing: Vec<_> = self.graph.edges_directed(p2, Direction::Outgoing).map(|e| (e.id(), e.target(), *e.weight())).collect();
                                for &(idx, _, _) in p2_incoming.iter().chain(&p2_outgoing) {
                                    self.graph.remove_edge(idx);
                                }
                                for (_, src, w) in p2_incoming {
                                    if matches!(w, Edge::Contains) {
                                        // make sure to visit src again in case this changes anything for them
                                        nodes_to_visit.push(src);
                                        progress.inc_length(1);
                                    }
                                    self.graph.add_edge(src, p1, w);
                                }
                                for (_, tar, w) in p2_outgoing {
                                    self.graph.add_edge(p1, tar, w);
                                }

                                if self.graph.node_count() < 100 {
                                    tracing::trace!(a = ?format_args!("{:#?}", petgraph::dot::Dot::new(&self.graph)));
                                }
                            } else {
                                v.insert(()); // mark as unmergeable
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn get_children_set(&self, n: NodeIndex) -> FnvHashSet<NodeIndex> {
        let mut set = FnvHashSet::default();
        for e in self.graph.edges_directed(n, Direction::Incoming).filter(|e| matches!(e.weight(), Edge::ContainsTransitively)) {
            if let Node::File { .. } = &self.graph[e.source()] {
                set.insert(e.source());
            }
        }
        set
    }
}

#[tracing::instrument(skip(dirs, graph, prefix))]
fn mkdir_p(
    dirs: &mut FnvHashMap<PathBuf, NodeIndex>,
    graph: &mut DiGraph<Node, Edge>,
    prefix: &str,
    dir: &Path,
) -> color_eyre::Result<NodeIndex> {
    if let Some(&x) = dirs.get(dir) {
        return Ok(x);
    }

    let node = graph.add_node(Node::Dir { name: dir.file_name().unwrap_or_default().to_owned(), cumulative_size: 0 });
    match dir.parent() {
        Some(parent) => {
            let parent = mkdir_p(dirs, graph, prefix, parent)?;
            create_transitive_edges(graph, node, parent);
        }
        _ => {
            // root dir
        }
    }

    dirs.insert(dir.to_owned(), node);

    Ok(node)
}

fn create_transitive_edges(graph: &mut DiGraph<Node, Edge>, node: NodeIndex, parent: NodeIndex) {
    graph.add_edge(node, parent, Edge::Contains);
    graph.add_edge(node, parent, Edge::ContainsTransitively);
    let mut neighbors = graph.neighbors(parent).detach();
    while let Some(e) = neighbors.next_edge(graph) {
        if matches!(graph[e], Edge::ContainsTransitively) {
            let (_, target) = graph.edge_endpoints(e).unwrap();
            graph.add_edge(node, target, Edge::ContainsTransitively);
        }
    }
}

pub fn dedup(conn: &mut Connection) -> color_eyre::Result<()> {
    let mut deduper = Deduper::default();
    deduper.import(conn)?;

    if deduper.graph.node_count() < 100 {
        tracing::trace!(a = ?format_args!("{:#?}", petgraph::dot::Dot::new(&deduper.graph)));
    }

    for (k, v) in &deduper.dirs {
        let ni = *v.get(Path::new("")).unwrap();
        match deduper.graph[ni] {
            Node::Dir { cumulative_size, .. } => println!("{k} has {}", indicatif::HumanBytes(cumulative_size)),
            _ => todo!(),
        };
    }

    deduper.dedup()?;

    if deduper.graph.node_count() < 100 {
        tracing::trace!(a = ?format_args!("{:#?}", petgraph::dot::Dot::new(&deduper.graph)));
    }

    struct Merged<'a> {
        ni: NodeIndex,
        names: &'a Vec<OsString>,
        dedup_cumulative_size: &'a u64,
        dup_cumulative_size: &'a u64,
    }
    impl Merged<'_> {
        fn saved_bytes(&self) -> u64 {
            self.dup_cumulative_size - self.dedup_cumulative_size
        }
    }
    impl PartialEq for Merged<'_> {
        fn eq(&self, other: &Self) -> bool {
            self.saved_bytes() == other.saved_bytes()
        }
    }
    impl Eq for Merged<'_> {}
    impl PartialOrd for Merged<'_> {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            self.saved_bytes().partial_cmp(&other.saved_bytes())
        }
    }
    impl Ord for Merged<'_> {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.saved_bytes().cmp(&other.saved_bytes())
        }
    }
    let mut heap = BinaryHeap::new();
    for (ni, n) in deduper.graph.node_references() {
        if let Node::MergedDir { names, dedup_cumulative_size, dup_cumulative_size } = n {
            // must be contained in multiple places
            heap.push(Merged {
                ni,
                names,
                dedup_cumulative_size,
                dup_cumulative_size,
            })
        }
    }

    let top_10_duplications = iter::from_fn(|| heap.pop()).take(10);
    for m in top_10_duplications {
        let names = m.names.iter().map(|x| x.to_string_lossy()).collect::<Vec<_>>().join(", ");
        println!("Merging ({names}) saves {} bytes", indicatif::HumanBytes(m.saved_bytes()));
    }

    Ok(())
}
