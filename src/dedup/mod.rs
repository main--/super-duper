use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::collections::hash_map::Entry;
use std::ffi::{OsString, OsStr};
use std::fmt::Debug;
use std::{mem, iter};
use std::path::{PathBuf, Path};

use blake3::Hash;
use color_eyre::eyre::OptionExt;
use fnv::{FnvHashMap, FnvHashSet};
use indicatif::{ProgressBar, ProgressStyle};
use petgraph::Direction;
use petgraph::algo::DfsSpace;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::{EdgeRef, IntoNodeReferences, Dfs, Walker, NodeFiltered};
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
        confl_files: NoDebug<Vec<NodeIndex>>,
    },
}
struct NoDebug<T>(T);
impl<T> Debug for NoDebug<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NoDebug").finish_non_exhaustive()
    }
}
impl Node {
    fn is_tombstone(&self) -> bool { matches!(self, Node::Tombstone) }
    fn take_dir_names(&mut self) -> Box<dyn Iterator<Item=OsString> + '_> {
        match self {
            Node::Dir { name, .. } => Box::new(iter::once(mem::take(name))),
            Node::MergedDir { names, .. } => Box::new(names.drain(..)),
            _ => Box::new(iter::empty()),
        }
    }
    fn dirname(&self) -> Option<&OsStr> {
        match self {
            Node::Dir { name, .. } => Some(name),
            Node::MergedDir { names, .. } => names.get(0).map(OsString::as_os_str),
            _ => None,
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

type Edge = ();

#[derive(Default)]
struct Deduper {
    dirs: FnvHashMap<String, FnvHashMap<PathBuf, NodeIndex>>,
    files: FnvHashMap<Hash, NodeIndex>,
    graph: DiGraph<Node, Edge>,
}

impl Deduper {
    #[inline(never)]
    fn import(&mut self, conn: &mut Connection) -> color_eyre::Result<()> {
        let mut stmt = conn.prepare("SELECT prefix, path, size, hash FROM files WHERE size > 0")?;
        let mut rows = stmt.query([])?;
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
            let file_name = path.file_name().ok_or_eyre("empty filename")?.to_owned();
            let fu8 = file_name.as_os_str().to_str().ok_or_eyre("non utf8 filename")?;
            if fu8.starts_with("Thumbs") && fu8.ends_with(".db") {
                continue;
            }
            if fu8.starts_with("desktop") && fu8.ends_with(".ini") {
                continue;
            }
            if fu8.ends_with(".pyc") {
                continue;
            }

            let dir_node = self.mkdir_p(prefix, dir)?;

            let file_node = match self.files.entry(hash) {
                Entry::Occupied(x) => *x.get(),
                Entry::Vacant(v) => {
                    let id = self.graph.add_node(Node::File { name: file_name, size, hash });
                    v.insert(id);
                    id
                }
            };

            self.graph.add_edge(dir_node, file_node, ());
        }

        // calculate cumulative sizes
        for ni in self.graph.node_indices() {
            if let &Node::File { size, .. } = &self.graph[ni] {
                let mut todo: Vec<_> = self.graph.neighbors_directed(ni, Direction::Incoming).collect();
                while let Some(p) = todo.pop() {
                    match &mut self.graph[p] {
                        Node::Dir { cumulative_size, .. } => {
                            *cumulative_size += size;
                        }
                        x => unreachable!("{x:?}"),
                    }
                    todo.extend(self.graph.neighbors_directed(p, Direction::Incoming));
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

    #[inline(never)]
    fn dedup(&mut self) -> color_eyre::Result<()> {
        // basic idea:
        // - visit all nodes
        // - if a node has multiple incoming edges, see if we can merge the parent directories
        // - merge by replacing the first parent with the merged node, and the second parent with a tombstone (rerouting all edges)
        // - iterate until all nodes have been visited

        // start out by making this list to visit all nodes at least once
        // if they're not ready to merge yet on our first visit, no problem - we will make sure to visit them again
        let mut nodes_to_visit: VecDeque<_> = self.graph.node_indices().collect();
        let mut space = DfsSpace::new(&self.graph);

        let progress = ProgressBar::new(nodes_to_visit.len() as u64);
        progress.set_style(ProgressStyle::with_template("{wide_bar} {human_pos}/{human_len} ETA {eta}")?);

        let mut checked_pairs = FnvHashSet::default();
        while let Some(node) = nodes_to_visit.pop_front() {
            progress.inc(1);

            if self.graph[node].is_tombstone() {
                // node was merged in a previous iteration
                continue;
            }

            let parents = self.graph.neighbors_directed(node, Direction::Incoming);

            if parents.clone().take(2).count() <= 1 {
                // nothing to merge here
                continue;
            }

            // try all pairs of parents (quadratic, but amortizes over files in a dir)
            let parents: FnvHashSet<_> = parents.collect();
            for &p1 in &parents {
                if self.graph[p1].is_tombstone() {
                    // p1 was merged in a previous iteration
                    continue;
                }

                let mut p1_children = None;

                for &p2 in &parents {
                    if p2 <= p1 {
                        // we arbitrarily decide that the lower index should be the left hand side here (i.e. p1)
                        continue;
                    }
                    if self.graph[p2].is_tombstone() {
                        // p2 was merged in a previous iteration
                        continue;
                    }
                    if petgraph::algo::has_path_connecting(&self.graph, p1, p2, Some(&mut space))
                        || petgraph::algo::has_path_connecting(&self.graph, p2, p1, Some(&mut space)) {
                        // these dirs are nested inside each other, so we cannot merge them
                        continue;
                    }

                    // only check for mergeability if we haven't checked this pair already
                    if checked_pairs.insert((p1, p2)) {
                        let p1_children = p1_children.get_or_insert_with(|| self.get_children_set(p1));
                        let p2_children = &mut self.get_children_set(p2);

                        let common_files = p1_children.intersection(p2_children).count();
                        let p1_only_files = p1_children.len() - common_files;
                        let p2_only_files = p2_children.len() - common_files;
                        let total_files = common_files + p1_only_files + p2_only_files;
                        let pct75 = (total_files * 7) / 8;

                        if (p1_only_files == 0 || p2_only_files == 0) && ((p1_only_files + p2_only_files) < common_files) {
                            // mergeable!!

                            let mut p1_node = mem::replace(&mut self.graph[p1], Node::Tombstone);
                            let mut p2_node = mem::replace(&mut self.graph[p2], Node::Tombstone);

                            self.graph[p1] = Node::MergedDir {
                                names: p1_node.take_dir_names().chain(p2_node.take_dir_names()).collect(),
                                dedup_cumulative_size: p1_node.dedup_size(),
                                dup_cumulative_size: p1_node.dup_size() + p2_node.dup_size(),
                                confl_files: NoDebug(p1_children.symmetric_difference(&p2_children).copied().collect()),
                            };
                            p1_children.extend(p2_children.iter().copied());

                            let mut p2_edges: Vec<_> = self.graph
                                .edges_directed(p2, Direction::Incoming)
                                .map(|e| (e.id(), e.source(), p1))
                                .chain(
                                    self.graph
                                        .edges_directed(p2, Direction::Outgoing)
                                        .map(|e| (e.id(), p1, e.target()))
                                )
                                .collect();
                            p2_edges.sort_unstable_by_key(|&(idx, _, _)| Reverse(idx));
                            for &(idx, _, _) in &p2_edges {
                                self.graph.remove_edge(idx);
                            }
                            for (_, from, to) in p2_edges {
                                if !self.graph.contains_edge(from, to) {
                                    self.graph.add_edge(from, to, ());
                                }
                            }

                            // make sure to visit p1 again in case this changes anything for them
                            // (maybe it allows their parents to merge)
                            nodes_to_visit.push_back(p1);
                            progress.inc_length(1);

                            if self.graph.node_count() < 100 {
                                tracing::trace!(a = ?format_args!("{:#?}", petgraph::dot::Dot::new(&self.graph)));
                            }
                        } else if common_files >= pct75 {
                            for &confl in p1_children.symmetric_difference(&p2_children) {
                                if let Node::File { name, size, hash } = &self.graph[confl] {
                                    let dirname1 = self.get_all_paths(p1).map(|x| x.to_string_lossy().into_owned()).collect::<Vec<_>>().join(", ");
                                    let dirname2 = self.get_all_paths(p2).map(|x| x.to_string_lossy().into_owned()).collect::<Vec<_>>().join(", ");
                                    let has_left = p1_children.contains(&confl);
                                    tracing::info!(?has_left, ?name, ?dirname1, ?dirname2, "confl");
                                }
                            }
                            tracing::info!("");
                        }
                    }
                }
            }
        }

        Ok(())
    }

    #[inline(never)]
    fn get_children_set(&self, n: NodeIndex) -> FnvHashSet<NodeIndex> {
        let mut set = FnvHashSet::default();
        let dfs = Dfs::new(&self.graph, n);
        for e in dfs.iter(&self.graph) {
            if let Node::File { .. } = &self.graph[e] {
                set.insert(e);
            }
        }
        set
    }

    fn get_all_paths(&self, n: NodeIndex) -> impl Iterator<Item=PathBuf> + '_ {
        self.get_all_paths_internal(n).into_iter().map(|v| PathBuf::from_iter(v.iter()))
    }
    fn get_all_paths_internal(&self, n: NodeIndex) -> Vec<Vec<&Path>> {
        let name = match &self.graph[n] {
            Node::Dir { name, .. } => name.as_os_str(),
            Node::MergedDir { names, .. } => names[0].as_os_str(),
            x => {
                let ancestors = NodeFiltered::from_fn(&self.graph, |x| {
                    petgraph::algo::has_path_connecting(&self.graph, x, n, None)
                });

                tracing::warn!(?x, ancestry = ?format_args!("{:#?}", petgraph::dot::Dot::new(&ancestors)), "unknown ancestor");

                tracing::warn!(?x, "unknown ancestor");
                OsStr::new("?")
            }
        };
        let name = Path::new(name);

        let mut paths = Vec::new();
        for parent in self.graph.neighbors_directed(n, Direction::Incoming) {
            paths.extend(self.get_all_paths_internal(parent));
        }
        paths.iter_mut().for_each(|p| p.push(name));
        if paths.is_empty() {
            paths.push(vec![name]);
        }
        paths
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

    let node = graph.add_node(Node::Dir { name: dir.file_name().unwrap_or(OsStr::new(prefix)).to_owned(), cumulative_size: 0 });
    match dir.parent() {
        Some(parent) => {
            let parent = mkdir_p(dirs, graph, prefix, parent)?;
            graph.add_edge(parent, node, ());
        }
        _ => {
            // root dir
        }
    }

    dirs.insert(dir.to_owned(), node);

    Ok(node)
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
        confl_files: &'a Vec<NodeIndex>,
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
        if let Node::MergedDir { names, dedup_cumulative_size, dup_cumulative_size, confl_files: NoDebug(confl_files) } = n {
            // must be contained in multiple places
            let parent_count = deduper.graph.neighbors_directed(ni, Direction::Incoming).count();
            if parent_count > 1 {
                heap.push(Merged {
                    ni,
                    names,
                    dedup_cumulative_size,
                    dup_cumulative_size,
                    confl_files,
                });
            }
        }
    }

    let top_duplications = iter::from_fn(|| heap.pop()).take(1000);
    for m in top_duplications {
        let paths = deduper.get_all_paths(m.ni);
        let names = m.names.iter().map(|x| x.to_string_lossy()).collect::<Vec<_>>().join(", ");
        let paths = paths.map(|x| x.to_string_lossy().into_owned()).collect::<Vec<_>>().join(", ");

        let mut confl_files = Vec::new();
        let mut confl_size = 0;
        for &f in m.confl_files {
            if let Node::File { name, size, .. } = &deduper.graph[f] {
                confl_files.push(name.to_string_lossy());
                confl_size += size;
            }
        }
        let confl_file_cnt = confl_files.len();
        let confl_file_names = confl_files.join(", ");

        println!("Merging ({names}) saves {} (in: {paths}), conflicting on {confl_file_cnt} files for {} ({confl_file_names})", indicatif::HumanBytes(m.saved_bytes()), indicatif::HumanBytes(confl_size));
    }

    Ok(())
}
