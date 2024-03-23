use std::fs::Metadata;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use blake3::Hash;
use rusqlite::Connection;


pub fn hardlink(conn: &mut Connection, path: &Path) -> color_eyre::Result<()> {
    let mut stmt = conn.prepare("SELECT prefix, path, size, hash FROM files WHERE prefix LIKE ? ESCAPE '\\' ORDER BY hash")?;
    let mut rows = stmt.query([format!("{}%", super::tostr(&path)?.replace("_", "\\_").replace("%", "\\%"))])?;

    struct Group {
        hash: Hash,
        files: Vec<(PathBuf, Metadata)>,
    }
    impl Default for Group {
        fn default() -> Self {
            Self { hash: Hash::from_bytes([0; blake3::OUT_LEN]), files: Default::default() }
        }
    }
    impl Group {
        fn ingest(&mut self, hash: Hash, path: PathBuf, metadata: Metadata) -> color_eyre::Result<()> {
            if hash != self.hash {
                self.finish()?;
                self.hash = hash;
                self.files.clear();
            }
            self.files.push((path, metadata));
            Ok(())
        }

        fn finish(&mut self) -> color_eyre::Result<()> {
            // lowest inode takes precedence
            let Some(min_ino) = self.files.iter().map(|(_, m)| m.ino()).min() else {
                assert!(self.files.is_empty());
                return Ok(());
            };
            tracing::info!(?self.files, "merging");
            let primary_idx = self.files.iter().position(|(_, m)| m.ino() == min_ino).expect("we just saw this");

            let num_files = self.files.len();
            let (primary_path, primary_meta) = self.files.swap_remove(primary_idx);
            let num_other_files = self.files.len();
            assert_eq!(num_files - 1, num_other_files);

            for (p, m) in &self.files {
                assert_eq!(primary_meta.len(), m.len());
                assert_ne!(&primary_path, p);
            }

            for (p, _) in self.files.drain(..) {
                std::fs::remove_file(&p)?;
                std::fs::hard_link(&primary_path, &p)?;
            }

            Ok(())
        }
    }

    let mut current = Group::default();
    while let Some(row) = rows.next()? {
        let prefix = Path::new(row.get_ref(0)?.as_str()?);
        let path = Path::new(row.get_ref(1)?.as_str()?);
        let size: u64 = row.get(2)?;
        let hash: [u8; blake3::OUT_LEN] = row.get(3)?;
        let hash = Hash::from_bytes(hash);
        let full_path = prefix.join(path);
        let metadata = full_path.metadata()?;
        assert_eq!(size, metadata.len()); // sanity check
        current.ingest(hash, full_path, metadata)?;
    }
    current.finish()?;
    Ok(())
}
