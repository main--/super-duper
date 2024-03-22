use std::fs::{self, File};
use std::os::linux::fs::MetadataExt;
use std::path::{PathBuf, Path};
use std::sync::Arc;

use blake3::Hasher;
use color_eyre::eyre::{OptionExt, bail};
use rusqlite::{Connection, Statement};

use crate::ArgsIndex;

use self::progress::ProgressWriterState;

mod progress;

pub struct DirIndexer<'a> {
    prefix: &'a str,
    progress: Arc<ProgressWriterState>,

    stmt_is_dir_done: Statement<'a>,
    stmt_get_dir_files: Statement<'a>,
    stmt_mark_dir_done: Statement<'a>,
    stmt_is_file_done: Statement<'a>,
    stmt_insert_file: Statement<'a>,
}

impl DirIndexer<'_> {
    pub fn index(conn: &mut Connection, i: ArgsIndex) -> color_eyre::Result<()> {
        let prefix = i.prefix.as_ref().map(String::as_str).map_or_else(|| i.folder.to_str().ok_or_eyre("folder is invalid unicode lol"), Ok)?;
        let _span = tracing::info_span!("indexer", %prefix).entered();
        let mut indexer = DirIndexer {
            prefix,
            progress: ProgressWriterState::create(),
            stmt_is_dir_done: conn.prepare("SELECT COUNT(*) > 0 FROM dirs WHERE prefix = ? AND path = ?")?,
            stmt_get_dir_files: conn.prepare("SELECT COUNT(*), SUM(size) FROM files WHERE prefix = ? AND path LIKE ? ESCAPE '\\'")?,
            stmt_mark_dir_done: conn.prepare("INSERT INTO dirs(prefix, path) VALUES(?, ?) ON CONFLICT DO NOTHING")?,
            stmt_is_file_done: conn.prepare("SELECT COUNT(*) > 0 FROM files WHERE prefix = ? AND path = ?")?,
            stmt_insert_file: conn.prepare("INSERT INTO files(prefix, path, size, hash) VALUES(?, ?, ?, ?) ON CONFLICT DO UPDATE SET hash = excluded.hash, size = excluded.size")?,
        };
        indexer.index_dir(&i.folder, PathBuf::new())?;
        conn.execute_batch("PRAGMA optimize")?;
        Ok(())
    }

    fn index_dir(&mut self, folder: &Path, rel_path: PathBuf) -> color_eyre::Result<()> {
        if !folder.is_dir() {
            bail!("{} is not a directory!", folder.display());
        }

        let sql_relpath = rel_path.to_str().ok_or_eyre("non-utf8 path")?;
        let is_done: bool = self.stmt_is_dir_done.query_row([&self.prefix, sql_relpath], |r| r.get(0))?;
        if is_done && !rel_path.as_os_str().is_empty() {
            tracing::trace!("skipping dir");
            let like_escaped_path = rel_path.to_str().ok_or_eyre("non utf8 path")?.replace("_", "\\_").replace("%", "\\%");
            let (cnt, sz) = self.stmt_get_dir_files.query_row((&self.prefix, format!("{like_escaped_path}%")), |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?;
            self.progress.increment_raw(cnt, sz);
            return Ok(());
        }

        for entry in fs::read_dir(folder)? {
            let entry = entry?;
            let meta = entry.metadata()?;
            let abs_path = entry.path();
            let rel_path = rel_path.join(entry.file_name());

            let _span = tracing::info_span!("dir entry", path = %entry.file_name().to_string_lossy()).entered();
            if meta.is_symlink() {
                tracing::info!("skipping symlink");
            } else if meta.is_dir() {
                tracing::trace!("recursing");
                self.index_dir(&abs_path, rel_path)?;
            } else if meta.is_file() {
                let st_nlink = meta.st_nlink();
                if st_nlink != 1 {
                    tracing::warn!(%st_nlink, "hardlink found");
                }

                let bytes = meta.len();
                self.index_file(&abs_path, &rel_path, bytes)?;
                self.progress.increment(bytes);
            } else {
                tracing::info!("skipping unknown thing");
            }
            meta.st_dev();
        }

        self.stmt_mark_dir_done.execute([&self.prefix, sql_relpath])?;

        Ok(())
    }

    fn index_file(&mut self, abs_path: &PathBuf, rel_path: &PathBuf, size: u64) -> Result<(), color_eyre::eyre::Error> {
        let sql_relpath = rel_path.to_str().ok_or_eyre("non-utf8 path")?;
        let is_done = self.stmt_is_file_done.query_row([&self.prefix, sql_relpath], |row| row.get(0))?;
        if is_done {
            tracing::trace!("skipping file");
            return Ok(());
        }

        let f = File::open(&abs_path)?;
        let mut hasher = Hasher::new();
        hasher.update_reader(f)?;
        let hash = hasher.finalize();
        tracing::trace!(%hash);
        self.stmt_insert_file.execute((&self.prefix, rel_path.to_str().ok_or_eyre("non-utf8 path")?, size, hash.as_bytes()))?;
        Ok(())
    }
}
