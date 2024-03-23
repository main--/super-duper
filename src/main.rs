use std::fs::File;
use std::path::{PathBuf, Path};

use argh::FromArgs;
use color_eyre::eyre::{OptionExt, bail};
use rusqlite::{Connection, Rows};
use tracing::Level;
use tracing_error::ErrorLayer;
use tracing_subscriber::layer::SubscriberExt;

mod index;
mod dedup;
mod cleanup;

#[derive(FromArgs)]
/// Find duplicated files and folders
struct Args {
    #[argh(subcommand)]
    cmd: ArgsCmd,
    #[argh(option)]
    /// path to our sqlite database
    db: PathBuf,
}
#[derive(FromArgs)]
#[argh(subcommand)]
enum ArgsCmd {
    Index(ArgsIndex),
    Ls(ArgsLs),
    Dedup(ArgsDedup),
    Delta(ArgsDelta),
    HardLink(ArgsHardlink),
}

#[derive(FromArgs)]
#[argh(subcommand, name = "index")]
/// Index a folder structure
struct ArgsIndex {
    #[argh(positional)]
    /// target folder to index
    folder: PathBuf,

    #[argh(option)]
    /// prefix to use instead of folder path. this is needed in cases where you moved the data.
    /// you can specify the old prefix (obtain using super-duper ls) when the folder path has changed
    /// to help super-duper recognize pre-existing index data.
    prefix: Option<String>,
}

#[derive(FromArgs)]
#[argh(subcommand, name = "ls")]
/// List indexed folder structures
struct ArgsLs {
    #[argh(option)]
    /// translate filename
    translate_filename: Option<String>,
}

#[derive(FromArgs)]
#[argh(subcommand, name = "dedup")]
/// Find most impactful duplications
struct ArgsDedup {}

#[derive(FromArgs)]
#[argh(subcommand, name = "delta")]
/// Find difference in directory trees
struct ArgsDelta {
    #[argh(positional)]
    /// folder1
    folder1: PathBuf,
    #[argh(positional)]
    /// folder2
    folder2: PathBuf,
    #[argh(switch)]
    /// left
    left: bool,
}

#[derive(FromArgs)]
#[argh(subcommand, name = "hardlink")]
/// Find most impactful duplications
struct ArgsHardlink {
    #[argh(positional)]
    /// path
    path: PathBuf,
}


fn main() -> color_eyre::Result<()> {
    let subscriber = tracing_subscriber::Registry::default()
        .with(tracing_subscriber::filter::LevelFilter::from_level(Level::DEBUG))
        .with(ErrorLayer::default())
        .with(tracing_subscriber::fmt::layer())
        ;
    tracing::subscriber::set_global_default(subscriber)?;

    color_eyre::install()?;

    let args: Args = argh::from_env();


    let mut conn = Connection::open(&args.db)?;
    conn.execute_batch(r#"
PRAGMA journal_mode = WAL;
PRAGMA synchronous = normal;
PRAGMA temp_store = memory;
PRAGMA mmap_size = 30000000000;
PRAGMA case_sensitive_like = ON;

CREATE TABLE IF NOT EXISTS files(
    prefix TEXT NOT NULL,
    path TEXT NOT NULL,
    size INTEGER NOT NULL,
    hash BLOB NOT NULL,
    PRIMARY KEY (prefix, path)
);

CREATE INDEX IF NOT EXISTS filehash ON files(hash);

-- dirs listed here are done
CREATE TABLE IF NOT EXISTS dirs(
    prefix TEXT NOT NULL,
    path TEXT NOT NULL,
    PRIMARY KEY (prefix, path)
);
    "#)?;

    match args.cmd {
        ArgsCmd::Index(i) => index::DirIndexer::index(&mut conn, i)?,
        ArgsCmd::Ls(ArgsLs { translate_filename: None }) => {
            struct Row {
                prefix: String,
                size: u64,
                count: u64,
            }
            let mut stmt = conn.prepare("SELECT prefix, SUM(size), COUNT(*) FROM files GROUP BY prefix")?;
            let rows = stmt.query_map([], |row| {
                Ok(Row {
                    prefix: row.get(0)?,
                    size: row.get(1)?,
                    count: row.get(2)?,
                })
            })?;
            println!("Indexed directory trees:");
            for row in rows {
                let Row { prefix, size, count } = row?;
                let size = indicatif::HumanBytes(size);
                let count = indicatif::HumanCount(count);
                println!("{prefix} ({count} files, {size})");
            }
        }
        ArgsCmd::Ls(ArgsLs { translate_filename: Some(fname) }) => {
            fn print_paths(rows: Rows) -> color_eyre::Result<()> {
                let paths = rows.mapped(|row| Ok((row.get::<_, [u8; blake3::OUT_LEN]>(0)?, Path::new(&row.get::<_, String>(1)?).join(row.get::<_, String>(2)?))));
                for row in paths {
                    let (hash, path) = row?;
                    let hash = blake3::Hash::from_bytes(hash).to_hex();
                    println!("{hash} {}", path.display());
                }
                Ok(())
            }
            if Path::new(&fname).exists() {
                let hash = blake3::Hasher::new().update_reader(File::open(&fname)?)?.finalize();
                let mut stmt = conn.prepare("SELECT hash, prefix, path FROM files WHERE hash = ? ORDER BY hash")?;
                print_paths(stmt.query([hash.as_bytes()])?)?;
            } else {
                let like = format!("%/{}", fname.replace("_", "\\_").replace("%", "\\%"));
                let mut stmt = conn.prepare("SELECT f2.hash, f2.prefix, f2.path FROM files f1, files f2 WHERE f1.path LIKE ? ESCAPE '\\' AND f1.hash = f2.hash ORDER BY f2.hash")?;
                print_paths(stmt.query([like])?)?;
            }
        }
        ArgsCmd::Delta(ArgsDelta { folder1, folder2, left }) => {
            let prefixes: Vec<PathBuf> = conn.prepare("SELECT DISTINCT prefix FROM dirs")?.query_map([], |r| Ok(PathBuf::from(r.get::<_, String>(0)?)))?.collect::<Result<_, _>>()?;
            fn find_prefix<'a>(prefixes: &'a [PathBuf], buf: &'a Path) -> Option<(&'a Path, &'a Path)> {
                for pre in prefixes {
                    if let Ok(suffix) = buf.strip_prefix(pre) {
                        return Some((pre, suffix));
                    }
                }
                None
            }
            let (pre1, path1) = find_prefix(&prefixes, &folder1).ok_or_eyre("folder1 prefix not found")?;
            let (pre2, path2) = find_prefix(&prefixes, &folder2).ok_or_eyre("folder2 prefix not found")?;

            let mut stmt_exists = conn.prepare("SELECT COUNT(*) > 0 FROM dirs WHERE prefix = ? AND path = ?")?;
            let f1_exists: bool = stmt_exists.query_row([tostr(pre1)?, tostr(path1)?], |r| r.get(0))?;
            if !f1_exists {
                bail!("folder1 doesnt exist");
            }
            let f2_exists: bool = stmt_exists.query_row([tostr(pre2)?, tostr(path2)?], |r| r.get(0))?;
            if !f2_exists {
                bail!("folder2 doesnt exist");
            }

            //tracing::debug!(?pre1, ?path1, ?pre2, ?path2);

            let sql = r#"
            WITH files1 AS (
                SELECT prefix, path, hash
                FROM files
                WHERE prefix = ?
                AND path LIKE ? ESCAPE '\'
            ), files2 AS (
                SELECT prefix, path, hash
                FROM files
                WHERE prefix = ?
                AND path LIKE ? ESCAPE '\'
            )
            SELECT f1.prefix, f1.path, f2.prefix, f2.path
            FROM files1 f1 FULL OUTER JOIN files2 f2 ON f1.hash = f2.hash
            WHERE (f1.prefix IS NULL) OR (f2.prefix IS NULL)
            "#;
            let sql = match left {
                true => sql.replace("FULL OUTER JOIN", "LEFT OUTER JOIN"),
                false => sql.to_owned(),
            };
            let mut stmt = conn.prepare(&sql)?;
            fn path2like(p: &Path) -> color_eyre::Result<String> {
                if p.as_os_str().is_empty() {
                    Ok(format!("%"))
                } else {
                    Ok(format!("{}/%", tostr(p)?.replace("_", "\\_").replace("%", "\\%")))
                }
            }
            let params = [
                tostr(pre1)?,
                &path2like(path1)?,
                tostr(pre2)?,
                &path2like(path2)?,
            ];
            let mut rows = stmt.query(params)?;
            while let Some(row) = rows.next()? {
                let pre1: Option<String> = row.get(0)?;
                let path1: Option<String> = row.get(1)?;
                let pre2: Option<String> = row.get(2)?;
                let path2: Option<String> = row.get(3)?;
                match (pre1, path1, pre2, path2) {
                    (Some(pre), Some(path), None, None) | (None, None, Some(pre), Some(path)) => {
                        println!("{}", Path::new(&pre).join(&path).display());
                    }
                    _ => unreachable!()
                }
            }
        }
        ArgsCmd::Dedup(ArgsDedup {}) => dedup::dedup(&mut conn)?,
        ArgsCmd::HardLink(ArgsHardlink { path }) => cleanup::hardlink(&mut conn, &path)?,
    }

    Ok(())
}

fn tostr(p: &Path) -> color_eyre::Result<&str> {
    p.as_os_str().to_str().ok_or_eyre("non utf8 path")
}
