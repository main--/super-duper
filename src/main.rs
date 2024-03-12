use std::path::PathBuf;

use argh::FromArgs;
use rusqlite::Connection;
use tracing::Level;
use tracing_error::ErrorLayer;
use tracing_subscriber::layer::SubscriberExt;

mod index;
mod dedup;

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
struct ArgsLs {}

#[derive(FromArgs)]
#[argh(subcommand, name = "dedup")]
/// Find most impactful duplications
struct ArgsDedup {}

fn main() -> color_eyre::Result<()> {
    let subscriber = tracing_subscriber::Registry::default()
        // any number of other subscriber layers may be added before or
        // after the `ErrorLayer`...
        .with(tracing_subscriber::filter::LevelFilter::from_level(Level::TRACE))
        .with(ErrorLayer::default())
        .with(tracing_subscriber::fmt::layer())
        ;

    // set the subscriber as the default for the application
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
        ArgsCmd::Ls(ArgsLs {}) => {
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
        ArgsCmd::Dedup(ArgsDedup {}) => dedup::dedup(&mut conn)?,
    }

    Ok(())
}

