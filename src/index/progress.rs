use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

pub struct ProgressWriterState {
    files_found: AtomicU64,
    bytes_found: AtomicU64,
}
impl ProgressWriterState {
    pub fn create() -> Arc<ProgressWriterState> {
        let arc = Arc::new(Self {
            files_found: AtomicU64::new(0),
            bytes_found: AtomicU64::new(0),
        });
        let weak = Arc::downgrade(&arc);
        thread::spawn(move || {
            let mut last_bytes = 0;
            loop {
                match weak.upgrade() {
                    Some(state) => {
                        let files_found = indicatif::HumanCount(state.files_found.load(Ordering::Relaxed));
                        let bytes_found = indicatif::HumanBytes(state.bytes_found.load(Ordering::Relaxed));
                        //let bytes_per_second = bytes_found.0 as f64 / state.start_ts.elapsed().as_secs_f64();
                        let bytes_per_second = indicatif::HumanBytes(bytes_found.0 - last_bytes);
                        last_bytes = bytes_found.0;
                        println!("Found {files_found} files for a total of {bytes_found} ({bytes_per_second}/s)");
                    }
                    None => break,
                }
                thread::sleep(Duration::from_secs(1));
            }
        });
        arc
    }

    pub fn increment(&self, bytes: u64) {
        self.increment_raw(1, bytes);
    }
    pub fn increment_raw(&self, count: u64, bytes: u64) {
        self.files_found.fetch_add(count, Ordering::Relaxed);
        self.bytes_found.fetch_add(bytes, Ordering::Relaxed);
    }
}
