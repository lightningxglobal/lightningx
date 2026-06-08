//! Shared harness for chaos / failover drills: ArchivingMediaDriver
//! lifecycle, child-process management with SIGKILL, and wait helpers.
#![allow(dead_code)]

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub fn jar_path() -> Option<String> {
    [
        std::env::var("AERON_ALL_JAR").ok(),
        Some(
            "/home/alpha/work/3party/aeron/aeron-all/build/libs/aeron-all-1.52.0-SNAPSHOT.jar"
                .into(),
        ),
    ]
    .into_iter()
    .flatten()
    .find(|p| std::path::Path::new(p).exists())
}

pub struct DriverGuard {
    child: Child,
    pub dir: std::path::PathBuf,
    pub aeron_dir: String,
    pub control_channel: String,
}

impl Drop for DriverGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Start an ArchivingMediaDriver with production durability settings
/// (file.sync.level=2) on a unique control port + temp dirs.
pub fn start_archiving_driver(jar: &str, control_port: u16) -> std::io::Result<DriverGuard> {
    let base = std::env::temp_dir().join(format!(
        "chaos-{}-{}-{}",
        control_port,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&base)?;
    let aeron_dir = base.join("driver").to_string_lossy().into_owned();
    let archive_dir = base.join("archive").to_string_lossy().into_owned();
    let control_channel = format!("aeron:udp?endpoint=localhost:{control_port}");

    let child = Command::new("java")
        .arg("--add-opens")
        .arg("java.base/jdk.internal.misc=ALL-UNNAMED")
        .arg("--add-opens")
        .arg("java.base/sun.nio.ch=ALL-UNNAMED")
        .arg("-cp")
        .arg(jar)
        .arg(format!("-Daeron.dir={aeron_dir}"))
        .arg(format!("-Daeron.archive.dir={archive_dir}"))
        .arg(format!("-Daeron.archive.control.channel={control_channel}"))
        .arg("-Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0")
        .arg("-Daeron.archive.recording.events.enabled=false")
        .arg("-Daeron.archive.file.sync.level=2")
        .arg("-Daeron.archive.catalog.file.sync.level=2")
        .arg("-Daeron.threading.mode=SHARED")
        .arg("-Daeron.archive.threading.mode=SHARED")
        .arg("-Daeron.shared.idle.strategy=sleep-ns")
        .arg("io.aeron.archive.ArchivingMediaDriver")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    Ok(DriverGuard {
        child,
        dir: base,
        aeron_dir,
        control_channel,
    })
}

/// A child process that is SIGKILLed on drop (never leaks across panics).
pub struct ProcGuard {
    pub child: Option<Child>,
    pub name: String,
}

impl ProcGuard {
    pub fn spawn(name: &str, cmd: &mut Command) -> std::io::Result<Self> {
        let child = cmd.spawn()?;
        Ok(Self {
            child: Some(child),
            name: name.to_string(),
        })
    }

    /// SIGKILL — the whole point of a chaos drill: no graceful anything.
    pub fn kill9(&mut self) {
        if let Some(c) = &mut self.child {
            let _ = c.kill();
            let _ = c.wait();
        }
        self.child = None;
    }

    /// OS pid of the running child (for /proc sampling), if alive.
    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(|c| c.id())
    }

    pub fn is_running(&mut self) -> bool {
        match &mut self.child {
            Some(c) => matches!(c.try_wait(), Ok(None)),
            None => false,
        }
    }
}

impl Drop for ProcGuard {
    fn drop(&mut self) {
        self.kill9();
    }
}

pub fn wait_for<T>(what: &str, timeout: Duration, mut probe: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(v) = probe() {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

pub fn pg_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://user:password@localhost:5432/mydb".to_string())
}

/// Deterministic LCG for reproducible chaos timing.
pub struct Lcg(pub u64);
impl Lcg {
    pub fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo + 1)
    }
}

/// Cross-process serialization for the heavy full-topology drills: each
/// spawns a JVM driver + engine + pg-writer + desk and needs real
/// wall-clock timing, so running 5-6 of them concurrently (cargo runs
/// test BINARIES in parallel) starves them and causes spurious timeouts.
/// This flock-style guard (O_EXCL lockfile + stale-steal) lets only one
/// heavy drill run at a time; drop releases it. Acquire AFTER the
/// infra-availability skip checks so skipped drills don't serialize.
pub struct DrillGuard {
    path: std::path::PathBuf,
}

impl DrillGuard {
    pub fn acquire() -> Self {
        let path = std::env::temp_dir().join("lightning-heavy-drill.lock");
        let deadline = Instant::now() + Duration::from_secs(600);
        loop {
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(_) => return Self { path },
                Err(_) => {
                    // Steal a stale lock (a panicked drill that never
                    // released): older than 5 min can't be a live drill.
                    if let Ok(meta) = std::fs::metadata(&path) {
                        if let Ok(modified) = meta.modified() {
                            if modified.elapsed().map(|e| e > Duration::from_secs(300)).unwrap_or(false) {
                                let _ = std::fs::remove_file(&path);
                                continue;
                            }
                        }
                    }
                    assert!(Instant::now() < deadline, "timed out waiting for the heavy-drill lock");
                    std::thread::sleep(Duration::from_millis(200));
                }
            }
        }
    }
}

impl Drop for DrillGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Resident set size (KiB) of a process from /proc/<pid>/status, or None
/// if it can't be read (process gone / not Linux).
pub fn rss_kib(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest.trim().trim_end_matches(" kB").trim().parse().ok();
        }
    }
    None
}
