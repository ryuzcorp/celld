// Modified by ryuzcorp (ryuzcorp/celld): a serving process is non-dumpable.
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Process hardening the fork adds (patch 0053).
//!
//! A serving node holds its platform's credentials in its environment
//! (`AWS_*`, `CELLD_DEPLOY_AWS_*`, `CELLD_OPERATOR_TOKEN`, tenant vars), and
//! a fleet runs its nodes under a uid that tenant builds share while they
//! run. A dumpable process lets any process of that uid read
//! `/proc/<pid>/environ`, `/proc/<pid>/mem`, and `/proc/<pid>/maps`, so a
//! build could copy the node's credentials out and publish to the fleet's
//! bucket.
//!
//! `prctl(PR_SET_DUMPABLE, 0)` marks the process non-dumpable: the kernel
//! then refuses those files to everyone but root (or a process holding
//! `CAP_SYS_PTRACE`) and refuses `ptrace` from a non-root sibling. The cost
//! is that a non-root debugger cannot attach and the kernel writes no core
//! dump; a root operator keeps both.
//!
//! `execve` resets the flag, so every serving process must set it again
//! after it starts. `main` does that before it reads anything else, and
//! every way celld starts a serving process (the node, `celld dev`'s
//! supervisor and its node, the control plane's credential-rotation
//! re-exec) passes through `main` with its serving arguments. Operator
//! commands (`celld deploy`, `d1`, `r2`, ...) stay dumpable: they are not
//! long-lived, and an operator debugging one needs to attach.
//!
//! The guarantee holds from that call onward, not from `execve`: between
//! the two the process is still dumpable, and a same-uid sibling can read
//! its environment. Initial boot, `celld dev`'s node respawn, and the
//! control-plane credential-rotation re-exec each pay that window. Closing
//! it needs the flag set before the image runs (a launcher, or a wrapper
//! that clears the environment), which celld does not do.
//!
//! A child a serving process spawns inherits that environment, and
//! `execve` makes the child dumpable again — `PR_SET_DUMPABLE` does not
//! cross it. A child that needs none of the fleet's variables therefore
//! starts with [`minimal_child_environment`].

/// Start a child of a serving process from an empty environment plus the
/// variables an ordinary program needs to run: `PATH`, `HOME`, and
/// `TMPDIR`. None of them authenticates; everything the serving process
/// holds (`AWS_*`, `CELLD_DEPLOY_AWS_*`, `CELLD_OPERATOR_TOKEN`, tenant
/// vars) stays out of the child, which `execve` makes dumpable again.
pub fn minimal_child_environment(command: &mut std::process::Command) {
    command.env_clear();
    for name in ["PATH", "HOME", "TMPDIR"] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
}

/// Make this process non-dumpable. Linux only; elsewhere this is a no-op.
#[cfg(target_os = "linux")]
pub fn set_non_dumpable() -> std::io::Result<()> {
    // SAFETY: `PR_SET_DUMPABLE` takes the scalar in `arg2` and reads or
    // writes no memory this process owns.
    let result = unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Make this process non-dumpable. Linux only; elsewhere this is a no-op.
#[cfg(not(target_os = "linux"))]
pub fn set_non_dumpable() -> std::io::Result<()> {
    Ok(())
}

/// The kernel's view of this process's dumpable attribute (0 is
/// non-dumpable). The tests read it to prove the call landed.
#[cfg(all(test, target_os = "linux"))]
fn dumpable() -> i32 {
    // SAFETY: `PR_GET_DUMPABLE` writes nothing and reads no memory of ours.
    unsafe { libc::prctl(libc::PR_GET_DUMPABLE, 0, 0, 0, 0) }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};

    /// The child re-enters this test with the marker set, sets the flag,
    /// and reports what the kernel says.
    const CHILD_ENV: &str = "CELLD_HARDENING_TEST_CHILD";
    const TEST: &str = "hardening::tests::a_serving_child_is_not_dumpable";

    #[test]
    fn a_serving_child_is_not_dumpable() {
        if std::env::var_os(CHILD_ENV).is_some() {
            return child();
        }
        parent();
    }

    /// The child half: set the flag, say what `PR_GET_DUMPABLE` reports, and
    /// stay alive until the parent closes stdin.
    fn child() {
        set_non_dumpable().expect("the flag is set");
        let mut stdout = std::io::stdout().lock();
        writeln!(stdout, "ready dumpable={}", dumpable()).expect("report to the parent");
        stdout.flush().expect("flush the report");
        let mut line = String::new();
        let _ = std::io::stdin().lock().read_line(&mut line);
    }

    /// The parent half: a same-uid sibling must not be able to read the
    /// child's environment, and the child's own `PR_GET_DUMPABLE` must be 0.
    #[allow(clippy::disallowed_methods)] // The test reads /proc, not node storage.
    fn parent() {
        let mut child = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args(["--exact", TEST, "--nocapture"])
            .env(CHILD_ENV, "1")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn the child");
        let mut reader = BufReader::new(child.stdout.take().expect("piped stdout"));
        let mut line = String::new();
        loop {
            line.clear();
            assert!(
                reader.read_line(&mut line).expect("read the report") > 0,
                "the child exited before it reported the flag"
            );
            if let Some(report) = line.trim().strip_prefix("ready ") {
                assert_eq!(
                    report, "dumpable=0",
                    "PR_SET_DUMPABLE(0) must leave PR_GET_DUMPABLE at 0"
                );
                break;
            }
        }
        // Root reads any /proc file, so the denial is only observable
        // unprivileged; the child's own report above covers that case.
        if unsafe { libc::geteuid() } != 0 {
            let path = format!("/proc/{}/environ", child.id());
            let opened = std::fs::File::open(&path);
            assert!(
                opened.is_err(),
                "a same-uid reader could still open {path} of a non-dumpable process"
            );
        }
        drop(child.stdin.take());
        let mut sink = Vec::new();
        reader.read_to_end(&mut sink).expect("drain the child");
        child.wait().expect("reap the child");
    }
}

#[cfg(all(test, unix))]
mod environment_tests {
    use super::*;

    /// A child a serving process spawns must not see the fleet's
    /// variables: it starts from `PATH`, `HOME`, and `TMPDIR` alone.
    #[test]
    fn a_child_starts_from_the_minimal_environment() {
        let mut command = std::process::Command::new("env");
        minimal_child_environment(&mut command);
        let output = command.output().expect("`env` resolves on the kept PATH");
        let mut keys: Vec<&str> = std::str::from_utf8(&output.stdout)
            .expect("`env` prints UTF-8")
            .lines()
            .filter_map(|line| line.split_once('=').map(|(name, _)| name))
            .collect();
        let mut expected: Vec<&str> = ["PATH", "HOME", "TMPDIR"]
            .into_iter()
            .filter(|name| std::env::var_os(name).is_some())
            .collect();
        keys.sort_unstable();
        expected.sort_unstable();
        assert_eq!(
            keys, expected,
            "the child must see the three ordinary variables and nothing else"
        );
    }
}
