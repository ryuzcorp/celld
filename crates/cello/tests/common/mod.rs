// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by Ryuz Corp (Cello): shared ephemeral-port boot helpers (0077).

//! Shared boot helpers for the black-box runner tests: each test boots the
//! real `cello dev` binary and polls TCP with wall-clock deadlines.
//!
//! The dev node's public port is ephemeral (`--port 0`); the node's own
//! announce lines on the supervisor's stderr (`--logs` forwards them) name
//! the addresses it bound. Tests take them through `forward_announce` +
//! `announced` instead of probing a port first, so no probe-then-bind race
//! can hand a test another test's node. The node is non-dumpable (patch
//! 0053), so `/proc/<pid>/fd` no longer names its listeners.

use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};

/// One address of a `cello dev` node, from the announce line the supervisor
/// forwards to its stderr: `cello listening on <addr> (` names the public
/// listener, `cello internal listening on <addr> (` the internal one.
pub fn announced(lines: &Mutex<Vec<String>>, prefix: &str) -> Option<String> {
    lines
        .lock()
        .expect("announce lock")
        .iter()
        .find_map(|line| {
            let address = line.strip_prefix(prefix)?.split_once(' ')?.0;
            Some(address.to_string())
        })
}

/// Collect the supervisor's stderr lines, which carry the node's announce
/// lines. If the child exits before announcing, the caller panics with the
/// lines collected so far, so a future boot failure stays diagnosable.
pub fn forward_announce(stderr: std::process::ChildStderr, lines: Arc<Mutex<Vec<String>>>) {
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut line = String::new();
        while reader.read_line(&mut line).is_ok_and(|read| read > 0) {
            lines
                .lock()
                .expect("announce lock")
                .push(std::mem::take(&mut line));
        }
    });
}
