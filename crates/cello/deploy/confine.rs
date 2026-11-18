// Modified by ryuzcorp (ryuzcorp/celld): confine esbuild's reads to the project. Apache-2.0.

//! Read confinement for the esbuild child.
//!
//! esbuild reads whatever the project's sources point at: `..` and absolute
//! imports, directory links, an input's `//# sourceMappingURL=`, a
//! `tsconfig.json` `extends` chain, and the parent directories it searches
//! for `node_modules` and configs. Every such read can carry an outside
//! file's bytes into the bundle, its source map, or esbuild's own error
//! output, which quotes the offending source line. The deploy may run with
//! more privilege than the project's author, so on Linux the child runs
//! under a Landlock ruleset that permits file and directory reads only
//! beneath the directories named here, plus the esbuild executable itself
//! (the kernel opens a binary for reading to execute it). A denied read
//! fails like a missing file, so nothing outside reaches any output. Writes
//! are left alone: the ruleset handles only the two read rights. The binary
//! must therefore be self-contained, as the release esbuild is: a wrapper
//! script could not read its interpreter.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Where `program` executes from: itself when it names a path, else the
/// first match on `PATH`, as `execvp` searches, resolved against celld's own
/// working directory. Either way it must be an executable regular file;
/// `None` when nothing qualifies. The result is canonical, so the child,
/// which starts in the project directory, executes exactly the file the
/// Landlock rule names: a relative `PATH` entry or `CELLD_ESBUILD` can
/// never pick up a binary planted in the project.
pub(super) fn locate_executable(program: &str) -> Option<PathBuf> {
    use std::os::unix::fs::PermissionsExt as _;
    let executable = |candidate: &Path| {
        std::fs::metadata(candidate)
            .is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
    };
    let found = if program.contains('/') {
        Some(PathBuf::from(program)).filter(|path| executable(path))
    } else {
        std::env::split_paths(&std::env::var_os("PATH")?)
            .map(|directory| directory.join(program))
            .find(|candidate| executable(candidate))
    };
    found?.canonicalize().ok()
}

/// A ruleset ready for the child, or the reason the kernel offers none.
pub(super) enum ReadConfinement {
    #[cfg(target_os = "linux")]
    Ruleset(std::os::fd::OwnedFd),
    Unavailable(String),
}

impl ReadConfinement {
    /// A ruleset permitting reads only beneath `directories` and of the
    /// file `executable`. A kernel or sandbox without Landlock yields
    /// `Unavailable`; once the kernel has answered that it supports it, any
    /// later failure is an error rather than a silent fallback.
    pub(super) fn beneath(directories: &[&Path], executable: &Path) -> anyhow::Result<Self> {
        imp::beneath(directories, executable)
    }

    /// Whether the child will run confined.
    pub(super) fn confines(&self) -> bool {
        !matches!(self, ReadConfinement::Unavailable(_))
    }

    /// Apply the ruleset to `command`'s child just before it executes.
    /// The parent keeps the ruleset descriptor until the child has started,
    /// so `self` must outlive the spawn.
    pub(super) fn apply(&self, command: &mut Command) {
        match self {
            #[cfg(target_os = "linux")]
            ReadConfinement::Ruleset(ruleset) => imp::apply(ruleset, command),
            ReadConfinement::Unavailable(_) => {}
        }
    }
}

#[cfg(target_os = "linux")]
mod imp {
    use super::ReadConfinement;
    use anyhow::Context as _;
    use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
    use std::os::unix::fs::OpenOptionsExt as _;
    use std::os::unix::process::CommandExt as _;
    use std::path::Path;
    use std::process::Command;

    const CREATE_RULESET_VERSION: u32 = 1 << 0;
    const RULE_PATH_BENEATH: libc::c_int = 1;
    const ACCESS_FS_READ_FILE: u64 = 1 << 2;
    const ACCESS_FS_READ_DIR: u64 = 1 << 3;
    const READS: u64 = ACCESS_FS_READ_FILE | ACCESS_FS_READ_DIR;

    /// `struct landlock_ruleset_attr` up to ABI 1. The kernel accepts a
    /// shorter struct and treats the later fields as zero.
    #[repr(C)]
    struct RulesetAttr {
        handled_access_fs: u64,
    }

    /// `struct landlock_path_beneath_attr`, packed as in the UAPI header.
    #[repr(C, packed)]
    struct PathBeneathAttr {
        allowed_access: u64,
        parent_fd: i32,
    }

    pub(super) fn beneath(
        directories: &[&Path],
        executable: &Path,
    ) -> anyhow::Result<ReadConfinement> {
        // SAFETY: the version query takes no attribute and returns the ABI
        // version or -1 with errno.
        let abi = unsafe {
            libc::syscall(
                libc::SYS_landlock_create_ruleset,
                std::ptr::null::<RulesetAttr>(),
                0usize,
                CREATE_RULESET_VERSION,
            )
        };
        if abi < 1 {
            let error = std::io::Error::last_os_error();
            return Ok(ReadConfinement::Unavailable(format!(
                "the kernel offers no Landlock ({error})"
            )));
        }
        let attr = RulesetAttr {
            handled_access_fs: READS,
        };
        // SAFETY: `attr` is a valid `landlock_ruleset_attr` prefix of the
        // size passed; the returned descriptor is close-on-exec.
        let fd = unsafe {
            libc::syscall(
                libc::SYS_landlock_create_ruleset,
                &attr as *const RulesetAttr,
                std::mem::size_of::<RulesetAttr>(),
                0u32,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error()).context("create the Landlock ruleset");
        }
        // SAFETY: the syscall returned a fresh descriptor this code owns.
        let ruleset = unsafe { OwnedFd::from_raw_fd(fd as i32) };
        for directory in directories {
            allow(&ruleset, directory, libc::O_DIRECTORY, READS)?;
        }
        // A file rule may grant file rights only.
        allow(&ruleset, executable, 0, ACCESS_FS_READ_FILE)?;
        Ok(ReadConfinement::Ruleset(ruleset))
    }

    /// Grant `access` beneath `path` (or on it, for a file). The `O_PATH`
    /// open follows links, so the rule lands on what `path` resolves to,
    /// which is what the child's reads reach too.
    fn allow(
        ruleset: &OwnedFd,
        path: &Path,
        flags: libc::c_int,
        access: u64,
    ) -> anyhow::Result<()> {
        let handle = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_PATH | libc::O_CLOEXEC | flags)
            .open(path)
            .with_context(|| format!("open {} for the Landlock rule", path.display()))?;
        let rule = PathBeneathAttr {
            allowed_access: access,
            parent_fd: handle.as_raw_fd(),
        };
        // SAFETY: `rule` is a valid `landlock_path_beneath_attr`, and both
        // descriptors stay open across the call.
        let added = unsafe {
            libc::syscall(
                libc::SYS_landlock_add_rule,
                ruleset.as_raw_fd(),
                RULE_PATH_BENEATH,
                &rule as *const PathBeneathAttr,
                0u32,
            )
        };
        if added != 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("add the Landlock rule for {}", path.display()));
        }
        Ok(())
    }

    pub(super) fn apply(ruleset: &OwnedFd, command: &mut Command) {
        let ruleset = ruleset.as_raw_fd();
        // SAFETY: the hook runs between fork and exec and makes only the two
        // async-signal-safe syscalls; it allocates nothing.
        unsafe {
            command.pre_exec(move || {
                // Landlock requires no_new_privs for an unprivileged caller,
                // and the child gains nothing from setuid binaries anyway.
                if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::syscall(libc::SYS_landlock_restrict_self, ruleset, 0u32) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use super::ReadConfinement;
    use std::path::Path;

    pub(super) fn beneath(
        _directories: &[&Path],
        _executable: &Path,
    ) -> anyhow::Result<ReadConfinement> {
        Ok(ReadConfinement::Unavailable(
            "read confinement needs Linux Landlock".to_string(),
        ))
    }
}
