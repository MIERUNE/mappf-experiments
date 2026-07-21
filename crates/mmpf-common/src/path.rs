//! Best-effort filesystem identity for preflight artifact checks.

use std::path::{Path, PathBuf};

/// Returns whether two paths resolve to the same existing or prospective file.
///
/// Existing paths are canonicalized directly. For a not-yet-created output,
/// the existing parent is canonicalized before its file name is reattached, so
/// `.`/`..` and parent-directory symlinks compare by real location. If either
/// identity cannot be resolved, the function conservatively returns `false`;
/// callers must still handle errors when they open the path.
pub fn same_file_target(a: &Path, b: &Path) -> bool {
    match (resolve_identity(a), resolve_identity(b)) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

fn resolve_identity(path: &Path) -> Option<PathBuf> {
    if let Ok(canonical) = path.canonicalize() {
        return Some(canonical);
    }
    let file_name = path.file_name()?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    Some(parent.canonicalize().ok()?.join(file_name))
}

#[cfg(test)]
mod tests {
    use super::same_file_target;

    #[test]
    fn normalizes_dot_for_a_prospective_file() {
        let directory = std::env::temp_dir();
        let direct = directory.join("mmpf-common-prospective-artifact");
        let dotted = directory.join(".").join("mmpf-common-prospective-artifact");

        assert!(same_file_target(&direct, &dotted));
    }

    #[cfg(unix)]
    #[test]
    fn resolves_an_existing_symlink_alias() {
        use std::{fs, os::unix::fs::symlink};

        let directory =
            std::env::temp_dir().join(format!("mmpf-common-path-test-{}", std::process::id()));
        let target = directory.join("target");
        let alias = directory.join("alias");
        fs::create_dir_all(&directory).expect("create test directory");
        fs::write(&target, b"artifact").expect("create target");
        let _ = fs::remove_file(&alias);
        symlink(&target, &alias).expect("create symlink");

        assert!(same_file_target(&target, &alias));

        fs::remove_dir_all(directory).expect("remove test directory");
    }
}
