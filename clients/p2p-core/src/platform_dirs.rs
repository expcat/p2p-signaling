use std::path::PathBuf;

#[cfg(target_os = "windows")]
pub(crate) fn data_dir() -> Option<PathBuf> {
    env_path("LOCALAPPDATA").or_else(|| env_path("APPDATA"))
}

#[cfg(target_os = "macos")]
pub(crate) fn data_dir() -> Option<PathBuf> {
    home_dir().map(|home| home.join("Library").join("Application Support"))
}

#[cfg(all(unix, not(target_os = "macos")))]
pub(crate) fn data_dir() -> Option<PathBuf> {
    env_path("XDG_DATA_HOME").or_else(|| home_dir().map(|home| home.join(".local").join("share")))
}

#[cfg(not(any(target_os = "windows", target_os = "macos", unix)))]
pub(crate) fn data_dir() -> Option<PathBuf> {
    None
}

fn env_path(key: &str) -> Option<PathBuf> {
    let value = std::env::var_os(key)?;
    if value.is_empty() {
        None
    } else {
        Some(PathBuf::from(value))
    }
}

#[cfg(any(target_os = "macos", all(unix, not(target_os = "macos"))))]
fn home_dir() -> Option<PathBuf> {
    env_path("HOME")
}
