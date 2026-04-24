use std::path::PathBuf;

pub fn to_windows_path(path: &PathBuf) -> PathBuf {
    let s = path.to_string_lossy();
    if let Some(rest) = s.strip_prefix("/mnt/") {
        let mut chars = rest.chars();
        if let Some(drive) = chars.next() {
            if drive.is_ascii_alphabetic() {
                let after_drive = chars.as_str();
                if after_drive.is_empty() || after_drive.starts_with('/') {
                    let tail = after_drive.trim_start_matches('/').replace('/', "\\");
                    return PathBuf::from(format!("{}:\\{}", drive.to_uppercase(), tail));
                }
            }
        }
    }
    path.clone()
}

pub fn wsl_path(path: &std::path::Path) -> PathBuf {
    let s = path.to_string_lossy();
    let mut chars = s.chars();
    if let (Some(drive), Some(':')) = (chars.next(), chars.next()) {
        if drive.is_ascii_alphabetic() {
            let rest = s[2..].replace('\\', "/");
            let rest = rest.trim_start_matches('/');
            return PathBuf::from(format!("/mnt/{}/{}", drive.to_lowercase(), rest));
        }
    }
    path.to_path_buf()
}

pub fn is_wsl() -> bool {
    std::fs::read_to_string("/proc/version")
        .map(|v| v.to_lowercase().contains("microsoft"))
        .unwrap_or(false)
}

pub fn to_windows_path_any(path: &PathBuf) -> PathBuf {
    if !is_wsl() { return path.clone(); }
    let out = std::process::Command::new("wslpath")
        .args(["-w", &path.to_string_lossy()])
        .output();
    if let Ok(o) = out {
        let s = String::from_utf8_lossy(&o.stdout).trim().to_string();
        if !s.is_empty() { return PathBuf::from(s); }
    }
    path.clone()
}

pub fn books_dir() -> PathBuf {
    let dir = dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join("luck")
        .join("books");
    let _ = std::fs::create_dir_all(&dir);
    dir
}

pub fn fmt_num(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out.chars().rev().collect()
}

pub fn format_hm(seconds: u64) -> String {
    let h = seconds / 3600;
    let m = (seconds % 3600) / 60;
    format!("{}h {:02}m", h, m)
}

pub fn format_duration(s: u64) -> String {
    let h = s / 3600;
    let m = (s % 3600) / 60;
    let s = s % 60;
    if h > 0 {
        format!("{}:{:02}:{:02}", h, m, s)
    } else {
        format!("{}:{:02}", m, s)
    }
}

#[allow(dead_code)]
pub fn first_line(s: &str) -> &str {
    s.lines()
        .find(|l| !l.trim().is_empty())
        .unwrap_or(s)
        .trim()
}

