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

#[allow(dead_code)]
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

