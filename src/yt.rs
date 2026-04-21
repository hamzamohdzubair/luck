use anyhow::{Context, Result};
use rand::Rng;
use std::process::{Command, Stdio};

use crate::utils::format_duration;

pub fn is_youtube_url(url: &str) -> bool {
    url.contains("youtube.com") || url.contains("youtu.be")
}

pub fn yt_is_playlist(url: &str) -> bool {
    if url.contains("/playlist?") {
        return true;
    }
    if url.contains("watch?") || url.contains("youtu.be/") {
        return false;
    }
    url.contains("list=")
}

pub fn fetch_yt_title(url: &str, is_playlist: bool) -> Option<String> {
    let field = if is_playlist { "playlist_title" } else { "title" };
    let output = Command::new("yt-dlp")
        .args(["--quiet", "--print", field, "--no-playlist", url])
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if output.status.success() {
        let t = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !t.is_empty() && t != "NA" {
            return Some(t);
        }
    }
    None
}

pub fn fetch_playlist_info(url: &str) -> (Option<String>, Option<usize>) {
    let output = Command::new("yt-dlp")
        .args([
            "--quiet", "--flat-playlist", "--playlist-items", "1",
            "--print", "playlist_title", "--print", "playlist_count", url,
        ])
        .stderr(Stdio::null())
        .output();
    match output {
        Ok(out) if out.status.success() => {
            let text = String::from_utf8_lossy(&out.stdout);
            let mut lines = text.lines();
            let title = lines
                .next()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty() && s != "NA");
            let count = lines
                .next()
                .and_then(|s| s.trim().parse::<usize>().ok())
                .filter(|&n| n > 0);
            (title, count)
        }
        _ => (None, None),
    }
}

pub fn get_video_duration(url: &str) -> Option<u64> {
    let output = Command::new("yt-dlp")
        .args(["--quiet", "--print", "duration", "--no-playlist", url])
        .stderr(Stdio::null())
        .output()
        .ok()?;
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<f64>()
        .ok()
        .map(|d| d as u64)
}


pub fn with_random_timestamp(url: &str, rng: &mut impl rand::Rng) -> String {
    match get_video_duration(url) {
        Some(d) if d > 0 => {
            let t = rng.gen_range(0..d);
            println!("⏱ {} / {}", format_duration(t), format_duration(d));
            if url.contains('?') {
                format!("{}&t={}", url, t)
            } else {
                format!("{}?t={}", url, t)
            }
        }
        _ => {
            eprintln!("⚠ Could not get video duration (is yt-dlp installed?), opening without timestamp");
            url.to_string()
        }
    }
}

pub fn pick_random_playlist_video(
    url: &str,
    video_count: Option<usize>,
) -> Result<(String, usize, usize)> {
    if let Some(total) = video_count {
        let idx = rand::thread_rng().gen_range(1..=total);
        let output = Command::new("yt-dlp")
            .args([
                "--quiet", "--flat-playlist", "--playlist-items",
                &idx.to_string(), "--print", "url", url,
            ])
            .stderr(Stdio::null())
            .output()
            .context("Failed to run yt-dlp (is it installed?)")?;
        let video_url = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !video_url.is_empty() {
            return Ok((video_url, idx, total));
        }
    }
    let output = Command::new("yt-dlp")
        .args(["--quiet", "--flat-playlist", "--print", "url", url])
        .stderr(Stdio::null())
        .output()
        .context("Failed to run yt-dlp (is it installed?)")?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let urls: Vec<&str> = stdout.lines().filter(|l| !l.is_empty()).collect();
    if urls.is_empty() {
        anyhow::bail!("No videos found in playlist");
    }
    let total = urls.len();
    let idx = rand::thread_rng().gen_range(0..total);
    Ok((urls[idx].to_string(), idx + 1, total))
}

pub fn get_clipboard() -> Result<String> {
    let attempts = [
        ("wl-paste", vec!["--no-newline"]),
        ("xclip", vec!["-selection", "clipboard", "-o"]),
        ("xsel", vec!["--clipboard", "--output"]),
        ("powershell.exe", vec!["-command", "Get-Clipboard"]),
    ];
    for (cmd, args) in &attempts {
        if let Ok(out) = Command::new(cmd).args(args).output() {
            if out.status.success() {
                let t = String::from_utf8_lossy(&out.stdout).trim().to_string();
                if !t.is_empty() {
                    return Ok(t);
                }
            }
        }
    }
    anyhow::bail!(
        "Could not read clipboard. Install xclip, xsel, wl-paste, or use WSL with powershell."
    )
}
