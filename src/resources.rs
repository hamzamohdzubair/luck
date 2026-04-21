use anyhow::{Context, Result};
use rand::Rng;
use rusqlite::{Connection, params};
use std::path::PathBuf;
use walkdir::WalkDir;

use crate::tags::{TYPE_TAG_MAP, apply_type_tags, apply_named_tag};
use crate::utils::{to_windows_path, wsl_path};
use crate::yt::{pick_random_playlist_video, with_random_timestamp};

pub const TYPE_PLAYLIST: &str = "playlist";
pub const TYPE_VIDEO:    &str = "video";
pub const TYPE_PDF:      &str = "pdf";
pub const TYPE_BOOK:     &str = "book";
pub const TYPE_LINK:     &str = "link";
pub const TYPE_DIR:      &str = "dir";

#[derive(Debug)]
pub enum Resource {
    PdfFile { id: i64, name: String, path: PathBuf, pages: u32 },
    PhysicalBook { id: i64, title: String, pages: u32 },
    YouTubePlaylist { id: i64, name: String, url: String, video_count: Option<usize> },
    YouTubeVideo { id: i64, name: String, url: String },
    Link { id: i64, name: String, url: String },
    Directory { id: i64, name: String, url: String },
}

impl Resource {
    pub fn id(&self) -> i64 {
        match self {
            Resource::PdfFile { id, .. } => *id,
            Resource::PhysicalBook { id, .. } => *id,
            Resource::YouTubePlaylist { id, .. } => *id,
            Resource::YouTubeVideo { id, .. } => *id,
            Resource::Link { id, .. } => *id,
            Resource::Directory { id, .. } => *id,
        }
    }
}

pub fn load_all_resources(conn: &Connection) -> Result<Vec<Resource>> {
    let mut out: Vec<Resource> = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT id, type, name, url, path, pages, video_count FROM resources ORDER BY id",
    )?;
    let mapped = stmt.query_map([], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, Option<String>>(3)?,
            r.get::<_, Option<String>>(4)?,
            r.get::<_, Option<u32>>(5)?,
            r.get::<_, Option<usize>>(6)?,
        ))
    })?;
    for row in mapped {
        let (id, rtype, name, url, path, pages, video_count) = row?;
        let resource = match rtype.as_str() {
            TYPE_PLAYLIST => Resource::YouTubePlaylist {
                id, name, url: url.unwrap_or_default(), video_count,
            },
            TYPE_VIDEO => Resource::YouTubeVideo {
                id, name, url: url.unwrap_or_default(),
            },
            TYPE_PDF => Resource::PdfFile {
                id, name, path: PathBuf::from(path.unwrap_or_default()), pages: pages.unwrap_or(0),
            },
            TYPE_BOOK => Resource::PhysicalBook {
                id, title: name, pages: pages.unwrap_or(0),
            },
            TYPE_LINK => Resource::Link {
                id, name, url: url.unwrap_or_default(),
            },
            TYPE_DIR => Resource::Directory {
                id, name, url: url.unwrap_or_default(),
            },
            _ => continue,
        };
        out.push(resource);
    }
    Ok(out)
}

pub fn migrate_expand_pdf_folders(conn: &Connection) -> Result<()> {
    let needed = conn.execute(
        "INSERT OR IGNORE INTO migrations (name) VALUES ('expand_pdf_folders')",
        [],
    )? > 0;
    if !needed {
        return Ok(());
    }

    let type_tag_names: std::collections::HashSet<&str> =
        TYPE_TAG_MAP.iter().map(|(n, _)| *n).collect();

    let folders: Vec<(i64, String)> = {
        let mut stmt = conn.prepare("SELECT id, path FROM resources WHERE type='pdf'")?;
        let v: Vec<(i64, String)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get::<_, String>(1)?)))?
            .filter_map(|r| r.ok())
            .filter(|(_, path)| !path.starts_with("http") && std::path::Path::new(path).is_dir())
            .collect();
        v
    };

    for (folder_id, folder_path) in &folders {
        let tag_names: Vec<String> = {
            let mut stmt = conn.prepare(
                "SELECT t.name FROM tags t JOIN resource_tags rt ON rt.tag_id = t.id \
                 WHERE rt.resource_id = ?1",
            )?;
            let v: Vec<String> = stmt
                .query_map(params![folder_id], |r| r.get(0))?
                .filter_map(|r| r.ok())
                .collect();
            v
        };

        let folder = wsl_path(std::path::Path::new(folder_path));
        let pdfs: Vec<PathBuf> = WalkDir::new(&folder)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .and_then(|x| x.to_str())
                    .map(|x| x.eq_ignore_ascii_case("pdf"))
                    .unwrap_or(false)
            })
            .map(|e| e.path().to_path_buf())
            .collect();

        if pdfs.is_empty() {
            continue;
        }

        let mut inserted: Vec<i64> = Vec::new();
        for pdf in &pdfs {
            let pdf_path = pdf.to_string_lossy().to_string();
            let exists: bool = conn
                .query_row(
                    "SELECT COUNT(*) FROM resources WHERE type='pdf' AND path=?1",
                    params![pdf_path],
                    |r| r.get::<_, i64>(0),
                )
                .unwrap_or(0)
                > 0;
            if exists {
                continue;
            }
            let name = pdf
                .file_stem()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| pdf_path.clone());
            let pages = lopdf::Document::load(pdf)
                .ok()
                .map(|d| d.get_pages().len() as u32)
                .unwrap_or(0);
            conn.execute(
                "INSERT INTO resources (type, name, path, pages) VALUES ('pdf', ?1, ?2, ?3)",
                params![name, pdf_path, pages],
            )?;
            inserted.push(conn.last_insert_rowid());
        }

        if inserted.is_empty() {
            continue;
        }

        for &id in &inserted {
            apply_type_tags(conn, "pdf", id)?;
            for tag_name in &tag_names {
                if !type_tag_names.contains(tag_name.as_str()) {
                    apply_named_tag(conn, id, tag_name)?;
                }
            }
        }

        conn.execute("DELETE FROM resource_tags WHERE resource_id=?1", params![folder_id])?;
        conn.execute("DELETE FROM resources WHERE id=?1", params![folder_id])?;
    }

    Ok(())
}

fn find_sumatra() -> Option<PathBuf> {
    let known = [
        "/mnt/c/Program Files/SumatraPDF/SumatraPDF.exe",
        "/mnt/c/Program Files (x86)/SumatraPDF/SumatraPDF.exe",
    ];
    for loc in &known {
        let p = std::path::Path::new(loc);
        if p.exists() {
            return Some(p.to_path_buf());
        }
    }
    let out = std::process::Command::new("powershell.exe")
        .args([
            "-NoProfile",
            "-command",
            "$p = \"$env:LOCALAPPDATA\\SumatraPDF\\SumatraPDF.exe\"; if (Test-Path $p) { $p }",
        ])
        .output()
        .ok()?;
    if out.status.success() {
        let win = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !win.is_empty() {
            let wsl = wsl_path(std::path::Path::new(&win));
            if wsl.exists() {
                return Some(wsl);
            }
        }
    }
    None
}

fn open_pdf_at_page(win_path: &PathBuf, page: u32) -> Result<()> {
    if let Some(sumatra) = find_sumatra() {
        std::process::Command::new(&sumatra)
            .args(["-page", &page.to_string(), &win_path.to_string_lossy().to_string()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .context("Failed to launch SumatraPDF")?;
        return Ok(());
    }
    eprintln!("⚠ SumatraPDF not found; opening without page targeting");
    opener::open(win_path).context("Failed to open PDF")?;
    Ok(())
}

pub fn dispatch_resource(resource: &Resource, rng: &mut impl Rng) -> Result<()> {
    match resource {
        Resource::PdfFile { name, path, pages, .. } => {
            let path = &wsl_path(path);
            let actual_pages = if *pages > 0 {
                *pages
            } else {
                lopdf::Document::load(path).ok().map(|d| d.get_pages().len() as u32).unwrap_or(0)
            };
            let win_path = to_windows_path(path);
            if actual_pages > 0 {
                let page = rng.gen_range(1..=actual_pages);
                println!("📄 {} — page {}/{}", name, page, actual_pages);
                open_pdf_at_page(&win_path, page)?;
            } else {
                println!("📄 {}", name);
                opener::open(&win_path).context("Failed to open PDF")?;
            }
        }
        Resource::PhysicalBook { title, pages, .. } => {
            let page = rng.gen_range(1..=*pages);
            println!("📖 Open \"{}\" to page {}/{}", title, page, pages);
        }
        Resource::YouTubePlaylist { name, url, video_count, .. } => {
            println!("🎬 {}", name);
            let (video_url, n, total) = pick_random_playlist_video(url, *video_count)?;
            println!("📺 Lecture {}/{}", n, total);
            let final_url = with_random_timestamp(&video_url, rng);
            println!("🔗 {}", final_url);
            opener::open_browser(&final_url).context("Failed to open browser")?;
        }
        Resource::YouTubeVideo { name, url, .. } => {
            println!("🎥 {}", name);
            let final_url = with_random_timestamp(url, rng);
            println!("🔗 {}", final_url);
            opener::open_browser(&final_url).context("Failed to open browser")?;
        }
        Resource::Link { name, url, .. } => {
            println!("🔗 {}", name);
            println!("   {}", url);
            opener::open_browser(url).context("Failed to open browser")?;
        }
        Resource::Directory { name, url, .. } => {
            println!("📁 {}", name);
            println!("   {}", url);
            opener::open_browser(url).context("Failed to open browser")?;
        }
    }
    Ok(())
}
