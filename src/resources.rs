use anyhow::{Context, Result};
use rand::Rng;
use rand::seq::SliceRandom;
use rusqlite::{Connection, params};
use std::path::PathBuf;
use walkdir::WalkDir;

use crate::llm::extract_pdf_title_author;
use crate::tags::{TYPE_TAG_MAP, apply_type_tags, apply_named_tag};
use crate::utils::{books_dir, is_wsl, to_windows_path, to_windows_path_any, wsl_path};
use crate::yt::{pick_random_playlist_video, with_random_timestamp};

pub const TYPE_PLAYLIST: &str = "playlist";
pub const TYPE_VIDEO:    &str = "video";
pub const TYPE_PDF:      &str = "pdf";
pub const TYPE_BOOK:     &str = "book";
pub const TYPE_LINK:     &str = "link";
pub const TYPE_DIR:      &str = "dir";

#[derive(Debug)]
pub enum Resource {
    PdfFolder { id: i64, name: String, path: PathBuf },
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
            Resource::PdfFolder { id, .. } => *id,
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
            TYPE_PDF => {
                let path_str = path.unwrap_or_default();
                let path_buf = PathBuf::from(&path_str);
                if path_str.to_lowercase().ends_with(".pdf") {
                    Resource::PdfFile { id, name, path: path_buf, pages: pages.unwrap_or(0) }
                } else {
                    Resource::PdfFolder { id, name, path: path_buf }
                }
            }
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

/// Scan a directory for PDF files via WSL filesystem.
pub fn scan_pdfs(folder: &std::path::Path) -> Vec<PathBuf> {
    WalkDir::new(folder)
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
        .collect()
}

fn ps_escape(s: &str) -> String {
    s.replace('\'', "''")
}

/// Scan a directory for PDF files via PowerShell, for paths on virtual drives
/// (e.g. Google Drive for Desktop) that are inaccessible from WSL.
/// Returns Windows-style paths.
pub fn scan_pdfs_win(win_path: &PathBuf) -> Vec<PathBuf> {
    let path_str = win_path.to_string_lossy();
    let script = format!(
        "[Console]::OutputEncoding=[System.Text.Encoding]::UTF8;\
         Get-ChildItem -Recurse -Filter '*.pdf' -Path '{}' -ErrorAction SilentlyContinue \
         | Select-Object -ExpandProperty FullName",
        ps_escape(&path_str)
    );
    let out = std::process::Command::new("powershell.exe")
        .args(["-NoProfile", "-NonInteractive", "-Command", &script])
        .output();
    match out {
        Ok(o) if !o.stdout.is_empty() => String::from_utf8_lossy(&o.stdout)
            .lines()
            .map(|l| l.trim())
            .filter(|l| !l.is_empty() && l.to_lowercase().ends_with(".pdf"))
            .map(|l| PathBuf::from(l))
            .collect(),
        _ => vec![],
    }
}

/// Get page count for a Windows-only path, checking `pdf_file_cache` first.
/// On cache miss, copies to %TEMP% via PowerShell, counts with lopdf, caches, cleans up.
fn get_pdf_pages_win_cached(conn: &Connection, win_path: &PathBuf) -> u32 {
    let key = win_path.to_string_lossy().to_string();
    if let Ok(cached) = conn.query_row(
        "SELECT pages FROM pdf_file_cache WHERE path = ?1",
        params![key],
        |r| r.get::<_, u32>(0),
    ) {
        return cached;
    }
    let pages = pdf_pages_win(win_path);
    let _ = conn.execute(
        "INSERT OR REPLACE INTO pdf_file_cache (path, pages) VALUES (?1, ?2)",
        params![key, pages],
    );
    pages
}

/// Copy a Windows-only PDF to %TEMP%, count pages with lopdf, delete the copy.
fn pdf_pages_win(win_path: &PathBuf) -> u32 {
    if !is_wsl() { return 0; }
    let script = format!(
        "$dst = \"$env:TEMP\\luck_pdf_tmp.pdf\"; Copy-Item '{}' $dst -Force; $dst",
        ps_escape(&win_path.to_string_lossy())
    );
    let out = std::process::Command::new("powershell.exe")
        .args(["-NoProfile", "-NonInteractive", "-Command", &script])
        .output();
    let win_tmp = match out {
        Ok(o) if o.status.success() => {
            let s = String::from_utf8_lossy(&o.stdout).trim().to_string();
            if s.is_empty() { return 0; }
            PathBuf::from(s)
        }
        _ => return 0,
    };
    let wsl_tmp = wsl_path(&win_tmp);
    let pages = lopdf::Document::load(&wsl_tmp)
        .ok()
        .map(|d| d.get_pages().len() as u32)
        .unwrap_or(0);
    let _ = std::fs::remove_file(&wsl_tmp);
    pages
}

/// Get page count for a PDF, using the cache table to avoid re-loading.
pub fn get_pdf_pages(conn: &Connection, pdf_path: &std::path::Path) -> u32 {
    let key = pdf_path.to_string_lossy().to_string();
    if let Ok(cached) = conn.query_row(
        "SELECT pages FROM pdf_file_cache WHERE path = ?1",
        params![key],
        |r| r.get::<_, u32>(0),
    ) {
        return cached;
    }
    let pages = lopdf::Document::load(pdf_path)
        .ok()
        .map(|d| d.get_pages().len() as u32)
        .unwrap_or(0);
    let _ = conn.execute(
        "INSERT OR REPLACE INTO pdf_file_cache (path, pages) VALUES (?1, ?2)",
        params![key, pages],
    );
    pages
}

/// Extract the filename stem from a path, handling Windows `\` separators correctly
/// even when running on Linux where PathBuf only knows `/`.
pub fn path_stem(path: &PathBuf) -> String {
    let s = path.to_string_lossy();
    let filename = s.rsplit(['\\', '/']).next().unwrap_or(&s);
    match filename.rfind('.') {
        Some(i) => filename[..i].to_string(),
        None => filename.to_string(),
    }
}

/// Extract title, optional author, and page count from a PDF.
/// Tries the WSL path first; on WSL, falls back to a temp-copy via PowerShell.
/// Title falls back to text heuristic then filename stem.
pub fn extract_pdf_metadata(wsl_pdf: &PathBuf, win_pdf: &PathBuf) -> (String, Option<String>, u32) {
    // Determine which path to actually read from
    let read_path: Option<PathBuf> = if wsl_pdf.exists() {
        Some(wsl_pdf.clone())
    } else if is_wsl() {
        // Copy from virtual drive to %TEMP% via PowerShell
        let script = format!(
            "$dst = \"$env:TEMP\\luck_pdf_tmp.pdf\"; Copy-Item '{}' $dst -Force; $dst",
            ps_escape(&win_pdf.to_string_lossy())
        );
        let out = std::process::Command::new("powershell.exe")
            .args(["-NoProfile", "-NonInteractive", "-Command", &script])
            .output();
        match out {
            Ok(o) if o.status.success() => {
                let s = String::from_utf8_lossy(&o.stdout).trim().to_string();
                if !s.is_empty() { Some(wsl_path(std::path::Path::new(&s))) } else { None }
            }
            _ => None,
        }
    } else {
        None
    };

    let is_temp = read_path.as_ref().map(|p| {
        p.to_string_lossy().contains("AppData") || p.to_string_lossy().contains("/tmp")
            || p.to_string_lossy().contains("Temp") || p.to_string_lossy().contains("luck_pdf_tmp")
    }).unwrap_or(false);

    let doc_opt = read_path.as_ref().and_then(|p| lopdf::Document::load(p).ok());
    let pages   = doc_opt.as_ref().map(|d| d.get_pages().len() as u32).unwrap_or(0);

    let isbn = doc_opt.as_ref().and_then(|d| crate::llm::extract_isbn_from_doc(d));

    let (title_meta, author_meta) = doc_opt
        .as_ref()
        .map(|d| extract_pdf_title_author(d))
        .unwrap_or((None, None));

    // Clean up temp file
    if is_temp {
        if let Some(rp) = &read_path {
            let _ = std::fs::remove_file(rp);
        }
    }

    // Priority: ISBN lookup → metadata title → stem search → stem fallback
    let (title, author) = if let Some(ref i) = isbn {
        match crate::llm::lookup_book_by_isbn(i) {
            Some((t, a)) => (Some(t), a),
            None => (title_meta, author_meta),
        }
    } else if title_meta.is_some() {
        (title_meta, author_meta)
    } else {
        let stem = path_stem(win_pdf);
        match crate::llm::lookup_book_title_online(&stem) {
            Some((t, a)) => (Some(t), a),
            None => (None, None),
        }
    };

    let final_title = title.unwrap_or_else(|| path_stem(win_pdf));

    (final_title, author, pages)
}

fn sanitize_filename(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '\\' | '/' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c => c,
        })
        .collect::<String>()
        .trim()
        .to_string()
}

/// Copy a PDF to the local books store (`~/.local/share/luck/books/`).
/// Filename is `<Title> - <Author>.pdf` or `<Title>.pdf`. Appends ` (2)`, ` (3)` on collision.
pub fn copy_pdf_to_store(
    src_win: &PathBuf,
    src_wsl: &PathBuf,
    title: &str,
    author: Option<&str>,
) -> Result<PathBuf> {
    let store = books_dir();

    let base = if let Some(a) = author {
        format!("{} - {}", sanitize_filename(title), sanitize_filename(a))
    } else {
        sanitize_filename(title)
    };

    let mut dest = store.join(format!("{}.pdf", base));
    let mut counter = 2u32;
    while dest.exists() {
        dest = store.join(format!("{} ({}).pdf", base, counter));
        counter += 1;
    }

    if src_wsl.exists() {
        std::fs::copy(src_wsl, &dest)
            .with_context(|| format!("copy {} → {}", src_wsl.display(), dest.display()))?;
    } else if is_wsl() {
        let dest_win = to_windows_path_any(&dest);
        let script = format!(
            "Copy-Item '{}' '{}'",
            ps_escape(&src_win.to_string_lossy()),
            ps_escape(&dest_win.to_string_lossy())
        );
        let out = std::process::Command::new("powershell.exe")
            .args(["-NoProfile", "-NonInteractive", "-Command", &script])
            .output()
            .context("PowerShell copy failed")?;
        if !out.status.success() {
            anyhow::bail!(
                "PowerShell copy failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
    } else {
        anyhow::bail!("Source PDF not accessible: {}", src_wsl.display());
    }

    Ok(dest)
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
            .filter(|(_, path)| !path.starts_with("http") && !path.to_lowercase().ends_with(".pdf"))
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

pub fn migrate_expand_pdf_folders_v2(conn: &Connection) -> Result<()> {
    let needed = conn.execute(
        "INSERT OR IGNORE INTO migrations (name) VALUES ('expand_pdf_folders_v2')",
        [],
    )? > 0;
    if !needed {
        return Ok(());
    }

    let type_tag_names: std::collections::HashSet<&str> =
        TYPE_TAG_MAP.iter().map(|(n, _)| *n).collect();

    let folders: Vec<(i64, String)> = {
        let mut stmt = conn.prepare(
            "SELECT id, path FROM resources WHERE type='pdf' AND path NOT LIKE '%.pdf'",
        )?;
        let rows: Vec<(i64, String)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get::<_, String>(1)?)))?
            .filter_map(|r| r.ok())
            .collect();
        rows
    };

    if folders.is_empty() {
        return Ok(());
    }

    for (folder_id, folder_path) in &folders {
        let tag_names: Vec<String> = {
            let mut stmt = conn.prepare(
                "SELECT t.name FROM tags t JOIN resource_tags rt ON rt.tag_id = t.id \
                 WHERE rt.resource_id = ?1",
            )?;
            let rows: Vec<String> = stmt
                .query_map(params![folder_id], |r| r.get(0))?
                .filter_map(|r| r.ok())
                .collect();
            rows
        };

        let wsl_folder = wsl_path(std::path::Path::new(folder_path));
        let win_folder = to_windows_path(&wsl_folder);

        let pdf_pairs: Vec<(PathBuf, PathBuf)> = {
            let wsl_pdfs = scan_pdfs(&wsl_folder);
            if !wsl_pdfs.is_empty() {
                wsl_pdfs.iter().map(|p| (p.clone(), to_windows_path(p))).collect()
            } else if is_wsl() {
                let win_pdfs = scan_pdfs_win(&win_folder);
                win_pdfs.iter().map(|p| (PathBuf::new(), p.clone())).collect()
            } else {
                vec![]
            }
        };

        if pdf_pairs.is_empty() {
            eprintln!("Migration v2: no PDFs found in '{}', leaving as-is.", folder_path);
            continue;
        }

        let existing_titles: std::collections::HashSet<String> = {
            let mut stmt = conn.prepare("SELECT LOWER(name) FROM resources WHERE type='pdf'")?;
            let rows: Vec<String> = stmt
                .query_map([], |r| r.get::<_, String>(0))?
                .filter_map(|r| r.ok())
                .collect();
            rows.into_iter().collect()
        };

        eprint!("Migrating '{}' ({} PDFs)...", folder_path, pdf_pairs.len());

        let mut inserted: Vec<i64> = Vec::new();
        let mut seen_titles = std::collections::HashSet::new();

        for (wsl_pdf, win_pdf) in &pdf_pairs {
            let (title, author, pages) = extract_pdf_metadata(wsl_pdf, win_pdf);
            let title_lower = title.to_lowercase();
            if existing_titles.contains(&title_lower) || seen_titles.contains(&title_lower) {
                continue;
            }
            seen_titles.insert(title_lower);

            let local_path = match copy_pdf_to_store(win_pdf, wsl_pdf, &title, author.as_deref()) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("\n  Failed to copy '{}': {}", title, e);
                    continue;
                }
            };

            let local_str = local_path.to_string_lossy().to_string();
            conn.execute(
                "INSERT INTO resources (type, name, path, pages) VALUES ('pdf', ?1, ?2, ?3)",
                params![title, local_str, pages],
            )?;
            inserted.push(conn.last_insert_rowid());
        }

        eprintln!(" done ({} new).", inserted.len());

        for &id in &inserted {
            apply_type_tags(conn, TYPE_PDF, id)?;
            for tag_name in &tag_names {
                if !type_tag_names.contains(tag_name.as_str()) {
                    apply_named_tag(conn, id, tag_name)?;
                }
            }
        }

        if !inserted.is_empty() {
            conn.execute(
                "DELETE FROM resource_tags WHERE resource_id=?1",
                params![folder_id],
            )?;
            conn.execute("DELETE FROM resources WHERE id=?1", params![folder_id])?;
        }
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

pub fn dispatch_resource(conn: &Connection, resource: &Resource, rng: &mut impl Rng) -> Result<()> {
    match resource {
        Resource::PdfFolder { name, path, .. } => {
            let wsl_folder = wsl_path(path);
            let win_folder = to_windows_path(&wsl_folder);

            // Try WSL-accessible scan first; on WSL, fall back to PowerShell for
            // virtual drives (e.g. Google Drive) that WSL cannot see via /mnt/<drive>/.
            let (pdfs_wsl, pdfs_win): (Vec<PathBuf>, Vec<PathBuf>) = {
                let w = scan_pdfs(&wsl_folder);
                if w.is_empty() && is_wsl() { (vec![], scan_pdfs_win(&win_folder)) } else { (w, vec![]) }
            };

            if pdfs_wsl.is_empty() && pdfs_win.is_empty() {
                anyhow::bail!("No PDF files found in {} ({})", name, win_folder.display());
            }

            // Cache folder PDF count for `luck stats` (no page totals — those
            // are built lazily from pdf_file_cache as individual files get picked).
            let folder_key = path.to_string_lossy().to_string();
            let pdf_count = if !pdfs_wsl.is_empty() { pdfs_wsl.len() } else { pdfs_win.len() };
            let _ = conn.execute(
                "INSERT OR REPLACE INTO pdf_scan_cache (path, pdf_count, total_pages) \
                 VALUES (?1, ?2, COALESCE((SELECT total_pages FROM pdf_scan_cache WHERE path=?1), 0))",
                params![folder_key, pdf_count as i64],
            );

            // Pick from whichever scan succeeded.
            let (pdf_wsl, pdf_win): (Option<&PathBuf>, Option<&PathBuf>) = if !pdfs_wsl.is_empty() {
                (pdfs_wsl.choose(rng), None)
            } else {
                (None, pdfs_win.choose(rng))
            };

            let (win_path, pdf_name, pages) = if let Some(pdf) = pdf_wsl {
                let pdf_name = pdf.file_stem().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
                let pg = get_pdf_pages(conn, pdf);
                (to_windows_path(pdf), pdf_name, pg)
            } else {
                let pdf = pdf_win.unwrap();
                let pdf_name = pdf.file_stem().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
                let pg = get_pdf_pages_win_cached(conn, pdf);
                (pdf.clone(), pdf_name, pg)
            };

            if pages > 0 {
                let page = rng.gen_range(1..=pages);
                println!("📁 {}  →  📄 {} — page {}/{}", name, pdf_name, page, pages);
                open_pdf_at_page(&win_path, page)?;
            } else {
                println!("📁 {}  →  📄 {}", name, pdf_name);
                opener::open(&win_path).context("Failed to open PDF")?;
            }
        }
        Resource::PdfFile { name, path, pages, .. } => {
            let path = &wsl_path(path);
            let actual_pages = if *pages > 0 { *pages } else { get_pdf_pages(conn, path) };
            let win_path = to_windows_path_any(path);
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
