use anyhow::{Context, Result};
use rusqlite::{Connection, params};

use crate::db::open_db;
use crate::llm::{build_pdf_folder_metadata, prompt_and_apply_topic_tags, prompt_link_type_tags, suggest_topic_tags};
use crate::resources::{
    TYPE_LINK, TYPE_PDF, TYPE_BOOK, TYPE_PLAYLIST, TYPE_VIDEO,
    scan_pdfs, scan_pdfs_win, extract_pdf_metadata, copy_pdf_to_store,
};
use crate::tags::{apply_type_tags, apply_named_tags};
use crate::utils::{is_wsl, to_windows_path, wsl_path};
use crate::yt::{
    fetch_playlist_info, fetch_yt_title, get_video_duration, get_clipboard, is_youtube_url, yt_is_playlist,
};

fn is_drive_url(url: &str) -> bool {
    url.contains("drive.google.com") || url.contains("docs.google.com")
}

pub fn add(
    from_clipboard: bool,
    dir: Option<String>,
    pages: Option<u32>,
    name: Option<String>,
) -> Result<()> {
    let conn = open_db()?;

    if from_clipboard {
        add_from_clipboard(&conn, name)?;
    } else if let Some(path) = dir {
        add_pdf_folder(&conn, path)?;
    } else if let Some(p) = pages {
        add_physical_book(&conn, name, p)?;
    } else {
        anyhow::bail!("Specify one of: -l (link), -d <dir>, -p <pages>");
    }

    Ok(())
}

fn add_from_clipboard(conn: &Connection, name: Option<String>) -> Result<()> {
    let url = get_clipboard()?;
    if !url.starts_with("http://") && !url.starts_with("https://") {
        anyhow::bail!("Clipboard content doesn't look like a URL: {}", url);
    }

    if is_drive_url(&url) {
        return add_dir_link(conn, url, name);
    }

    if !is_youtube_url(&url) {
        return add_link(conn, url, name);
    }

    if yt_is_playlist(&url) {
        add_yt_playlist(conn, url, name)
    } else {
        add_yt_video(conn, url, name)
    }
}

fn add_dir_link(conn: &Connection, url: String, name: Option<String>) -> Result<()> {
    let exists: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM resources WHERE type='dir' AND url = ?1",
            params![url],
            |r| r.get::<_, u32>(0),
        )
        .map(|c| c > 0)?;
    if exists {
        anyhow::bail!("Already in Directories: {}", url);
    }

    let resolved_name = name.unwrap_or_else(|| url.clone());
    conn.execute(
        "INSERT INTO resources (type, name, url) VALUES ('dir', ?1, ?2)",
        params![resolved_name, url],
    )?;
    let id = conn.last_insert_rowid();
    println!("Added directory: {}", resolved_name);

    let metadata = format!("Name: {}\nURL: {}", resolved_name, url);
    prompt_and_apply_topic_tags(conn, id, &metadata)?;
    Ok(())
}

fn add_link(conn: &Connection, url: String, name: Option<String>) -> Result<()> {
    let exists: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM resources WHERE type='link' AND url = ?1",
            params![url],
            |r| r.get::<_, u32>(0),
        )
        .map(|c| c > 0)?;
    if exists {
        anyhow::bail!("Already in Links: {}", url);
    }

    let resolved_name = name.unwrap_or_else(|| url.clone());
    conn.execute(
        "INSERT INTO resources (type, name, url) VALUES ('link', ?1, ?2)",
        params![resolved_name, url],
    )?;
    let id = conn.last_insert_rowid();
    println!("Added to Links: {}", resolved_name);

    apply_type_tags(conn, TYPE_LINK, id)?;
    prompt_link_type_tags(conn, id)?;

    let metadata = format!("Name: {}\nURL: {}", resolved_name, url);
    prompt_and_apply_topic_tags(conn, id, &metadata)?;
    Ok(())
}

fn add_yt_playlist(conn: &Connection, url: String, name: Option<String>) -> Result<()> {
    let exists: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM resources WHERE type='playlist' AND url = ?1",
            params![url],
            |r| r.get::<_, u32>(0),
        )
        .map(|c| c > 0)?;
    if exists {
        anyhow::bail!("Already in YouTube Playlists: {}", url);
    }

    eprint!("Fetching playlist info...");
    let (title, count) = fetch_playlist_info(&url);
    eprintln!(" done");
    let resolved_name = name.or(title).unwrap_or_else(|| url.clone());
    conn.execute(
        "INSERT INTO resources (type, name, url, video_count) VALUES ('playlist', ?1, ?2, ?3)",
        params![resolved_name, url, count],
    )?;
    let id = conn.last_insert_rowid();
    println!("Added to YouTube Playlists: {}", resolved_name);
    apply_type_tags(conn, TYPE_PLAYLIST, id)?;
    let metadata = format!("Title: {}\nURL: {}", resolved_name, url);
    prompt_and_apply_topic_tags(conn, id, &metadata)?;
    Ok(())
}

fn add_yt_video(conn: &Connection, url: String, name: Option<String>) -> Result<()> {
    let exists: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM resources WHERE type='video' AND url = ?1",
            params![url],
            |r| r.get::<_, u32>(0),
        )
        .map(|c| c > 0)?;
    if exists {
        anyhow::bail!("Already in YouTube Videos: {}", url);
    }

    let resolved_name = if let Some(n) = name {
        n
    } else {
        eprint!("Fetching title...");
        let t = fetch_yt_title(&url, false).unwrap_or_else(|| url.clone());
        eprintln!(" done");
        t
    };
    conn.execute(
        "INSERT INTO resources (type, name, url) VALUES ('video', ?1, ?2)",
        params![resolved_name, url],
    )?;
    let id = conn.last_insert_rowid();
    eprint!("Fetching duration...");
    let dur = get_video_duration(&url).unwrap_or(0);
    conn.execute(
        "INSERT OR REPLACE INTO yt_duration_cache (url, duration_secs) VALUES (?1, ?2)",
        params![url, dur as i64],
    )?;
    eprintln!(" done");
    println!("Added to YouTube Videos: {}", resolved_name);
    apply_type_tags(conn, TYPE_VIDEO, id)?;
    let metadata = format!("Title: {}\nURL: {}", resolved_name, url);
    prompt_and_apply_topic_tags(conn, id, &metadata)?;
    Ok(())
}

fn add_pdf_folder(conn: &Connection, path: String) -> Result<()> {
    let expanded = shellexpand::tilde(&path).to_string();
    let expanded = expanded.trim_end_matches(['/', '\\']).to_string();
    let wsl_folder = wsl_path(std::path::Path::new(&expanded));
    let win_folder = to_windows_path(&wsl_folder);

    // Check: folder already tracked?
    let tracked: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM tracked_folders WHERE path=?1",
            params![expanded],
            |r| r.get::<_, u32>(0),
        )
        .map(|c| c > 0)?;
    if tracked {
        anyhow::bail!("PDF folder already tracked: {}", expanded);
    }

    // Hard error: subfolder or superset of already-tracked folder
    let tracked_paths: Vec<String> = {
        let mut stmt = conn.prepare("SELECT path FROM tracked_folders")?;
        let rows: Vec<String> = stmt.query_map([], |r| r.get(0))?
            .filter_map(|r| r.ok())
            .collect();
        rows
    };
    let sep = if expanded.contains('\\') { '\\' } else { '/' };
    let new_prefix = format!("{}{}", expanded, sep);
    for existing in &tracked_paths {
        let existing_prefix = format!("{}{}", existing, sep);
        if expanded.starts_with(&existing_prefix) {
            anyhow::bail!(
                "'{}' is a subfolder of already-tracked '{}'. \
                 This would cause duplicate PDFs in the database.",
                expanded, existing
            );
        } else if existing.starts_with(&new_prefix) {
            anyhow::bail!(
                "Already-tracked '{}' is a subfolder of '{}'. \
                 This would cause duplicate PDFs in the database.",
                existing, expanded
            );
        }
    }

    // Scan PDFs
    let pdf_pairs: Vec<(std::path::PathBuf, std::path::PathBuf)> = {
        let wsl_pdfs = scan_pdfs(&wsl_folder);
        if !wsl_pdfs.is_empty() {
            wsl_pdfs.iter().map(|p| (p.clone(), to_windows_path(p))).collect()
        } else if is_wsl() {
            let win_pdfs = scan_pdfs_win(&win_folder);
            win_pdfs.iter().map(|p| (std::path::PathBuf::new(), p.clone())).collect()
        } else {
            vec![]
        }
    };

    if pdf_pairs.is_empty() {
        anyhow::bail!("No PDF files found in: {}", expanded);
    }

    // Register tracked folder
    conn.execute(
        "INSERT OR IGNORE INTO tracked_folders (path) VALUES (?1)",
        params![expanded],
    )?;

    // Load existing titles for dedup (case-insensitive)
    let existing_titles: std::collections::HashSet<String> = {
        let mut stmt = conn.prepare("SELECT LOWER(name) FROM resources WHERE type='pdf'")?;
        let rows: Vec<String> = stmt.query_map([], |r| r.get::<_, String>(0))?
            .filter_map(|r| r.ok())
            .collect();
        rows.into_iter().collect()
    };

    // Phase 1: extract metadata + classify
    println!("Scanning {} PDFs...", pdf_pairs.len());

    enum Status { New, DuplicateTitle, AlreadyByPath }
    struct Candidate {
        wsl_pdf: std::path::PathBuf,
        win_pdf: std::path::PathBuf,
        title: String,
        author: Option<String>,
        pages: u32,
        status: Status,
    }

    let mut candidates: Vec<Candidate> = Vec::new();
    let mut seen_titles: std::collections::HashSet<String> = std::collections::HashSet::new();
    let n = pdf_pairs.len();

    for (i, (wsl_pdf, win_pdf)) in pdf_pairs.iter().enumerate() {
        eprint!("\r  [{}/{}] Extracting metadata...", i + 1, n);

        // Already tracked by path?
        let path_key = if !wsl_pdf.as_os_str().is_empty() {
            wsl_pdf.to_string_lossy().to_string()
        } else {
            win_pdf.to_string_lossy().to_string()
        };
        let by_path: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM resources WHERE type='pdf' AND path=?1",
                params![path_key],
                |r| r.get::<_, u32>(0),
            )
            .map(|c| c > 0)
            .unwrap_or(false);
        if by_path {
            candidates.push(Candidate {
                wsl_pdf: wsl_pdf.clone(), win_pdf: win_pdf.clone(),
                title: path_key, author: None, pages: 0,
                status: Status::AlreadyByPath,
            });
            continue;
        }

        let (title, author, pages) = extract_pdf_metadata(wsl_pdf, win_pdf);
        let title_lower = title.to_lowercase();

        let status = if existing_titles.contains(&title_lower) || seen_titles.contains(&title_lower) {
            Status::DuplicateTitle
        } else {
            seen_titles.insert(title_lower);
            Status::New
        };

        candidates.push(Candidate { wsl_pdf: wsl_pdf.clone(), win_pdf: win_pdf.clone(), title, author, pages, status });
    }
    eprintln!();

    let new_count  = candidates.iter().filter(|c| matches!(c.status, Status::New)).count();
    let dup_count  = candidates.iter().filter(|c| matches!(c.status, Status::DuplicateTitle)).count();
    let path_count = candidates.iter().filter(|c| matches!(c.status, Status::AlreadyByPath)).count();

    println!("  Found {} PDFs:", candidates.len());
    println!("    → {} new", new_count);
    if dup_count  > 0 { println!("    → {} duplicate title (skipping)", dup_count); }
    if path_count > 0 { println!("    → {} already tracked by path (skipping)", path_count); }

    if new_count == 0 {
        println!("Nothing to add.");
        return Ok(());
    }

    // Phase 2: copy + insert New candidates
    let mut inserted_ids: Vec<i64> = Vec::new();
    let mut sample_titles: Vec<String> = Vec::new();
    let new_candidates: Vec<&Candidate> = candidates.iter()
        .filter(|c| matches!(c.status, Status::New))
        .collect();

    println!("Copying {} PDFs to local store...", new_count);
    for (i, c) in new_candidates.iter().enumerate() {
        let display: String = c.title.chars().take(50).collect();
        eprint!("\r  [{}/{}] {}...", i + 1, new_count, display);

        let local_path = match copy_pdf_to_store(&c.win_pdf, &c.wsl_pdf, &c.title, c.author.as_deref()) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("\n  Failed to copy '{}': {}", c.title, e);
                continue;
            }
        };

        let local_str = local_path.to_string_lossy().to_string();
        conn.execute(
            "INSERT INTO resources (type, name, path, pages) VALUES ('pdf', ?1, ?2, ?3)",
            params![c.title, local_str, c.pages],
        )?;
        let id = conn.last_insert_rowid();
        apply_type_tags(conn, TYPE_PDF, id)?;
        inserted_ids.push(id);
        sample_titles.push(c.title.clone());
    }
    eprintln!();

    // One LLM tag call for the whole batch
    if !inserted_ids.is_empty() {
        let metadata = build_pdf_folder_metadata(&expanded, &sample_titles);
        let confirmed_tags = suggest_topic_tags(conn, &metadata)?;
        for id in &inserted_ids {
            apply_named_tags(conn, *id, &confirmed_tags)?;
        }
    }

    println!("Added {} PDFs from '{}'.", inserted_ids.len(), expanded);
    Ok(())
}

fn add_physical_book(conn: &Connection, name: Option<String>, pages: u32) -> Result<()> {
    let title = name.context("Provide -n <title> for physical books.")?;
    let exists: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM resources WHERE type='book' AND name = ?1",
            params![title],
            |r| r.get::<_, u32>(0),
        )
        .map(|c| c > 0)?;
    if exists {
        anyhow::bail!("Already in Physical Books: {}", title);
    }
    conn.execute(
        "INSERT INTO resources (type, name, pages) VALUES ('book', ?1, ?2)",
        params![title, pages],
    )?;
    let id = conn.last_insert_rowid();
    println!("Added to Physical Books: {} ({} pages)", title, pages);
    apply_type_tags(conn, TYPE_BOOK, id)?;
    let metadata = format!("Title: {}\nPages: {}", title, pages);
    prompt_and_apply_topic_tags(conn, id, &metadata)?;
    Ok(())
}
