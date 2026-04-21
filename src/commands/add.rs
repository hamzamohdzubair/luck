use anyhow::{Context, Result};
use rusqlite::{Connection, params};
use walkdir::WalkDir;

use crate::db::open_db;
use crate::llm::{build_pdf_metadata, get_folder_topic_tags, prompt_and_apply_topic_tags, prompt_link_type_tags};
use crate::resources::{TYPE_LINK, TYPE_PDF, TYPE_BOOK, TYPE_PLAYLIST, TYPE_VIDEO};
use crate::tags::{apply_named_tags, apply_type_tags};
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
    let folder = std::path::Path::new(&expanded);
    if !folder.is_dir() {
        anyhow::bail!("Not a directory: {}", expanded);
    }

    let pdfs: Vec<std::path::PathBuf> = WalkDir::new(folder)
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
        anyhow::bail!("No PDF files found in: {}", expanded);
    }

    let mut new_ids: Vec<i64> = Vec::new();
    let mut skipped = 0usize;

    for pdf in &pdfs {
        let pdf_path = pdf.to_string_lossy().to_string();
        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM resources WHERE type='pdf' AND path=?1",
                params![pdf_path],
                |r| r.get::<_, u32>(0),
            )
            .map(|c| c > 0)?;
        if exists {
            skipped += 1;
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
        let id = conn.last_insert_rowid();
        apply_type_tags(conn, TYPE_PDF, id)?;
        new_ids.push(id);
    }

    println!(
        "Added {} PDFs from {} ({} already existed)",
        new_ids.len(),
        expanded,
        skipped
    );

    if !new_ids.is_empty() {
        eprint!("Building metadata for tagging...");
        let metadata = build_pdf_metadata(&expanded);
        eprintln!(" done");
        let tags = get_folder_topic_tags(conn, &metadata)?;
        if !tags.is_empty() {
            for &id in &new_ids {
                apply_named_tags(conn, id, &tags)?;
            }
        }
    }

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
