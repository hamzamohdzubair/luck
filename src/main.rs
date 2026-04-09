use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use rand::seq::SliceRandom;
use rand::Rng;
use rusqlite::{Connection, params};
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use walkdir::WalkDir;

#[derive(Parser)]
#[command(name = "luck")]
#[command(about = "Random learning resource picker", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum SourcesCommands {
    /// List all sources with their IDs and entry counts
    Ls,
    /// Summarise each source (PDF counts, video hours, book pages)
    #[command(alias = "sum")]
    Summary,
}

#[derive(Subcommand)]
enum Commands {
    /// Pick a random resource. Optionally filter by table IDs: "3", "1-4", "1,3,5"
    Pick {
        filter: Option<String>,
    },
    /// Manage and inspect learning sources
    Sources {
        #[command(subcommand)]
        subcommand: SourcesCommands,
    },
    /// Add an entry (type is inferred from flags)
    Add {
        /// Pick URL from clipboard (auto-detects playlist vs video)
        #[arg(short = 'l', long = "link")]
        from_clipboard: bool,
        /// PDF folder path
        #[arg(short = 'd', long = "dir")]
        dir: Option<String>,
        /// Page count (physical book)
        #[arg(short = 'p', long = "pages")]
        pages: Option<u32>,
        /// Section counts per chapter, e.g. 3,5,6,7 (structured book)
        #[arg(short = 's', long = "sections")]
        sections: Option<String>,
        /// Name/title (auto-detected from URL for -l if omitted)
        #[arg(short = 'n', long = "name")]
        name: Option<String>,
    },
}

// Fixed table IDs
const ID_YT_PLAYLISTS:    u32 = 1;
const ID_YT_VIDEOS:       u32 = 2;
const ID_PDF_FOLDERS:     u32 = 3;
const ID_PHYSICAL_BOOKS:  u32 = 4;
const ID_STRUCTURED_BOOKS:u32 = 5;

struct TableMeta {
    id:    u32,
    name:  &'static str,
    count: usize,
}

fn get_db_path() -> Result<PathBuf> {
    let dir = dirs::data_dir()
        .context("Could not find data directory")?
        .join("luck");
    fs::create_dir_all(&dir)?;
    Ok(dir.join("luck.db"))
}

fn open_db() -> Result<Connection> {
    let path = get_db_path()?;
    let conn = Connection::open(&path)?;
    conn.execute_batch("
        CREATE TABLE IF NOT EXISTS yt_playlists (
            id    INTEGER PRIMARY KEY,
            name  TEXT NOT NULL,
            url   TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS yt_videos (
            id    INTEGER PRIMARY KEY,
            name  TEXT NOT NULL,
            url   TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS pdf_folders (
            id    INTEGER PRIMARY KEY,
            path  TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS physical_books (
            id    INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            pages INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS structured_books (
            id       INTEGER PRIMARY KEY,
            title    TEXT NOT NULL,
            sections TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pdf_scan_cache (
            path        TEXT PRIMARY KEY,
            pdf_count   INTEGER NOT NULL,
            total_pages INTEGER NOT NULL
        );
    ")?;
    Ok(conn)
}

fn table_counts(conn: &Connection) -> Result<[usize; 5]> {
    let counts = [
        conn.query_row("SELECT COUNT(*) FROM yt_playlists",    [], |r| r.get(0))?,
        conn.query_row("SELECT COUNT(*) FROM yt_videos",       [], |r| r.get(0))?,
        conn.query_row("SELECT COUNT(*) FROM pdf_folders",     [], |r| r.get(0))?,
        conn.query_row("SELECT COUNT(*) FROM physical_books",  [], |r| r.get(0))?,
        conn.query_row("SELECT COUNT(*) FROM structured_books",[], |r| r.get(0))?,
    ];
    Ok(counts)
}

fn all_table_meta(conn: &Connection) -> Result<Vec<TableMeta>> {
    let counts = table_counts(conn)?;
    Ok(vec![
        TableMeta { id: ID_YT_PLAYLISTS,     name: "YouTube Playlists", count: counts[0] },
        TableMeta { id: ID_YT_VIDEOS,        name: "YouTube Videos",    count: counts[1] },
        TableMeta { id: ID_PDF_FOLDERS,      name: "PDF Folders",       count: counts[2] },
        TableMeta { id: ID_PHYSICAL_BOOKS,   name: "Physical Books",    count: counts[3] },
        TableMeta { id: ID_STRUCTURED_BOOKS, name: "Structured Books",  count: counts[4] },
    ])
}

fn print_sources_ls(conn: &Connection) -> Result<()> {
    let tables = all_table_meta(conn)?;
    let name_w = tables.iter().map(|t| t.name.len()).max().unwrap_or(4);
    let header = format!(" {:<3}  {:<name_w$}  {}", "ID", "Name", "Entries");
    let sep = "─".repeat(header.len());
    println!("{}", sep);
    println!("{}", header);
    println!("{}", sep);
    for t in &tables {
        println!(" {:<3}  {:<name_w$}  {}", t.id, t.name, t.count);
    }
    println!("{}", sep);
    Ok(())
}

/// Convert a `/mnt/X/...` WSL path back to `X:\...` for Windows to open.
/// Paths that don't match the pattern are returned unchanged.
fn to_windows_path(path: &PathBuf) -> PathBuf {
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

/// Convert a Windows-style path (e.g. `G:\My Drive\books`) to its WSL mount
/// equivalent (`/mnt/g/My Drive/books`). Paths that are already Unix-style are
/// returned unchanged.
fn wsl_path(path: &std::path::Path) -> PathBuf {
    let s = path.to_string_lossy();
    // Match "X:\" or "X:/" at the start (case-insensitive drive letter)
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

/// Returns `(pdf_count, total_pages)` for a folder.
fn scan_pdf_folder(path: &std::path::Path) -> (usize, u64) {
    WalkDir::new(wsl_path(path))
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path().extension()
                .and_then(|x| x.to_str())
                .map(|x| x.eq_ignore_ascii_case("pdf"))
                .unwrap_or(false)
        })
        .fold((0, 0), |(count, pages), entry| {
            let p = lopdf::Document::load(entry.path())
                .ok()
                .map(|d| d.get_pages().len() as u64)
                .unwrap_or(0);
            (count + 1, pages + p)
        })
}

fn fetch_playlist_total_duration(url: &str) -> u64 {
    let output = Command::new("yt-dlp")
        .args(["--flat-playlist", "--print", "duration", url])
        .output();
    match output {
        Ok(out) if out.status.success() => {
            String::from_utf8_lossy(&out.stdout)
                .lines()
                .filter_map(|l| l.trim().parse::<f64>().ok())
                .map(|d| d as u64)
                .sum()
        }
        _ => 0,
    }
}

fn fmt_num(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 { out.push(','); }
        out.push(c);
    }
    out.chars().rev().collect()
}

fn format_hm(seconds: u64) -> String {
    let h = seconds / 3600;
    let m = (seconds % 3600) / 60;
    format!("{}h {:02}m", h, m)
}

fn sources_summary(conn: &Connection) -> Result<()> {
    // ── Video sources ──────────────────────────────────────────────────────────
    let playlist_count: usize = conn.query_row("SELECT COUNT(*) FROM yt_playlists", [], |r| r.get(0))?;
    let video_count: usize    = conn.query_row("SELECT COUNT(*) FROM yt_videos",    [], |r| r.get(0))?;

    let (playlist_secs, video_secs) = if playlist_count > 0 || video_count > 0 {
        eprintln!("Fetching video durations via yt-dlp (this may take a moment)...");
        let mut pl_secs: u64 = 0;
        if playlist_count > 0 {
            let mut stmt = conn.prepare("SELECT url FROM yt_playlists")?;
            let urls: Vec<String> = stmt.query_map([], |r| r.get(0))?
                .filter_map(|r| r.ok())
                .collect();
            for url in &urls {
                pl_secs += fetch_playlist_total_duration(url);
            }
        }
        let mut vid_secs: u64 = 0;
        if video_count > 0 {
            let mut stmt = conn.prepare("SELECT url FROM yt_videos")?;
            let urls: Vec<String> = stmt.query_map([], |r| r.get(0))?
                .filter_map(|r| r.ok())
                .collect();
            for url in &urls {
                vid_secs += get_video_duration(url).unwrap_or(0);
            }
        }
        (pl_secs, vid_secs)
    } else {
        (0, 0)
    };

    let total_video_secs = playlist_secs + video_secs;

    println!("Video Sources");
    {
        let col1_w = "YouTube Playlists".len();
        let col2_w = "Entries".len();
        let col3_w = "Duration".len().max(format_hm(total_video_secs).len());
        let header  = format!(" {:<col1_w$}  {:>col2_w$}  {:<col3_w$}", "Source", "Entries", "Duration");
        let sep = "─".repeat(header.len());
        println!("{}", sep);
        println!("{}", header);
        println!("{}", sep);
        println!(" {:<col1_w$}  {:>col2_w$}  {:<col3_w$}", "YouTube Playlists", playlist_count, format_hm(playlist_secs));
        println!(" {:<col1_w$}  {:>col2_w$}  {:<col3_w$}", "YouTube Videos",    video_count,    format_hm(video_secs));
        println!("{}", sep);
        println!(" {:<col1_w$}  {:>col2_w$}  {:<col3_w$}", "Total", playlist_count + video_count, format_hm(total_video_secs));
        println!("{}", sep);
    }
    println!();

    // ── Books & PDF Folders ────────────────────────────────────────────────────
    let total_pages: i64 = conn.query_row(
        "SELECT COALESCE(SUM(pages), 0) FROM physical_books", [], |r| r.get(0))?;

    // Collect PDF folder data, warning on inaccessible paths
    let mut stmt = conn.prepare("SELECT path FROM pdf_folders")?;
    let folders: Vec<String> = stmt.query_map([], |r| r.get(0))?
        .filter_map(|r| r.ok())
        .collect();

    struct FolderStat { stored: String, resolved: PathBuf, pdf_pages: u64, accessible: bool }
    let mut folder_stats: Vec<FolderStat> = Vec::new();
    let mut total_pdf_pages = 0u64;
    for stored in &folders {
        let raw = std::path::Path::new(stored);
        let resolved = wsl_path(raw);
        let accessible = resolved.exists();

        // Check cache first
        let cached: Option<(usize, u64)> = conn.query_row(
            "SELECT pdf_count, total_pages FROM pdf_scan_cache WHERE path = ?1",
            params![stored],
            |r| Ok((r.get::<_, usize>(0)?, r.get::<_, u64>(1)?)),
        ).ok();

        let (_pdf_count, pdf_pages) = if let Some(hit) = cached {
            hit
        } else if accessible {
            eprintln!("Scanning \"{}\" (first time — result will be cached)...", stored);
            let result = scan_pdf_folder(&resolved);
            conn.execute(
                "INSERT OR REPLACE INTO pdf_scan_cache (path, pdf_count, total_pages) VALUES (?1, ?2, ?3)",
                params![stored, result.0 as i64, result.1 as i64],
            )?;
            result
        } else {
            (0, 0)
        };

        total_pdf_pages += pdf_pages;
        folder_stats.push(FolderStat { stored: stored.clone(), resolved, pdf_pages, accessible });
    }

    println!("Book Sources");
    {
        let grand_total_pages = total_pages as u64 + total_pdf_pages;
        let phys_count: usize   = conn.query_row("SELECT COUNT(*) FROM physical_books",   [], |r| r.get(0))?;
        let struct_count: usize = conn.query_row("SELECT COUNT(*) FROM structured_books", [], |r| r.get(0))?;

        // (label, count, pages)
        let mut rows: Vec<(String, usize, Option<u64>)> = vec![
            ("Physical Books".to_string(),   phys_count,   Some(total_pages as u64)),
            ("Structured Books".to_string(), struct_count, None),
        ];
        for fs in &folder_stats {
            let (count, pages) = if fs.accessible {
                let c: usize = conn.query_row(
                    "SELECT pdf_count FROM pdf_scan_cache WHERE path = ?1",
                    params![fs.stored], |r| r.get(0)).unwrap_or(0);
                (c, Some(fs.pdf_pages))
            } else {
                (0, None)
            };
            rows.push((fs.stored.clone(), count, pages));
        }

        let col1_w = rows.iter().map(|(l, _, _)| l.len()).max().unwrap_or(5)
            .max("Source".len()).max("Total".len());
        let col2_w = rows.iter().map(|(_, c, _)| c.to_string().len()).max().unwrap_or(1)
            .max("Books".len());
        let col3_w = fmt_num(grand_total_pages).len().max("Pages".len());

        let sep = format!(" {}", "─".repeat(col1_w + 2 + col2_w + 2 + col3_w + 1));
        println!("{}", sep);
        println!(" {:<col1_w$}  {:>col2_w$}  {:>col3_w$}", "Source", "Books", "Pages");
        println!("{}", sep);
        for (label, count, pages) in &rows {
            let pages_str = match pages { Some(p) => fmt_num(*p), None => "—".to_string() };
            println!(" {:<col1_w$}  {:>col2_w$}  {:>col3_w$}", label, count, pages_str);
        }
        println!("{}", sep);
        let total_count: usize = rows.iter().map(|(_, c, _)| c).sum();
        println!(" {:<col1_w$}  {:>col2_w$}  {:>col3_w$}", "Total", total_count, fmt_num(grand_total_pages));
        println!("{}", sep);
    }

    // Warn about inaccessible paths after the table
    for fs in &folder_stats {
        if !fs.accessible {
            eprintln!();
            eprintln!("Warning: \"{}\" is not accessible from WSL.", fs.stored);
            eprintln!("  Resolved to: {}", fs.resolved.display());
            // Suggest mounting if it looks like a Windows drive letter
            let s = fs.stored.as_str();
            if s.len() >= 2 && s.chars().nth(1) == Some(':') {
                let drive = s.chars().next().unwrap().to_uppercase().to_string();
                eprintln!("  To mount the {}: drive in WSL, run:", drive);
                eprintln!("    sudo mkdir -p /mnt/{} && sudo mount -t drvfs {}:\\ /mnt/{}",
                    drive.to_lowercase(), drive, drive.to_lowercase());
            }
        }
    }

    Ok(())
}

fn parse_table_ids(filter: &str) -> Result<Vec<u32>> {
    let mut ids = Vec::new();
    for part in filter.split(',') {
        let part = part.trim();
        if let Some((a, b)) = part.split_once('-') {
            let start: u32 = a.trim().parse().context("Invalid table ID range")?;
            let end: u32   = b.trim().parse().context("Invalid table ID range")?;
            for id in start..=end { ids.push(id); }
        } else {
            ids.push(part.parse().context("Invalid table ID")?);
        }
    }
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}

fn validate_ids(ids: &[u32]) -> Result<()> {
    for &id in ids {
        if id < 1 || id > 5 {
            anyhow::bail!("Table ID {} does not exist. Valid IDs are 1-5.", id);
        }
    }
    Ok(())
}

// ── Resource types ────────────────────────────────────────────────────────────

#[derive(Debug)]
enum Resource {
    PdfFolder(PathBuf),
    PhysicalBook { title: String, pages: u32 },
    StructuredBook { title: String, sections: Vec<u32> },
    YouTubePlaylist { name: String, url: String },
    YouTubeVideo { name: String, url: String },
}

fn load_resources(conn: &Connection, ids: &[u32]) -> Result<Vec<Resource>> {
    let mut out = Vec::new();

    if ids.contains(&ID_YT_PLAYLISTS) {
        let mut stmt = conn.prepare("SELECT name, url FROM yt_playlists")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_,String>(0)?, r.get::<_,String>(1)?)))?;
        for row in rows { let (name, url) = row?; out.push(Resource::YouTubePlaylist { name, url }); }
    }
    if ids.contains(&ID_YT_VIDEOS) {
        let mut stmt = conn.prepare("SELECT name, url FROM yt_videos")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_,String>(0)?, r.get::<_,String>(1)?)))?;
        for row in rows { let (name, url) = row?; out.push(Resource::YouTubeVideo { name, url }); }
    }
    if ids.contains(&ID_PDF_FOLDERS) {
        let mut stmt = conn.prepare("SELECT path FROM pdf_folders")?;
        let rows = stmt.query_map([], |r| r.get::<_,String>(0))?;
        for row in rows { out.push(Resource::PdfFolder(PathBuf::from(row?))); }
    }
    if ids.contains(&ID_PHYSICAL_BOOKS) {
        let mut stmt = conn.prepare("SELECT title, pages FROM physical_books")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_,String>(0)?, r.get::<_,u32>(1)?)))?;
        for row in rows { let (title, pages) = row?; out.push(Resource::PhysicalBook { title, pages }); }
    }
    if ids.contains(&ID_STRUCTURED_BOOKS) {
        let mut stmt = conn.prepare("SELECT title, sections FROM structured_books")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_,String>(0)?, r.get::<_,String>(1)?)))?;
        for row in rows {
            let (title, sections_str) = row?;
            let sections = parse_sections(&sections_str)
                .with_context(|| format!("Corrupt sections data for '{}'", title))?;
            out.push(Resource::StructuredBook { title, sections });
        }
    }

    Ok(out)
}

fn parse_sections(s: &str) -> Option<Vec<u32>> {
    let s = s.trim().strip_prefix('[')?.strip_suffix(']')?;
    s.split(',').map(|n| n.trim().parse().ok()).collect()
}

// ── Pick ─────────────────────────────────────────────────────────────────────

fn pick(filter: Option<String>) -> Result<()> {
    let conn = open_db()?;
    let ids: Vec<u32> = match &filter {
        None => vec![1, 2, 3, 4, 5],
        Some(f) => {
            let ids = parse_table_ids(f)?;
            validate_ids(&ids)?;
            ids
        }
    };

    let resources = load_resources(&conn, &ids)?;

    if resources.is_empty() {
        eprintln!("No entries found. Add some with:");
        eprintln!("  luck add -l                     # YouTube video or playlist from clipboard");
        eprintln!("  luck add -n \"Title\" -p 300      # physical book");
        eprintln!("  luck add -n \"Title\" -s 3,5,6,7  # structured book");
        eprintln!("  luck add -d /path/to/folder     # PDF folder");
        std::process::exit(0);
    }

    let mut rng = rand::thread_rng();
    let resource = resources.choose(&mut rng).unwrap();

    dispatch_resource(resource, &mut rng)
}

/// Find SumatraPDF.exe, checking fixed locations then asking PowerShell.
fn find_sumatra() -> Option<PathBuf> {
    let known = [
        "/mnt/c/Program Files/SumatraPDF/SumatraPDF.exe",
        "/mnt/c/Program Files (x86)/SumatraPDF/SumatraPDF.exe",
    ];
    for loc in &known {
        let p = std::path::Path::new(loc);
        if p.exists() { return Some(p.to_path_buf()); }
    }

    // Per-user installs live under %LOCALAPPDATA%. Resolve that via PowerShell.
    let out = Command::new("powershell.exe")
        .args(["-NoProfile", "-command",
               "$p = \"$env:LOCALAPPDATA\\SumatraPDF\\SumatraPDF.exe\"; if (Test-Path $p) { $p }"])
        .output().ok()?;

    if out.status.success() {
        let win = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !win.is_empty() {
            let wsl = wsl_path(std::path::Path::new(&win));
            if wsl.exists() { return Some(wsl); }
        }
    }
    None
}

/// Open a PDF at a specific page. Tries SumatraPDF first (supports `-page N`),
/// falls back to the default handler (which won't jump to the page).
fn open_pdf_at_page(win_path: &PathBuf, page: u32) -> Result<()> {
    if let Some(sumatra) = find_sumatra() {
        Command::new(&sumatra)
            .args(["-page", &page.to_string(), &win_path.to_string_lossy().to_string()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .context("Failed to launch SumatraPDF")?;
        return Ok(());
    }

    // SumatraPDF not found — open without page targeting
    eprintln!("⚠ SumatraPDF not found; opening without page targeting");
    opener::open(win_path).context("Failed to open PDF")?;
    Ok(())
}

fn dispatch_resource(resource: &Resource, rng: &mut impl Rng) -> Result<()> {
    match resource {
        Resource::PdfFolder(folder) => {
            let folder = &wsl_path(folder);
            let pdfs: Vec<PathBuf> = WalkDir::new(folder)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.path().extension()
                        .and_then(|x| x.to_str())
                        .map(|x| x.eq_ignore_ascii_case("pdf"))
                        .unwrap_or(false)
                })
                .map(|e| e.path().to_path_buf())
                .collect();

            if pdfs.is_empty() {
                anyhow::bail!("No PDF files found in {}", folder.display());
            }

            let pdf = pdfs.choose(rng).unwrap();
            let pages = lopdf::Document::load(pdf).ok()
                .map(|d| d.get_pages().len() as u32)
                .unwrap_or(0);

            let win_path = to_windows_path(pdf);
            if pages > 0 {
                let page = rng.gen_range(1..=pages);
                println!("📄 {} — page {}/{}", pdf.display(), page, pages);
                open_pdf_at_page(&win_path, page)?;
            } else {
                println!("📄 {}", pdf.display());
                opener::open(&win_path).context("Failed to open PDF")?;
            }
        }

        Resource::PhysicalBook { title, pages } => {
            let page = rng.gen_range(1..=*pages);
            println!("📖 Open \"{}\" to page {}/{}", title, page, pages);
        }

        Resource::StructuredBook { title, sections } => {
            let chapter = rng.gen_range(0..sections.len());
            let sec = rng.gen_range(1..=sections[chapter]);
            println!("📖 Open \"{}\" — Chapter {}, Section {}", title, chapter + 1, sec);
        }

        Resource::YouTubePlaylist { name, url } => {
            println!("🎬 {}", name);
            let (video_url, n, total) = pick_random_playlist_video(url)?;
            println!("📺 Lecture {}/{}", n, total);
            let final_url = with_random_timestamp(&video_url, rng);
            println!("🔗 {}", final_url);
            opener::open_browser(&final_url).context("Failed to open browser")?;
        }

        Resource::YouTubeVideo { name, url } => {
            println!("🎥 {}", name);
            let final_url = with_random_timestamp(url, rng);
            println!("🔗 {}", final_url);
            opener::open_browser(&final_url).context("Failed to open browser")?;
        }
    }
    Ok(())
}

// ── yt-dlp helpers ───────────────────────────────────────────────────────────

fn yt_is_playlist(url: &str) -> bool {
    // A URL with /playlist? is unambiguously a playlist.
    // A watch URL with list= could be either; treat as video unless no v= param.
    if url.contains("/playlist?") {
        return true;
    }
    if url.contains("watch?") || url.contains("youtu.be/") {
        return false;
    }
    url.contains("list=")
}

fn fetch_yt_title(url: &str, is_playlist: bool) -> Option<String> {
    let field = if is_playlist { "playlist_title" } else { "title" };
    let output = Command::new("yt-dlp")
        .args(["--print", field, "--no-playlist", url])
        .output().ok()?;
    if output.status.success() {
        let t = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !t.is_empty() && t != "NA" { return Some(t); }
    }
    None
}

fn get_video_duration(url: &str) -> Option<u64> {
    let output = Command::new("yt-dlp")
        .args(["--print", "duration", "--no-playlist", url])
        .output().ok()?;
    String::from_utf8_lossy(&output.stdout).trim()
        .parse::<f64>().ok().map(|d| d as u64)
}

fn format_duration(s: u64) -> String {
    let h = s / 3600; let m = (s % 3600) / 60; let s = s % 60;
    if h > 0 { format!("{}:{:02}:{:02}", h, m, s) } else { format!("{}:{:02}", m, s) }
}

fn with_random_timestamp(url: &str, rng: &mut impl Rng) -> String {
    match get_video_duration(url) {
        Some(d) if d > 0 => {
            let t = rng.gen_range(0..d);
            println!("⏱ {} / {}", format_duration(t), format_duration(d));
            if url.contains('?') { format!("{}&t={}", url, t) } else { format!("{}?t={}", url, t) }
        }
        _ => {
            eprintln!("⚠ Could not get video duration (is yt-dlp installed?), opening without timestamp");
            url.to_string()
        }
    }
}

fn pick_random_playlist_video(url: &str) -> Result<(String, usize, usize)> {
    let output = Command::new("yt-dlp")
        .args(["--flat-playlist", "--print", "url", url])
        .output()
        .context("Failed to run yt-dlp (is it installed?)")?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let urls: Vec<&str> = stdout.lines().filter(|l| !l.is_empty()).collect();

    if urls.is_empty() { anyhow::bail!("No videos found in playlist"); }

    let total = urls.len();
    let idx = rand::thread_rng().gen_range(0..total);
    Ok((urls[idx].to_string(), idx + 1, total))
}

// ── Clipboard ────────────────────────────────────────────────────────────────

fn get_clipboard() -> Result<String> {
    let attempts = [
        ("wl-paste",     vec!["--no-newline"]),
        ("xclip",        vec!["-selection", "clipboard", "-o"]),
        ("xsel",         vec!["--clipboard", "--output"]),
        ("powershell.exe", vec!["-command", "Get-Clipboard"]),
    ];
    for (cmd, args) in &attempts {
        if let Ok(out) = Command::new(cmd).args(args).output() {
            if out.status.success() {
                let t = String::from_utf8_lossy(&out.stdout).trim().to_string();
                if !t.is_empty() { return Ok(t); }
            }
        }
    }
    anyhow::bail!("Could not read clipboard. Install xclip, xsel, wl-paste, or use WSL with powershell.")
}

// ── Add ──────────────────────────────────────────────────────────────────────

fn add(from_clipboard: bool, dir: Option<String>, pages: Option<u32>, section_counts: Option<String>, name: Option<String>) -> Result<()> {
    let conn = open_db()?;

    if from_clipboard {
        let url = get_clipboard()?;
        if !url.starts_with("http://") && !url.starts_with("https://") {
            anyhow::bail!("Clipboard content doesn't look like a URL: {}", url);
        }

        let is_playlist = yt_is_playlist(&url);

        // Duplicate check
        if is_playlist {
            let exists: bool = conn.query_row(
                "SELECT COUNT(*) FROM yt_playlists WHERE url = ?1", params![url], |r| r.get::<_,u32>(0)
            ).map(|c| c > 0)?;
            if exists { anyhow::bail!("Already in YouTube Playlists: {}", url); }
        } else {
            let exists: bool = conn.query_row(
                "SELECT COUNT(*) FROM yt_videos WHERE url = ?1", params![url], |r| r.get::<_,u32>(0)
            ).map(|c| c > 0)?;
            if exists { anyhow::bail!("Already in YouTube Videos: {}", url); }
        }

        let resolved_name = if let Some(n) = name {
            n
        } else {
            eprint!("Fetching title...");
            let t = fetch_yt_title(&url, is_playlist).unwrap_or_else(|| url.clone());
            eprintln!(" done");
            t
        };

        if is_playlist {
            conn.execute("INSERT INTO yt_playlists (name, url) VALUES (?1, ?2)", params![resolved_name, url])?;
            println!("Added to YouTube Playlists: {}", resolved_name);
        } else {
            conn.execute("INSERT INTO yt_videos (name, url) VALUES (?1, ?2)", params![resolved_name, url])?;
            println!("Added to YouTube Videos: {}", resolved_name);
        }

    } else if let Some(path) = dir {
        let expanded = shellexpand::tilde(&path).to_string();
        let exists: bool = conn.query_row(
            "SELECT COUNT(*) FROM pdf_folders WHERE path = ?1", params![expanded], |r| r.get::<_,u32>(0)
        ).map(|c| c > 0)?;
        if exists { anyhow::bail!("Already in PDF Folders: {}", expanded); }

        conn.execute("INSERT INTO pdf_folders (path) VALUES (?1)", params![expanded])?;
        println!("Added to PDF Folders: {}", expanded);

    } else if let Some(p) = pages {
        let title = name.context("Provide -n <title> for physical books.")?;
        let exists: bool = conn.query_row(
            "SELECT COUNT(*) FROM physical_books WHERE title = ?1", params![title], |r| r.get::<_,u32>(0)
        ).map(|c| c > 0)?;
        if exists { anyhow::bail!("Already in Physical Books: {}", title); }

        conn.execute("INSERT INTO physical_books (title, pages) VALUES (?1, ?2)", params![title, p])?;
        println!("Added to Physical Books: {} ({} pages)", title, p);

    } else if let Some(s) = section_counts {
        let title = name.context("Provide -n <title> for structured books.")?;
        let nums: Vec<u32> = s.split(',')
            .map(|n: &str| n.trim().parse::<u32>())
            .collect::<std::result::Result<_, _>>()
            .context("Sections must be comma-separated integers, e.g. 3,5,6,7")?;
        let sections_str = format!("[{}]", nums.iter().map(|n| n.to_string()).collect::<Vec<_>>().join(", "));

        let exists: bool = conn.query_row(
            "SELECT COUNT(*) FROM structured_books WHERE title = ?1", params![title], |r| r.get::<_,u32>(0)
        ).map(|c| c > 0)?;
        if exists { anyhow::bail!("Already in Structured Books: {}", title); }

        conn.execute("INSERT INTO structured_books (title, sections) VALUES (?1, ?2)", params![title, sections_str])?;
        println!("Added to Structured Books: {} ({} chapters)", title, nums.len());

    } else {
        anyhow::bail!("Specify one of: -l (link), -d <dir>, -p <pages>, -s <sections>");
    }

    Ok(())
}

// ── Main ─────────────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Pick { filter } => pick(filter)?,
        Commands::Sources { subcommand } => {
            let conn = open_db()?;
            match subcommand {
                SourcesCommands::Ls      => print_sources_ls(&conn)?,
                SourcesCommands::Summary => sources_summary(&conn)?,
            }
        }
        Commands::Add { from_clipboard, dir, pages, sections, name } =>
            add(from_clipboard, dir, pages, sections, name)?,
    }
    Ok(())
}
