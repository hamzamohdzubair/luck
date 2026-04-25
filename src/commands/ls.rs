use anyhow::Result;
use rusqlite::{Connection, params};
use std::io::Write;

use crate::resources::{scan_pdfs, get_pdf_pages};
use crate::utils::wsl_path;

// ── ANSI helpers ──────────────────────────────────────────────────────────────

const RESET: &str      = "\x1b[0m";
const BOLD: &str       = "\x1b[1m";
const DIM: &str        = "\x1b[2m";
const BG_HDR: &str     = "\x1b[48;5;24m\x1b[38;5;255m";
const BG_EVEN: &str    = "\x1b[48;5;235m";
const BG_ODD: &str     = "\x1b[48;5;237m";
const FG_URL: &str     = "\x1b[38;5;244m";
const FG_PICKS: &str   = "\x1b[38;5;178m";
const FG_TITLE: &str   = "\x1b[38;5;75m";
const FG_SEP: &str     = "\x1b[38;5;240m";
const FG_TAGS: &str    = "\x1b[38;5;73m";

fn type_color(t: &str) -> &'static str {
    match t {
        "video"    => "\x1b[38;5;39m",
        "playlist" => "\x1b[38;5;141m",
        "pdf"      => "\x1b[38;5;214m",
        "book"     => "\x1b[38;5;82m",
        "link"     => "\x1b[38;5;44m",
        "dir"      => "\x1b[38;5;208m",
        _          => "\x1b[38;5;252m",
    }
}

fn term_width() -> usize {
    crossterm::terminal::size().map(|(w, _)| w as usize).unwrap_or(100)
}

fn trunc(s: &str, max: usize) -> String {
    let clean: String = s.chars().map(|c| if c == '\n' || c == '\r' { ' ' } else { c }).collect();
    let chars: Vec<char> = clean.chars().collect();
    if chars.len() <= max {
        clean
    } else {
        chars[..max.saturating_sub(1)].iter().collect::<String>() + "…"
    }
}

fn pad(s: &str, width: usize) -> String {
    let len = s.chars().count();
    if len >= width { s.to_string() } else { format!("{}{}", s, " ".repeat(width - len)) }
}

// ── Row data ──────────────────────────────────────────────────────────────────

struct Row {
    id: i64,
    rtype: &'static str,
    name: String,
    tags: String,
    detail: String,
    picks: i64,
}

fn make_row(id: i64, rtype: &str, name: &str, tags: &str, url: Option<&str>, path: Option<&str>,
            pages: Option<u32>, video_count: Option<i64>, picks: i64) -> Row {
    let (rtype_s, detail) = match rtype {
        "video"    => ("video",    url.unwrap_or("").to_string()),
        "playlist" => ("playlist", {
            let vc = video_count.map_or(String::new(), |n| format!(" ({n} videos)"));
            format!("{}{}", url.unwrap_or(""), vc)
        }),
        "book"     => ("book",     pages.map_or(String::new(), |p| format!("{p} pages"))),
        "link"     => ("link",     url.unwrap_or("").to_string()),
        "dir"      => ("dir",      url.unwrap_or("").to_string()),
        _          => ("?",        String::new()),
    };
    let _ = path;
    Row { id, rtype: rtype_s, name: name.to_string(), tags: tags.to_string(), detail, picks }
}

// ── PDF folder expansion ──────────────────────────────────────────────────────

struct RawRow {
    id: i64,
    rtype: String,
    name: String,
    tags: Option<String>,
    url: Option<String>,
    path: Option<String>,
    pages: Option<u32>,
    video_count: Option<i64>,
    picks: i64,
}

fn expand_rows(conn: &Connection, raw: Vec<RawRow>) -> Vec<Row> {
    let mut out = Vec::new();
    for r in raw {
        let tags_str = r.tags.as_deref().unwrap_or("").to_string();
        if r.rtype == "pdf" {
            let path_str = r.path.as_deref().unwrap_or("");
            let resolved = wsl_path(std::path::Path::new(path_str));
            if !path_str.to_lowercase().ends_with(".pdf") {
                // Expand: one row per PDF in the folder
                let mut pdfs = scan_pdfs(&resolved);
                pdfs.sort();
                if pdfs.is_empty() {
                    out.push(Row {
                        id: r.id,
                        rtype: "pdf",
                        name: r.name,
                        tags: tags_str,
                        detail: "(empty folder)".to_string(),
                        picks: r.picks,
                    });
                } else {
                    for pdf in &pdfs {
                        let name = pdf
                            .file_stem()
                            .map(|s| s.to_string_lossy().to_string())
                            .unwrap_or_default();
                        let pages = get_pdf_pages(conn, pdf);
                        let detail = if pages > 0 {
                            format!("{pages} pages")
                        } else {
                            String::new()
                        };
                        out.push(Row { id: r.id, rtype: "pdf", name, tags: tags_str.clone(), detail, picks: r.picks });
                    }
                }
                continue;
            }
            // Individual PDF file
            let detail = r.pages.map_or(String::new(), |p| format!("{p} pages"));
            out.push(Row { id: r.id, rtype: "pdf", name: r.name, tags: tags_str, detail, picks: r.picks });
        } else {
            out.push(make_row(r.id, &r.rtype, &r.name, &tags_str, r.url.as_deref(), r.path.as_deref(),
                              r.pages, r.video_count, r.picks));
        }
    }
    out
}

// ── Render ────────────────────────────────────────────────────────────────────

fn render(rows: &[Row], label: &str) -> String {
    let tw = term_width();

    let id_w    = rows.iter().map(|r| format!("{}", r.id).len()).max().unwrap_or(2).max(2);
    let type_w  = 8usize;
    let picks_w = rows.iter().map(|r| format!("{}", r.picks).len()).max().unwrap_or(5).max(5);
    // fixed gutters: 2×7 = 14 spaces (before each of 6 columns + after last)
    let remaining = tw.saturating_sub(id_w + type_w + picks_w + 14);
    let name_w   = (remaining * 2 / 5).max(15);
    let tags_w   = (remaining / 5).max(12);
    let detail_w = remaining.saturating_sub(name_w + tags_w).max(15);

    let sep = format!("{FG_SEP}{}{RESET}\n", "─".repeat(tw));
    let mut buf = String::new();

    buf.push_str(&format!(
        "  {FG_TITLE}{BOLD}{label}{RESET}  {DIM}{} entries{RESET}\n",
        rows.len()
    ));

    buf.push_str(&format!(
        "{BG_HDR}{BOLD}  {:<id_w$}  {:<type_w$}  {:<name_w$}  {:<tags_w$}  {:<detail_w$}  {:>picks_w$}  {RESET}\n",
        "ID", "TYPE", "NAME", "TAGS", "DETAIL", "PICKS",
        id_w = id_w, type_w = type_w, name_w = name_w, tags_w = tags_w, detail_w = detail_w, picks_w = picks_w
    ));
    buf.push_str(&sep);

    for (i, row) in rows.iter().enumerate() {
        let bg = if i % 2 == 0 { BG_EVEN } else { BG_ODD };
        let tc = type_color(row.rtype);

        let name_cell   = pad(&trunc(&row.name,   name_w),   name_w);
        let tags_cell   = pad(&trunc(&row.tags,   tags_w),   tags_w);
        let detail_cell = pad(&trunc(&row.detail, detail_w), detail_w);

        buf.push_str(&format!(
            "{bg}  {DIM}{:>id_w$}{RESET}{bg}  {tc}{:<type_w$}{RESET}{bg}  {name_cell}{bg}  \
             {FG_TAGS}{tags_cell}{RESET}{bg}  {FG_URL}{detail_cell}{RESET}{bg}  \
             {FG_PICKS}{BOLD}{:>picks_w$}{RESET}{bg}  {RESET}\n",
            row.id, row.rtype, row.picks,
            id_w = id_w, type_w = type_w, picks_w = picks_w
        ));
    }

    buf.push_str(&sep);
    buf
}

// ── Command ───────────────────────────────────────────────────────────────────

fn map_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<RawRow> {
    Ok(RawRow {
        id:          r.get(0)?,
        rtype:       r.get(1)?,
        name:        r.get(2)?,
        url:         r.get(3)?,
        path:        r.get(4)?,
        pages:       r.get(5)?,
        video_count: r.get(6)?,
        picks:       r.get(7)?,
        tags:        r.get(8)?,
    })
}

const SELECT_COLS: &str =
    "SELECT r.id, r.type, r.name, r.url, r.path, r.pages, r.video_count, r.pick_count, \
     (SELECT GROUP_CONCAT(t2.name, ', ') \
      FROM resource_tags rt2 JOIN tags t2 ON t2.id = rt2.tag_id \
      WHERE rt2.resource_id = r.id \
      AND t2.name NOT IN ('video','playlist','youtube','pdf','physical','book','link') \
     ) AS topic_tags ";

pub fn cmd_ls(conn: &Connection, args: &[&str]) -> Result<()> {
    // Join all args, split on commas, trim — handles both "tag1, tag2" and "tag1,tag2"
    let joined = args.join(" ");
    let tags: Vec<&str> = joined.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();

    let (label, raw): (String, Vec<RawRow>) = if tags.is_empty() {
        let sql = format!("{SELECT_COLS}FROM resources r ORDER BY r.id");
        let mut stmt = conn.prepare(&sql)?;
        let raw = stmt.query_map([], map_row)?.filter_map(|r| r.ok()).collect();
        ("all".to_string(), raw)
    } else if tags.len() == 1 && tags[0].bytes().all(|b| b.is_ascii_digit()) {
        let id: i64 = tags[0].parse()?;
        let sql = format!("{SELECT_COLS}FROM resources r WHERE r.id = ?1");
        let mut stmt = conn.prepare(&sql)?;
        let raw: Vec<RawRow> = stmt.query_map(params![id], map_row)?.filter_map(|r| r.ok()).collect();
        if raw.is_empty() {
            anyhow::bail!("No resource with ID {}.", id);
        }
        (format!("#{}", id), raw)
    } else {
        let mut tag_ids: Vec<i64> = Vec::new();
        for tag in &tags {
            match conn.query_row("SELECT id FROM tags WHERE name = ?1", params![*tag], |r| r.get(0)) {
                Ok(id) => tag_ids.push(id),
                Err(_) => anyhow::bail!("Unknown tag '{}'. Use `luck topics add {}` to add it.", tag, tag),
            }
        }
        let placeholders: String = (1..=tag_ids.len()).map(|i| format!("?{i}")).collect::<Vec<_>>().join(", ");
        let count_idx = tag_ids.len() + 1;
        let sql = format!(
            "{SELECT_COLS}FROM resources r \
             WHERE r.id IN ( \
                 SELECT resource_id FROM resource_tags \
                 WHERE tag_id IN ({placeholders}) \
                 GROUP BY resource_id \
                 HAVING COUNT(DISTINCT tag_id) = ?{count_idx} \
             ) ORDER BY r.id"
        );
        let mut all_params: Vec<i64> = tag_ids.clone();
        all_params.push(tag_ids.len() as i64);
        let mut stmt = conn.prepare(&sql)?;
        let raw: Vec<RawRow> = stmt
            .query_map(rusqlite::params_from_iter(all_params.iter()), map_row)?
            .filter_map(|r| r.ok())
            .collect();
        let label = tags.iter().map(|t| format!("#{}", t)).collect::<Vec<_>>().join(", ");
        if raw.is_empty() {
            println!("No entries tagged {}.", label);
            return Ok(());
        }
        (label, raw)
    };

    if raw.is_empty() {
        println!("No resources found. Add some with:");
        println!("  luck add -l                     # YouTube video or playlist from clipboard");
        println!("  luck add -n \"Title\" -p 300      # physical book");
        println!("  luck add -d /path/to/folder     # PDF folder");
        return Ok(());
    }

    let rows = expand_rows(conn, raw);
    let output = render(&rows, &label);

    let pager = std::env::var("PAGER").unwrap_or_else(|_| "less".to_string());
    match std::process::Command::new(&pager)
        .arg("-R")
        .stdin(std::process::Stdio::piped())
        .spawn()
    {
        Ok(mut c) => {
            if let Some(stdin) = c.stdin.as_mut() {
                let _ = stdin.write_all(output.as_bytes());
            }
            let _ = c.wait();
        }
        Err(_) => print!("{}", output),
    }

    Ok(())
}
