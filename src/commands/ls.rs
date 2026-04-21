use anyhow::Result;
use rusqlite::{Connection, params};
use std::io::Write;

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
    detail: String,
    picks: i64,
}

fn make_row(id: i64, rtype: &str, name: &str, url: Option<&str>, path: Option<&str>,
            pages: Option<u32>, video_count: Option<i64>, picks: i64) -> Row {
    let (rtype_s, detail) = match rtype {
        "video"    => ("video",    url.unwrap_or("").to_string()),
        "playlist" => ("playlist", {
            let vc = video_count.map_or(String::new(), |n| format!(" ({n} videos)"));
            format!("{}{}", url.unwrap_or(""), vc)
        }),
        "pdf"      => ("pdf",      path.unwrap_or("").to_string()),
        "book"     => ("book",     pages.map_or(String::new(), |p| format!("{p} pages"))),
        "link"     => ("link",     url.unwrap_or("").to_string()),
        "dir"      => ("dir",      url.unwrap_or("").to_string()),
        _          => ("?",        String::new()),
    };
    Row { id, rtype: rtype_s, name: name.to_string(), detail, picks }
}

// ── Render ────────────────────────────────────────────────────────────────────

fn render(rows: &[Row], tag: &str) -> String {
    let tw = term_width();

    let id_w    = rows.iter().map(|r| format!("{}", r.id).len()).max().unwrap_or(2).max(2);
    let type_w  = 8usize;
    let picks_w = rows.iter().map(|r| format!("{}", r.picks).len()).max().unwrap_or(5).max(5);
    // fixed gutters: 2×6 = 12 spaces (before each of 5 columns + after last)
    let remaining = tw.saturating_sub(id_w + type_w + picks_w + 12);
    // name gets 40%, detail gets 60% of remaining
    let name_w   = (remaining * 2 / 5).max(15);
    let detail_w = remaining.saturating_sub(name_w).max(15);

    let sep = format!("{FG_SEP}{}{RESET}\n", "─".repeat(tw));
    let mut buf = String::new();

    buf.push_str(&format!(
        "  {FG_TITLE}{BOLD}#{tag}{RESET}  {DIM}{} entries{RESET}\n",
        rows.len()
    ));

    buf.push_str(&format!(
        "{BG_HDR}{BOLD}  {:<id_w$}  {:<type_w$}  {:<name_w$}  {:<detail_w$}  {:>picks_w$}  {RESET}\n",
        "ID", "TYPE", "NAME", "DETAIL", "PICKS",
        id_w = id_w, type_w = type_w, name_w = name_w, detail_w = detail_w, picks_w = picks_w
    ));
    buf.push_str(&sep);

    for (i, row) in rows.iter().enumerate() {
        let bg = if i % 2 == 0 { BG_EVEN } else { BG_ODD };
        let tc = type_color(row.rtype);

        let name_cell   = pad(&trunc(&row.name,   name_w),   name_w);
        let detail_cell = pad(&trunc(&row.detail, detail_w), detail_w);

        buf.push_str(&format!(
            "{bg}  {DIM}{:>id_w$}{RESET}{bg}  {tc}{:<type_w$}{RESET}{bg}  {name_cell}{bg}  \
             {FG_URL}{detail_cell}{RESET}{bg}  {FG_PICKS}{BOLD}{:>picks_w$}{RESET}{bg}  {RESET}\n",
            row.id, row.rtype, row.picks,
            id_w = id_w, type_w = type_w, picks_w = picks_w
        ));
    }

    buf.push_str(&sep);
    buf
}

// ── Command ───────────────────────────────────────────────────────────────────

pub fn cmd_ls(conn: &Connection, tag: &str) -> Result<()> {
    let tag_id: Option<i64> = conn.query_row(
        "SELECT id FROM tags WHERE name = ?1", params![tag], |r| r.get(0),
    ).ok();

    let Some(tag_id) = tag_id else {
        anyhow::bail!("Unknown tag '{}'. Use `luck topics add {}` to add it.", tag, tag);
    };

    let mut stmt = conn.prepare(
        "SELECT r.id, r.type, r.name, r.url, r.path, r.pages, r.video_count, r.pick_count \
         FROM resources r \
         JOIN resource_tags rt ON rt.resource_id = r.id \
         WHERE rt.tag_id = ?1 \
         ORDER BY r.id",
    )?;

    let rows: Vec<Row> = stmt
        .query_map(params![tag_id], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, Option<String>>(4)?,
                r.get::<_, Option<u32>>(5)?,
                r.get::<_, Option<i64>>(6)?,
                r.get::<_, i64>(7)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .map(|(id, rtype, name, url, path, pages, vc, picks)| {
            make_row(id, &rtype, &name, url.as_deref(), path.as_deref(), pages, vc, picks)
        })
        .collect();

    if rows.is_empty() {
        println!("No entries tagged #{}.", tag);
        return Ok(());
    }

    let output = render(&rows, tag);

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
