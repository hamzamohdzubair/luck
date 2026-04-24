use anyhow::Result;
use rusqlite::{Connection, params};
use std::collections::HashSet;
use std::path::PathBuf;

use crate::db::get_tracked_folders;
use crate::llm::auto_apply_topic_tags;
use crate::resources::{scan_pdfs, scan_pdfs_win, extract_pdf_metadata, copy_pdf_to_store, TYPE_PDF};
use crate::tags::apply_type_tags;
use crate::utils::{is_wsl, to_windows_path, wsl_path};

pub fn cmd_sync(conn: &Connection) -> Result<()> {
    let folders = get_tracked_folders(conn)?;

    if folders.is_empty() {
        println!("No tracked folders. Add one with: luck add -d <path>");
        return Ok(());
    }

    for (folder_id, folder_path) in &folders {
        println!("Syncing '{}'...", folder_path);

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
            println!("  No PDFs found (folder may be unmounted or empty).");
            continue;
        }

        let existing_titles: HashSet<String> = {
            let mut stmt = conn.prepare("SELECT LOWER(name) FROM resources WHERE type='pdf'")?;
            let rows: Vec<String> = stmt.query_map([], |r| r.get::<_, String>(0))?
                .filter_map(|r| r.ok())
                .collect();
            rows.into_iter().collect()
        };

        let mut added = 0usize;
        let mut skipped = 0usize;
        let mut seen_titles: HashSet<String> = HashSet::new();

        for (wsl_pdf, win_pdf) in &pdf_pairs {
            let (title, author, pages) = extract_pdf_metadata(wsl_pdf, win_pdf);
            let title_lower = title.to_lowercase();

            if existing_titles.contains(&title_lower) || seen_titles.contains(&title_lower) {
                skipped += 1;
                continue;
            }
            seen_titles.insert(title_lower);

            let local_path = match copy_pdf_to_store(win_pdf, wsl_pdf, &title, author.as_deref()) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("  Failed to copy '{}': {}", title, e);
                    continue;
                }
            };

            let local_str = local_path.to_string_lossy().to_string();
            conn.execute(
                "INSERT INTO resources (type, name, path, pages) VALUES ('pdf', ?1, ?2, ?3)",
                params![title, local_str, pages],
            )?;
            let id = conn.last_insert_rowid();
            apply_type_tags(conn, TYPE_PDF, id)?;

            println!("+ {}", title);
            let metadata = format!("Title: {}", title);
            auto_apply_topic_tags(conn, id, &metadata)?;
            added += 1;
        }

        // Warn about missing local copies
        let local_resources: Vec<(i64, String, String)> = {
            let mut stmt = conn.prepare(
                "SELECT id, name, path FROM resources WHERE type='pdf' AND path LIKE '%.pdf'",
            )?;
            let rows: Vec<(i64, String, String)> = stmt
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                .filter_map(|r| r.ok())
                .collect();
            rows
        };
        for (id, name, path) in &local_resources {
            if !path.is_empty() && !std::path::Path::new(path).exists() {
                eprintln!(
                    "  Warning: local copy missing for '{}' (id: {}). Use `luck rm {}` to clean up.",
                    name, id, id
                );
            }
        }

        conn.execute(
            "UPDATE tracked_folders SET last_synced_at = strftime('%s','now') WHERE id = ?1",
            params![folder_id],
        )?;

        println!(
            "  Done — {} new, {} duplicate (skipped).",
            added, skipped
        );
    }

    Ok(())
}
