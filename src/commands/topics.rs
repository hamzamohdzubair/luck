use anyhow::Result;
use rusqlite::{Connection, params};

use crate::tags::get_topic_vocab;

const RESET: &str    = "\x1b[0m";
const BOLD: &str     = "\x1b[1m";
const DIM: &str      = "\x1b[2m";
const FG_TAG: &str   = "\x1b[38;5;75m";
const FG_HDR: &str   = "\x1b[38;5;255m";
const FG_SEP: &str   = "\x1b[38;5;240m";

fn term_width() -> usize {
    crossterm::terminal::size().map(|(w, _)| w as usize).unwrap_or(100)
}

pub enum TopicsSubcommand {
    List,
    Add { name: String },
    Remove { name: String },
}

pub fn cmd_topics(conn: &Connection, sub: TopicsSubcommand) -> Result<()> {
    match sub {
        TopicsSubcommand::List => {
            let mut vocab = get_topic_vocab(conn)?;
            if vocab.is_empty() {
                println!("No topic tags defined.");
            } else {
                vocab.sort();
                let tw = term_width();
                let col_w = vocab.iter().map(|t| t.len() + 1).max().unwrap_or(10) + 2;
                let cols = ((tw.saturating_sub(2)) / col_w).max(1).min(4);

                println!();
                println!("  {BOLD}{FG_HDR}Topic Tags{RESET}  {FG_SEP}·{RESET}  {DIM}{} tags{RESET}", vocab.len());
                println!();

                let rows = (vocab.len() + cols - 1) / cols;
                for row in 0..rows {
                    print!("  ");
                    for col in 0..cols {
                        let idx = col * rows + row;
                        if idx < vocab.len() {
                            let tag = &vocab[idx];
                            let padding = col_w.saturating_sub(tag.len() + 1);
                            print!("{FG_TAG}#{tag}{RESET}{}", " ".repeat(padding));
                        }
                    }
                    println!();
                }
                println!();
            }
        }
        TopicsSubcommand::Add { name } => {
            let name = name.trim_start_matches('#').to_lowercase();
            conn.execute(
                "INSERT OR IGNORE INTO tags (name, weight) VALUES (?1, 1.0)",
                params![name],
            )?;
            println!("Added topic tag: #{}", name);
        }
        TopicsSubcommand::Remove { name } => {
            let name = name.trim_start_matches('#').to_lowercase();
            let tag_id: Option<i64> = conn
                .query_row("SELECT id FROM tags WHERE name = ?1", params![name], |r| r.get(0))
                .ok();
            if let Some(tid) = tag_id {
                conn.execute("DELETE FROM resource_tags WHERE tag_id = ?1", params![tid])?;
                conn.execute("DELETE FROM tags WHERE id = ?1", params![tid])?;
                println!("Removed topic tag: #{}", name);
            } else {
                eprintln!("Tag '{}' not found.", name);
            }
        }
    }
    Ok(())
}

pub fn cmd_tag(conn: &Connection, id: i64, tags: &[String]) -> Result<()> {
    use crate::tags::{TYPE_TAG_MAP, apply_named_tag};

    let vocab = get_topic_vocab(conn)?;
    let type_names: Vec<String> = TYPE_TAG_MAP.iter().map(|(n, _)| n.to_string()).collect();

    // Verify the resource exists
    let exists: bool = conn
        .query_row("SELECT COUNT(*) FROM resources WHERE id = ?1", params![id], |r| r.get::<_, i64>(0))
        .map(|c| c > 0)
        .unwrap_or(false);
    if !exists {
        anyhow::bail!("No resource with ID {} found.", id);
    }

    for tag_name in tags {
        let tag_name = tag_name.trim_start_matches('#');
        if !vocab.contains(&tag_name.to_string()) && !type_names.contains(&tag_name.to_string()) {
            eprintln!(
                "Unknown tag: '{}'. Use `luck topics add {}` to add it.",
                tag_name, tag_name
            );
            continue;
        }
        apply_named_tag(conn, id, tag_name)?;
        println!("Tagged {} with #{}", id, tag_name);
    }
    Ok(())
}
