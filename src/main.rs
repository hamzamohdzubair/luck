use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use rand::distributions::{Distribution, WeightedIndex};

mod commands;
mod db;
mod llm;
mod resources;
mod tags;
mod utils;
mod yt;

use commands::{
    add::add,
    ls::cmd_ls,
    retag::cmd_retag,
    rm::rm,
    stats::cmd_stats,
    sync::cmd_sync,
    topics::{cmd_tag, cmd_topics, TopicsSubcommand},
    weights::cmd_weights,
};
use db::{increment_pick_count, open_db};
use resources::{dispatch_resource, load_all_resources};
use tags::all_resource_eff_weights;

// ── CLI structs ───────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "luck")]
#[command(about = "Random learning resource picker", long_about = None)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum TopicsCommands {
    /// Add a new topic tag to the vocabulary
    Add { name: String },
    /// Remove a topic tag and detag all items
    Remove { name: String },
}

#[derive(Subcommand)]
enum Commands {
    /// Pick a random resource. Optionally filter by tags: "math", "video,math"
    Pick { filter: Option<String> },
    /// Remove an entry by resource ID (e.g. `luck rm 42`)
    Rm { id: i64 },
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
        /// Name/title (auto-detected from URL for -l if omitted)
        #[arg(short = 'n', long = "name")]
        name: Option<String>,
    },
    /// Show tag statistics
    Stats {
        /// Sort each section by descending current %
        #[arg(short = 's', long = "sort")]
        sort: bool,
    },
    /// Manage topic tag vocabulary
    Topics {
        #[command(subcommand)]
        subcommand: Option<TopicsCommands>,
    },
    /// Apply topic tags to a resource: `luck tag <id> math ml`
    Tag {
        id: i64,
        #[arg(required = true, num_args = 1..)]
        tags: Vec<String>,
    },
    /// List all entries for a tag with a pager
    Ls { tag: String },
    /// Interactive tag weight editor
    Weights,
    /// Sync tracked PDF folders for new additions
    Sync,
    /// Apply topic tags to resources. No args = untagged only; --all = every resource; <id> or <tag> = targeted
    Retag {
        /// Resource ID or tag name to retag
        target: Option<String>,
        /// Retag all resources, even those already tagged
        #[arg(short = 'a', long = "all")]
        all: bool,
        /// Auto-accept LLM suggestions without confirmation prompts
        #[arg(short = 'f', long = "force")]
        force: bool,
    },
}

// ── Pick ──────────────────────────────────────────────────────────────────────

fn resource_ids_for_tags(conn: &rusqlite::Connection, tag_names: &[&str]) -> Result<std::collections::HashSet<i64>> {
    use std::collections::HashSet;
    use rusqlite::params;

    let mut result: Option<HashSet<i64>> = None;
    for tag_name in tag_names {
        let tag_id: Option<i64> = conn.query_row(
            "SELECT id FROM tags WHERE name = ?1", params![tag_name], |r| r.get(0),
        ).ok();
        let Some(tag_id) = tag_id else {
            anyhow::bail!("Unknown tag '{}'. Use `luck topics add {}` to add it.", tag_name, tag_name);
        };
        let mut stmt = conn.prepare(
            "SELECT resource_id FROM resource_tags WHERE tag_id = ?1",
        )?;
        let ids: HashSet<i64> = stmt.query_map(params![tag_id], |r| r.get(0))?
            .filter_map(|r| r.ok()).collect();
        result = Some(match result {
            None    => ids,
            Some(r) => r.intersection(&ids).copied().collect(),
        });
    }
    Ok(result.unwrap_or_else(HashSet::new))
}

fn pick(filter: Option<String>) -> Result<()> {
    let conn = open_db()?;

    let all = load_all_resources(&conn)?;

    let candidates: Vec<&resources::Resource> = if let Some(ref f) = filter {
        let tag_names: Vec<&str> = f.split(',').map(|t| t.trim()).collect();
        let ids = resource_ids_for_tags(&conn, &tag_names)?;
        all.iter().filter(|r| ids.contains(&r.id())).collect()
    } else {
        all.iter().collect()
    };

    if candidates.is_empty() {
        if filter.is_some() {
            eprintln!("No resources found matching the given tags.");
        } else {
            eprintln!("No entries found. Add some with:");
            eprintln!("  luck add -l                     # YouTube video or playlist from clipboard");
            eprintln!("  luck add -n \"Title\" -p 300      # physical book");
            eprintln!("  luck add -d /path/to/folder     # PDF folder");
        }
        std::process::exit(0);
    }

    let eff_weights = all_resource_eff_weights(&conn)?;
    let weights: Vec<f64> = candidates
        .iter()
        .map(|r| eff_weights.get(&r.id()).copied().unwrap_or(1.0))
        .collect();

    let mut rng = rand::thread_rng();
    let dist = WeightedIndex::new(&weights).context("Could not build weight distribution")?;
    let idx = dist.sample(&mut rng);
    let resource = candidates[idx];

    dispatch_resource(&conn, resource, &mut rng)?;
    increment_pick_count(&conn, resource.id())?;
    Ok(())
}

// ── Main ──────────────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Pick { filter } => pick(filter)?,
        Commands::Rm { id } => {
            let conn = open_db()?;
            rm(&conn, id)?;
        }
        Commands::Add { from_clipboard, dir, pages, name } =>
            add(from_clipboard, dir, pages, name)?,
        Commands::Stats { sort } => {
            let conn = open_db()?;
            cmd_stats(&conn, sort)?;
        }
        Commands::Topics { subcommand } => {
            let conn = open_db()?;
            let sub = match subcommand {
                None => TopicsSubcommand::List,
                Some(TopicsCommands::Add { name }) => TopicsSubcommand::Add { name },
                Some(TopicsCommands::Remove { name }) => TopicsSubcommand::Remove { name },
            };
            cmd_topics(&conn, sub)?;
        }
        Commands::Tag { id, tags } => {
            let conn = open_db()?;
            cmd_tag(&conn, id, &tags)?;
        }
        Commands::Ls { tag } => {
            let conn = open_db()?;
            cmd_ls(&conn, &tag)?;
        }
        Commands::Weights => {
            let conn = open_db()?;
            cmd_weights(&conn)?;
        }
        Commands::Sync => {
            let conn = open_db()?;
            cmd_sync(&conn)?;
        }
        Commands::Retag { target, all, force } => cmd_retag(target, all, force)?,
    }
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::utils::{format_duration, format_hm, first_line, wsl_path, to_windows_path};
    use std::path::PathBuf;

    #[test]
    fn format_hm_zero() { assert_eq!(format_hm(0), "0h 00m"); }
    #[test]
    fn format_hm_one_hour() { assert_eq!(format_hm(3600), "1h 00m"); }
    #[test]
    fn format_hm_mixed() { assert_eq!(format_hm(7384), "2h 03m"); }
    #[test]
    fn format_hm_seconds_truncated() { assert_eq!(format_hm(3661), "1h 01m"); }

    #[test]
    fn format_duration_under_minute() { assert_eq!(format_duration(59), "0:59"); }
    #[test]
    fn format_duration_exact_minute() { assert_eq!(format_duration(60), "1:00"); }
    #[test]
    fn format_duration_with_hours() { assert_eq!(format_duration(3661), "1:01:01"); }

    #[test]
    fn tag_filter_splits_on_comma() {
        let tags: Vec<&str> = "math,video".split(',').map(|t| t.trim()).collect();
        assert_eq!(tags, vec!["math", "video"]);
    }
    #[test]
    fn tag_filter_trims_spaces() {
        let tags: Vec<&str> = "math, video".split(',').map(|t| t.trim()).collect();
        assert_eq!(tags, vec!["math", "video"]);
    }
    #[test]
    fn tag_filter_single() {
        let tags: Vec<&str> = "physics".split(',').map(|t| t.trim()).collect();
        assert_eq!(tags, vec!["physics"]);
    }

    #[test]
    fn yt_playlist_url() {
        assert!(crate::yt::yt_is_playlist("https://www.youtube.com/playlist?list=PLxxx"));
    }
    #[test]
    fn yt_watch_url_is_not_playlist() {
        assert!(!crate::yt::yt_is_playlist("https://www.youtube.com/watch?v=abc123"));
    }
    #[test]
    fn yt_youtu_be_is_not_playlist() {
        assert!(!crate::yt::yt_is_playlist("https://youtu.be/abc123"));
    }
    #[test]
    fn yt_watch_with_list_param_is_not_playlist() {
        assert!(!crate::yt::yt_is_playlist("https://www.youtube.com/watch?v=abc&list=PLxxx"));
    }

    #[test]
    fn first_line_single() { assert_eq!(first_line("hello"), "hello"); }
    #[test]
    fn first_line_multiline() { assert_eq!(first_line("hello\nworld"), "hello"); }
    #[test]
    fn first_line_skips_blank_leading() { assert_eq!(first_line("\nhello"), "hello"); }
    #[test]
    fn first_line_trims_whitespace() { assert_eq!(first_line("  hello  "), "hello"); }

    #[test]
    fn wsl_path_converts_windows_drive() {
        let p = std::path::Path::new(r"C:\Users\foo");
        assert_eq!(wsl_path(p), PathBuf::from("/mnt/c/Users/foo"));
    }
    #[test]
    fn wsl_path_lowercase_drive() {
        let p = std::path::Path::new(r"g:\books");
        assert_eq!(wsl_path(p), PathBuf::from("/mnt/g/books"));
    }
    #[test]
    fn wsl_path_unix_path_unchanged() {
        let p = std::path::Path::new("/home/user/docs");
        assert_eq!(wsl_path(p), PathBuf::from("/home/user/docs"));
    }
    #[test]
    fn to_windows_path_converts_mnt() {
        let p = PathBuf::from("/mnt/c/Users/foo");
        assert_eq!(to_windows_path(&p), PathBuf::from(r"C:\Users\foo"));
    }
    #[test]
    fn to_windows_path_non_mnt_unchanged() {
        let p = PathBuf::from("/home/user/docs");
        assert_eq!(to_windows_path(&p), PathBuf::from("/home/user/docs"));
    }
}
