use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use rand::seq::SliceRandom;
use rand::Rng;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Parser)]
#[command(name = "luck")]
#[command(about = "Random learning resource picker", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Pick a random learning resource (optionally filter by tags)
    Pick {
        /// Filter by tags (e.g., "you" matches "youtube, playlist")
        filter: Option<String>,
    },
    /// Open config file in $EDITOR
    Config,
}

const DEFAULT_CONFIG: &str = r#"# Luck - Random Learning Resource Picker

## book, pdf

| Path |
|------|
| ~/Documents/Books |
| ~/Google Drive/Learning/PDFs |

## book, physical

| Title | Pages |
|-------|-------|
| Deep Work | 296 |
| The Pragmatic Programmer | 352 |
| Designing Data-Intensive Applications | 616 |

## youtube, playlist

| Hint | URL |
|------|-----|
| MIT OpenCourseWare Algorithms | <https://www.youtube.com/playlist?list=PLxyz123> |
| Stanford CS229 ML Course | <https://www.youtube.com/playlist?list=PLabc456> |

## youtube, video

| Hint | URL |
|------|-----|
| Advanced Rust Patterns | <https://www.youtube.com/watch?v=dQw4w9WgXcQ> |
| Functional Programming | <https://www.youtube.com/watch?v=abc123def> |
"#;

#[derive(Debug, Clone)]
enum Resource {
    PdfFolder(PathBuf),
    PhysicalBook { title: String, pages: u32 },
    StructuredBook { title: String, structure: BookStructure },
    YouTubePlaylist { hint: Option<String>, url: String },
    YouTubeVideo { hint: Option<String>, url: String },
}

#[derive(Debug, Clone)]
enum BookStructure {
    // [5, 3, 6] - chapters with section counts
    Sections(Vec<u32>),
    // [[5, 3], [2, 4]] - chapters with sections, each with subsection counts
    Subsections(Vec<Vec<u32>>),
}

#[derive(Debug)]
struct Section {
    tags: Vec<String>,
    resources: Vec<Resource>,
}

fn get_config_path() -> Result<PathBuf> {
    let config_dir = dirs::config_dir()
        .context("Could not find config directory")?
        .join("luck");

    fs::create_dir_all(&config_dir)?;
    Ok(config_dir.join("luck.md"))
}

fn ensure_config_exists() -> Result<PathBuf> {
    let config_path = get_config_path()?;

    if !config_path.exists() {
        fs::write(&config_path, DEFAULT_CONFIG)?;
        println!("Created default config at: {}", config_path.display());
    }

    Ok(config_path)
}

fn open_config() -> Result<()> {
    let config_path = ensure_config_exists()?;

    let editor = std::env::var("EDITOR").unwrap_or_else(|_| "nano".to_string());

    Command::new(editor)
        .arg(&config_path)
        .status()
        .context("Failed to open editor")?;

    Ok(())
}

fn strip_angle_brackets(s: &str) -> String {
    s.trim_start_matches('<').trim_end_matches('>').to_string()
}

fn parse_book_structure(s: &str) -> Option<BookStructure> {
    let s = s.trim();

    // Check if it's a list of lists [[1,2],[3,4]]
    if s.starts_with("[[") && s.ends_with("]]") {
        let inner = &s[2..s.len()-2];
        let mut subsections = Vec::new();

        let mut depth = 0;
        let mut current_section = String::new();

        for ch in inner.chars() {
            match ch {
                '[' => depth += 1,
                ']' => {
                    depth -= 1;
                    if depth == 0 && !current_section.is_empty() {
                        // Parse this section's subsection counts
                        let nums: Vec<u32> = current_section
                            .split(',')
                            .filter_map(|n| n.trim().parse().ok())
                            .collect();
                        if !nums.is_empty() {
                            subsections.push(nums);
                        }
                        current_section.clear();
                    }
                }
                _ => {
                    if depth > 0 {
                        current_section.push(ch);
                    }
                }
            }
        }

        if !subsections.is_empty() {
            return Some(BookStructure::Subsections(subsections));
        }
    }
    // Check if it's a simple list [1,2,3]
    else if s.starts_with('[') && s.ends_with(']') {
        let inner = &s[1..s.len()-1];
        let sections: Vec<u32> = inner
            .split(',')
            .filter_map(|n| n.trim().parse().ok())
            .collect();

        if !sections.is_empty() {
            return Some(BookStructure::Sections(sections));
        }
    }

    None
}

fn parse_config(path: &Path) -> Result<Vec<Section>> {
    let content = fs::read_to_string(path)?;
    let mut sections = Vec::new();
    let mut current_tags: Option<Vec<String>> = None;
    let mut current_resources: Vec<Resource> = Vec::new();

    for line in content.lines() {
        let line = line.trim();

        // Skip empty lines
        if line.is_empty() || line.starts_with("---") || line.starts_with('*') {
            continue;
        }

        // Detect sections (## tag1, tag2, tag3)
        if line.starts_with("## ") {
            // Save previous section if exists
            if let Some(tags) = current_tags.take() {
                if !current_resources.is_empty() {
                    sections.push(Section {
                        tags,
                        resources: current_resources.clone(),
                    });
                    current_resources.clear();
                }
            }

            // Parse new section tags
            let tags_str = line[3..].trim();
            current_tags = Some(
                tags_str
                    .split(',')
                    .map(|s| s.trim().to_lowercase())
                    .collect(),
            );
            continue;
        }

        // Skip single # comments (but not ## headings)
        if line.starts_with('#') {
            continue;
        }

        // Parse table rows
        if line.starts_with('|') {
            // Skip separator rows (contain only dashes)
            if line.contains("---") {
                continue;
            }

            let parts: Vec<&str> = line.split('|')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();

            // Skip header rows
            if parts.is_empty() {
                continue;
            }
            let first_lower = parts[0].to_lowercase();
            if first_lower == "path" || first_lower == "title" || first_lower == "hint" || first_lower == "url" {
                continue;
            }

            // Infer resource type from table structure (independent of tags)
            if current_tags.is_some() {
                if parts.len() == 1 {
                    // Single column with path -> PDF folder
                    let path = shellexpand::tilde(parts[0]).to_string();
                    current_resources.push(Resource::PdfFolder(PathBuf::from(path)));
                } else if parts.len() >= 2 {
                    let second_col = parts[1];
                    let stripped_url = strip_angle_brackets(second_col);

                    // Check if second column is a book structure [1,2,3] or [[1,2],[3,4]]
                    if let Some(structure) = parse_book_structure(second_col) {
                        current_resources.push(Resource::StructuredBook {
                            title: parts[0].to_string(),
                            structure,
                        });
                    }
                    // Check if second column is a YouTube URL
                    else if stripped_url.contains("youtube.com") || stripped_url.contains("youtu.be") {
                        let hint = if parts[0].is_empty() || parts[0] == "-" {
                            None
                        } else {
                            Some(parts[0].to_string())
                        };

                        // Distinguish playlist vs video by URL pattern
                        if stripped_url.contains("/playlist?") || stripped_url.contains("&list=") {
                            current_resources.push(Resource::YouTubePlaylist {
                                hint,
                                url: stripped_url,
                            });
                        } else {
                            current_resources.push(Resource::YouTubeVideo {
                                hint,
                                url: stripped_url,
                            });
                        }
                    } else if let Ok(pages) = second_col.parse::<u32>() {
                        // Second column is a number -> Book with pages
                        current_resources.push(Resource::PhysicalBook {
                            title: parts[0].to_string(),
                            pages,
                        });
                    }
                }
            }
        }
    }

    // Save last section
    if let Some(tags) = current_tags {
        if !current_resources.is_empty() {
            sections.push(Section {
                tags,
                resources: current_resources,
            });
        }
    }

    Ok(sections)
}

fn pick_random_resource(filter: Option<String>) -> Result<()> {
    let config_path = ensure_config_exists()?;
    let sections = parse_config(&config_path)?;

    if sections.is_empty() {
        eprintln!("No sections found in config. Run 'luck config' to add some!");
        std::process::exit(1);
    }

    // Filter sections by tag if filter is provided
    let filtered_sections: Vec<&Section> = if let Some(ref f) = filter {
        let f_lower = f.to_lowercase();
        sections
            .iter()
            .filter(|s| s.tags.iter().any(|tag| tag.contains(&f_lower)))
            .collect()
    } else {
        sections.iter().collect()
    };

    if filtered_sections.is_empty() {
        eprintln!(
            "No sections found matching filter '{}'.\nAvailable tags: {}",
            filter.unwrap_or_default(),
            sections
                .iter()
                .flat_map(|s| s.tags.iter())
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        std::process::exit(1);
    }

    let mut rng = rand::thread_rng();

    // Pick random section
    let section = filtered_sections.choose(&mut rng).unwrap();

    // Pick random resource from that section
    let resource = section.resources.choose(&mut rng).unwrap();

    match resource {
        Resource::PdfFolder(path) => {
            println!("📁 Selected PDF folder: {}", path.display());
            // TODO: Scan for PDFs, pick random one, open at random page
            println!("(PDF scanning not yet implemented)");
        }
        Resource::PhysicalBook { title, pages } => {
            let page = rng.gen_range(1..=*pages);
            println!("📖 Open \"{}\" to page {}", title, page);
        }
        Resource::StructuredBook { title, structure } => {
            match structure {
                BookStructure::Sections(sections) => {
                    // Pick random chapter
                    let chapter_idx = rng.gen_range(0..sections.len());
                    let section_count = sections[chapter_idx];
                    // Pick random section
                    let section_num = rng.gen_range(1..=section_count);
                    println!("📖 Open \"{}\" Chapter {}, Section {}", title, chapter_idx + 1, section_num);
                }
                BookStructure::Subsections(subsections) => {
                    // Pick random chapter
                    let chapter_idx = rng.gen_range(0..subsections.len());
                    let sections = &subsections[chapter_idx];
                    // Pick random section
                    let section_idx = rng.gen_range(0..sections.len());
                    let subsection_count = sections[section_idx];
                    // Pick random subsection
                    let subsection_num = rng.gen_range(1..=subsection_count);
                    println!(
                        "📖 Open \"{}\" Chapter {}, Section {}, Subsection {}",
                        title,
                        chapter_idx + 1,
                        section_idx + 1,
                        subsection_num
                    );
                }
            }
        }
        Resource::YouTubePlaylist { hint, url } => {
            if let Some(h) = hint {
                println!("🎬 Selected playlist: {}", h);
            }
            println!("🔗 {}", url);
            println!("(Playlist video selection not yet implemented)");
        }
        Resource::YouTubeVideo { hint, url } => {
            if let Some(h) = hint {
                println!("🎥 Selected video: {}", h);
            }
            println!("🔗 {}", url);
            println!("(Random timestamp not yet implemented)");
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Pick { filter } => pick_random_resource(filter)?,
        Commands::Config => open_config()?,
    }

    Ok(())
}
