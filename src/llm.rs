use anyhow::{Context, Result};
use rusqlite::{Connection, params};
use std::io::{self, Write};
use walkdir::WalkDir;

use crate::tags::{apply_named_tag, apply_named_tags, get_topic_vocab};
use crate::utils::wsl_path;

pub enum ApiProvider {
    OpenAICompat { base_url: String, model: String, key: String },
    Anthropic { key: String },
}

pub fn detect_api() -> Option<ApiProvider> {
    if let Ok(key) = std::env::var("GROQ_API_KEY") {
        return Some(ApiProvider::OpenAICompat {
            base_url: "https://api.groq.com/openai/v1".to_string(),
            model: "llama-3.3-70b-versatile".to_string(),
            key,
        });
    }
    if let Ok(key) = std::env::var("ANTHROPIC_API_KEY") {
        return Some(ApiProvider::Anthropic { key });
    }
    if let Ok(key) = std::env::var("OPENAI_API_KEY") {
        return Some(ApiProvider::OpenAICompat {
            base_url: "https://api.openai.com/v1".to_string(),
            model: "gpt-4o-mini".to_string(),
            key,
        });
    }
    None
}

pub fn call_llm_for_tags(api: &ApiProvider, metadata: &str, vocab: &[String]) -> Result<Vec<String>> {
    let vocab_str = vocab.join(", ");
    let prompt = format!(
        "Categorize this learning resource. Return a JSON array of applicable topic tags \
         from this closed vocabulary: [{}]\n\
         Only include tags that clearly apply. Return [] if none match.\n\
         Return ONLY the JSON array, no surrounding text.\n\n\
         Resource metadata:\n{}",
        vocab_str, metadata
    );

    let raw = match api {
        ApiProvider::OpenAICompat { base_url, model, key } => {
            let body = serde_json::json!({
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "max_tokens": 120
            });
            let resp = ureq::post(&format!("{}/chat/completions", base_url))
                .set("Authorization", &format!("Bearer {}", key))
                .set("Content-Type", "application/json")
                .send_json(body)
                .context("LLM API call failed")?;
            let json: serde_json::Value = resp.into_json()?;
            json["choices"][0]["message"]["content"]
                .as_str()
                .unwrap_or("[]")
                .to_string()
        }
        ApiProvider::Anthropic { key } => {
            let body = serde_json::json!({
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 120,
                "messages": [{"role": "user", "content": prompt}]
            });
            let resp = ureq::post("https://api.anthropic.com/v1/messages")
                .set("x-api-key", key)
                .set("anthropic-version", "2023-06-01")
                .set("Content-Type", "application/json")
                .send_json(body)
                .context("Anthropic API call failed")?;
            let json: serde_json::Value = resp.into_json()?;
            json["content"][0]["text"]
                .as_str()
                .unwrap_or("[]")
                .to_string()
        }
    };

    let raw = raw.trim();
    let start = raw.find('[').unwrap_or(0);
    let end = raw.rfind(']').map(|i| i + 1).unwrap_or(raw.len());
    let tags: Vec<String> = serde_json::from_str(&raw[start..end]).unwrap_or_default();

    Ok(tags
        .into_iter()
        .map(|t| t.trim_start_matches('#').to_lowercase())
        .filter(|t| vocab.contains(t))
        .collect())
}

pub fn build_pdf_metadata(path: &str) -> String {
    let resolved = wsl_path(std::path::Path::new(path));
    let first_pdf = WalkDir::new(&resolved)
        .into_iter()
        .filter_map(|e| e.ok())
        .find(|e| {
            e.path()
                .extension()
                .and_then(|x| x.to_str())
                .map(|x| x.eq_ignore_ascii_case("pdf"))
                .unwrap_or(false)
        });

    let mut meta = format!("Folder path: {}", path);
    if let Some(entry) = first_pdf {
        let pdf_path = entry.path();
        if let Some(name) = pdf_path.file_stem().and_then(|n| n.to_str()) {
            meta.push_str(&format!("\nFilename: {}", name));
        }
        if let Ok(doc) = lopdf::Document::load(pdf_path) {
            if let Some(info) = extract_pdf_info(&doc) {
                meta.push_str(&format!("\nPDF metadata: {}", info));
            }
        }
        if let Ok(text) = pdf_extract::extract_text(pdf_path) {
            let preview: String = text.split_whitespace().take(250).collect::<Vec<_>>().join(" ");
            if !preview.is_empty() {
                meta.push_str(&format!("\nContent preview: {}", preview));
            }
        }
    }
    meta
}

fn extract_pdf_info(doc: &lopdf::Document) -> Option<String> {
    let info_ref = doc.trailer.get(b"Info").ok()?.as_reference().ok()?;
    let obj = doc.get_object(info_ref).ok()?;
    if let lopdf::Object::Dictionary(dict) = obj {
        let mut parts = Vec::new();
        for key in &[b"Title" as &[u8], b"Author", b"Subject"] {
            if let Ok(val) = dict.get(key) {
                if let Ok(s) = val.as_str() {
                    let decoded = String::from_utf8_lossy(s).trim().to_string();
                    if !decoded.is_empty() && decoded != "NA" {
                        parts.push(decoded);
                    }
                }
            }
        }
        if !parts.is_empty() {
            return Some(parts.join(" | "));
        }
    }
    None
}

pub fn get_folder_topic_tags(conn: &Connection, metadata: &str) -> Result<Vec<String>> {
    let vocab = get_topic_vocab(conn)?;
    if vocab.is_empty() {
        return Ok(vec![]);
    }
    let suggested = match detect_api() {
        Some(api) => {
            eprint!("Tagging...");
            let tags = call_llm_for_tags(&api, metadata, &vocab).unwrap_or_else(|e| {
                eprintln!(" (LLM error: {})", e);
                vec![]
            });
            eprintln!(" done");
            tags
        }
        None => {
            eprintln!(
                "No API key found (GROQ_API_KEY / ANTHROPIC_API_KEY / OPENAI_API_KEY). \
                 Skipping topic tags."
            );
            vec![]
        }
    };
    confirm_tags(suggested, &vocab)
}

pub fn prompt_and_apply_topic_tags(
    conn: &Connection,
    id: i64,
    metadata: &str,
) -> Result<()> {
    let vocab = get_topic_vocab(conn)?;
    if vocab.is_empty() {
        return Ok(());
    }

    let suggested = match detect_api() {
        Some(api) => {
            eprint!("Tagging...");
            let tags = call_llm_for_tags(&api, metadata, &vocab).unwrap_or_else(|e| {
                eprintln!(" (LLM error: {})", e);
                vec![]
            });
            eprintln!(" done");
            tags
        }
        None => {
            eprintln!(
                "No API key found (GROQ_API_KEY / ANTHROPIC_API_KEY / OPENAI_API_KEY). \
                 Skipping topic tags."
            );
            vec![]
        }
    };

    let confirmed = confirm_tags(suggested, &vocab)?;
    apply_named_tags(conn, id, &confirmed)?;
    Ok(())
}

pub fn prompt_link_type_tags(conn: &Connection, id: i64) -> Result<()> {
    print!("Link tags (e.g. practice, course — space-separated, empty=skip): ");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let input = input.trim();
    if input.is_empty() {
        return Ok(());
    }
    for tag_name in input.split_whitespace() {
        let tag_name = tag_name.trim_start_matches('#').to_lowercase();
        conn.execute(
            "INSERT OR IGNORE INTO tags (name, weight) VALUES (?1, 1.0)",
            params![tag_name],
        )?;
        apply_named_tag(conn, id, &tag_name)?;
    }
    Ok(())
}

fn confirm_tags(suggested: Vec<String>, vocab: &[String]) -> Result<Vec<String>> {
    if suggested.is_empty() {
        print!("No topics suggested. Enter manually (space-separated, empty=skip): ");
    } else {
        let display: Vec<String> = suggested.iter().map(|t| format!("#{}", t)).collect();
        print!("Topics: {}  [Enter=accept, n=skip, or type override]: ", display.join(" "));
    }
    io::stdout().flush()?;

    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    let line = line.trim();

    if suggested.is_empty() {
        if line.is_empty() {
            return Ok(vec![]);
        }
        return parse_tag_input(line, vocab);
    }

    match line.to_lowercase().as_str() {
        "" | "y" | "yes" => Ok(suggested),
        "n" | "no" => Ok(vec![]),
        other => parse_tag_input(other, vocab),
    }
}

fn parse_tag_input(input: &str, vocab: &[String]) -> Result<Vec<String>> {
    let tags: Vec<String> = input
        .split_whitespace()
        .map(|t| t.trim_start_matches('#').to_lowercase())
        .filter(|t| !t.is_empty())
        .collect();
    for tag in &tags {
        if !vocab.contains(tag) {
            eprintln!(
                "Warning: '{}' not in vocabulary (use `luck topics add {}` first)",
                tag, tag
            );
        }
    }
    Ok(tags.into_iter().filter(|t| vocab.contains(t)).collect())
}
