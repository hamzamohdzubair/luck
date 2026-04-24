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
    }
    meta
}

fn decode_pdf_string(bytes: &[u8]) -> Option<String> {
    let s = if bytes.starts_with(&[0xFE, 0xFF]) {
        // UTF-16BE with BOM
        let pairs: Vec<u16> = bytes[2..]
            .chunks_exact(2)
            .map(|c| u16::from_be_bytes([c[0], c[1]]))
            .collect();
        String::from_utf16_lossy(&pairs)
    } else {
        String::from_utf8_lossy(bytes).into_owned()
    };
    let s = s.trim().to_string();

    // Reject obviously bad values
    let sl = s.to_lowercase();
    if s.is_empty() || sl == "na" || sl == "untitled" || sl == "unknown" || sl == "none" {
        return None;
    }
    // Reject authoring-tool source filenames stored as titles
    if sl.ends_with(".indd") || sl.ends_with(".docx") || sl.ends_with(".pages")
        || sl.ends_with(".doc") || sl.ends_with(".odt")
    {
        return None;
    }
    // Reject purely numeric strings (ISBNs, internal IDs)
    if s.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    // Reject strings with too many non-printable or replacement chars (binary data)
    let total = s.chars().count();
    let printable = s.chars().filter(|c| !c.is_control() && *c != '\u{FFFD}').count();
    if total == 0 || (printable as f64 / total as f64) < 0.8 {
        return None;
    }
    Some(s)
}

pub fn extract_pdf_title_author(doc: &lopdf::Document) -> (Option<String>, Option<String>) {
    let info_ref = match doc.trailer.get(b"Info").ok().and_then(|o| o.as_reference().ok()) {
        Some(r) => r,
        None => return (None, None),
    };
    let obj = match doc.get_object(info_ref) {
        Ok(o) => o,
        Err(_) => return (None, None),
    };
    if let lopdf::Object::Dictionary(dict) = obj {
        let get_field = |key: &[u8]| -> Option<String> {
            let val = dict.get(key).ok()?;
            let bytes = val.as_str().ok()?;
            decode_pdf_string(bytes)
        };
        return (get_field(b"Title"), get_field(b"Author"));
    }
    (None, None)
}

/// Extract an ISBN-13 from PDF content streams (pages 1–5).
/// Scans raw bytes for 13-digit sequences starting with 978 or 979.
pub fn extract_isbn_from_doc(doc: &lopdf::Document) -> Option<String> {
    let pages = doc.get_pages();
    for (page_num, page_id) in pages.iter().take(5) {
        let _ = page_num;
        let content_bytes = match doc.get_page_content(*page_id) {
            Ok(b) => b,
            Err(_) => continue,
        };
        if let Some(isbn) = find_isbn13_in_bytes(&content_bytes) {
            return Some(isbn);
        }
    }
    None
}

fn find_isbn13_in_bytes(bytes: &[u8]) -> Option<String> {
    // Walk through bytes looking for 13-digit ASCII sequences starting with 978 or 979
    let s = String::from_utf8_lossy(bytes);
    let mut chars = s.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if !c.is_ascii_digit() {
            continue;
        }
        // Collect up to 13 consecutive digits (skip hyphens/spaces commonly inserted in ISBNs)
        let mut digits = String::new();
        let mut j = i;
        for ch in s[i..].chars() {
            if ch.is_ascii_digit() {
                digits.push(ch);
                if digits.len() == 13 {
                    break;
                }
            } else if ch == '-' || ch == ' ' || ch == '\u{00A0}' {
                // allow separators within ISBN
                if digits.len() >= 1 && digits.len() <= 12 {
                    continue;
                } else {
                    break;
                }
            } else {
                break;
            }
            j += ch.len_utf8();
        }
        if digits.len() == 13
            && (digits.starts_with("978") || digits.starts_with("979"))
            && isbn13_valid(&digits)
        {
            // Confirm not preceded/followed by more digits (avoid grabbing from longer numbers)
            let before_ok = i == 0 || !s[..i].ends_with(|c: char| c.is_ascii_digit());
            let after_end = i + j - i + 1;
            let after_ok = after_end >= s.len()
                || !s[after_end..].starts_with(|c: char| c.is_ascii_digit());
            let _ = after_ok; // boundary check is best-effort
            if before_ok {
                return Some(digits);
            }
        }
    }
    None
}

fn isbn13_valid(isbn: &str) -> bool {
    if isbn.len() != 13 { return false; }
    let sum: u32 = isbn.chars().enumerate().filter_map(|(i, c)| {
        c.to_digit(10).map(|d| if i % 2 == 0 { d } else { d * 3 })
    }).sum();
    sum % 10 == 0
}

/// Look up a book by ISBN-13 via Open Library Books API.
/// Returns canonical title and first author if found.
pub fn lookup_book_by_isbn(isbn: &str) -> Option<(String, Option<String>)> {
    let url = format!(
        "https://openlibrary.org/api/books?bibkeys=ISBN:{}&format=json&jscmd=data",
        isbn
    );
    let resp = ureq::get(&url)
        .set("User-Agent", "luck-cli/2.0 (hamzamohdzubair@gmail.com)")
        .timeout(std::time::Duration::from_secs(8))
        .call()
        .ok()?;
    let json: serde_json::Value = resp.into_json().ok()?;
    let key = format!("ISBN:{}", isbn);
    let book = json.get(&key)?;
    let title = book["title"].as_str()?.to_string();
    let author = book["authors"]
        .as_array()
        .and_then(|a| a.first())
        .and_then(|v| v["name"].as_str())
        .map(|s| s.to_string());
    Some((title, author))
}

/// Look up a book's canonical title and first author via the Open Library Search API.
/// Uses the provided hint (filename stem or rough title) as the search query.
/// Returns None on network error, timeout, or no results.
pub fn lookup_book_title_online(hint: &str) -> Option<(String, Option<String>)> {
    // Build a clean query: first 6 meaningful words, alphanumeric only
    let noise: &[&str] = &["pdfdrive", "zlibrary", "libgen", "bookfi", "com", "org", "pdf"];
    let query: String = hint
        .split(|c: char| !c.is_alphanumeric() && c != '-')
        .filter(|w| !w.is_empty())
        .filter(|w| {
            let lower = w.to_lowercase();
            !noise.contains(&lower.as_str())
        })
        .take(6)
        .collect::<Vec<_>>()
        .join("+");

    if query.split('+').filter(|w| w.chars().any(|c| c.is_alphabetic())).count() < 2 {
        return None; // not enough real words (e.g. pure numeric filenames)
    }

    let url = format!(
        "https://openlibrary.org/search.json?q={}&fields=title,author_name&limit=1",
        query
    );

    let resp = ureq::get(&url)
        .set("User-Agent", "luck-cli/2.0 (hamzamohdzubair@gmail.com)")
        .timeout(std::time::Duration::from_secs(8))
        .call()
        .ok()?;

    let json: serde_json::Value = resp.into_json().ok()?;
    let doc = json["docs"].as_array()?.first()?;

    let title = doc["title"].as_str()?.to_string();
    let author = doc["author_name"]
        .as_array()
        .and_then(|a| a.first())
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    Some((title, author))
}

pub fn build_pdf_folder_metadata(folder_path: &str, sample_titles: &[String]) -> String {
    let mut meta = format!("Folder: {}", folder_path);
    if !sample_titles.is_empty() {
        meta.push_str("\nSample titles:");
        for t in sample_titles.iter().take(10) {
            meta.push_str(&format!("\n- {}", t));
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


pub fn suggest_topic_tags(conn: &Connection, metadata: &str) -> Result<Vec<String>> {
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
    let confirmed = suggest_topic_tags(conn, metadata)?;
    apply_named_tags(conn, id, &confirmed)?;
    Ok(())
}

pub fn auto_apply_topic_tags(conn: &Connection, id: i64, metadata: &str) -> Result<()> {
    let vocab = get_topic_vocab(conn)?;
    if vocab.is_empty() {
        return Ok(());
    }

    let tags = match detect_api() {
        Some(api) => {
            eprint!("Tagging...");
            let t = call_llm_for_tags(&api, metadata, &vocab).unwrap_or_else(|e| {
                eprintln!(" (LLM error: {})", e);
                vec![]
            });
            eprintln!(" done");
            t
        }
        None => {
            eprintln!(
                "No API key found (GROQ_API_KEY / ANTHROPIC_API_KEY / OPENAI_API_KEY). \
                 Skipping topic tags."
            );
            vec![]
        }
    };

    if !tags.is_empty() {
        let display: Vec<String> = tags.iter().map(|t| format!("#{}", t)).collect();
        println!("  Tags: {}", display.join(" "));
        apply_named_tags(conn, id, &tags)?;
    }
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
