use anyhow::Result;
use rusqlite::{Connection, params};
use std::collections::HashMap;

/// (tag_name, resource types it applies to)
pub const TYPE_TAG_MAP: &[(&str, &[&str])] = &[
    ("video",    &["video"]),
    ("playlist", &["playlist"]),
    ("youtube",  &["video", "playlist"]),
    ("pdf",      &["pdf"]),
    ("physical", &["book"]),
    ("book",     &["pdf", "book"]),
    ("link",     &["link"]),
];

pub const LEAF_TYPE_TAGS: &[&str] = &["video", "playlist", "pdf", "physical", "link"];

pub const DEFAULT_TOPIC_TAGS: &[&str] = &[
    "physics", "chemistry", "geography", "law", "math", "ml",
    "programming", "cs", "electronics", "engineering", "civil",
    "philosophy", "society", "formal-science", "social-science",
    "applied-science", "natural-science", "humanities", "islam",
    "religion", "biology",
];

pub fn apply_type_tags(conn: &Connection, resource_type: &str, id: i64) -> Result<()> {
    for (tag_name, applies_to) in TYPE_TAG_MAP {
        if applies_to.contains(&resource_type) {
            apply_named_tag(conn, id, tag_name)?;
        }
    }
    Ok(())
}

pub fn apply_named_tag(conn: &Connection, id: i64, tag_name: &str) -> Result<()> {
    if let Ok(tag_id) = conn.query_row(
        "SELECT id FROM tags WHERE name = ?1",
        params![tag_name],
        |r| r.get::<_, i64>(0),
    ) {
        conn.execute(
            "INSERT OR IGNORE INTO resource_tags (resource_id, tag_id) VALUES (?1, ?2)",
            params![id, tag_id],
        )?;
    }
    Ok(())
}

pub fn apply_named_tags(conn: &Connection, id: i64, tag_names: &[String]) -> Result<()> {
    for name in tag_names {
        apply_named_tag(conn, id, name.trim_start_matches('#'))?;
    }
    Ok(())
}

pub fn get_topic_vocab(conn: &Connection) -> Result<Vec<String>> {
    let type_names: std::collections::HashSet<&str> =
        TYPE_TAG_MAP.iter().map(|(n, _)| *n).collect();
    let mut stmt = conn.prepare("SELECT name FROM tags ORDER BY name")?;
    let mapped = stmt.query_map([], |r| r.get::<_, String>(0))?;
    Ok(mapped
        .filter_map(|r| r.ok())
        .filter(|n| !type_names.contains(n.as_str()))
        .collect())
}

pub fn all_resource_eff_weights(conn: &Connection) -> Result<HashMap<i64, f64>> {
    let mut map: HashMap<i64, Vec<f64>> = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT rt.resource_id, t.weight FROM resource_tags rt JOIN tags t ON t.id = rt.tag_id",
    )?;
    let mapped = stmt.query_map([], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, f64>(1)?))
    })?;
    for row in mapped {
        let (rid, w) = row?;
        map.entry(rid).or_default().push(w);
    }
    Ok(map.into_iter().map(|(k, ws)| (k, ws.iter().product())).collect())
}
