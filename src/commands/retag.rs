use anyhow::Result;
use rusqlite::{Connection, params};

use crate::db::open_db;
use crate::llm::{build_pdf_metadata, prompt_and_apply_topic_tags};
use crate::resources::{Resource, TYPE_BOOK, TYPE_DIR, TYPE_LINK, TYPE_PDF, TYPE_PLAYLIST, TYPE_VIDEO, load_all_resources};
use crate::tags::{TYPE_TAG_MAP, apply_type_tags};

fn has_topic_tags(conn: &Connection, id: i64) -> Result<bool> {
    let type_tag_names: std::collections::HashSet<&str> =
        TYPE_TAG_MAP.iter().map(|(n, _)| *n).collect();
    let mut stmt = conn.prepare(
        "SELECT t.name FROM resource_tags rt JOIN tags t ON t.id = rt.tag_id WHERE rt.resource_id = ?1",
    )?;
    let names: Vec<String> = stmt
        .query_map(params![id], |r| r.get(0))?
        .filter_map(|r| r.ok())
        .collect();
    Ok(names.iter().any(|n| !type_tag_names.contains(n.as_str())))
}

fn resource_metadata(resource: &Resource) -> String {
    match resource {
        Resource::YouTubeVideo { name, url, .. } => format!("Title: {}\nURL: {}", name, url),
        Resource::YouTubePlaylist { name, url, .. } => format!("Title: {}\nURL: {}", name, url),
        Resource::Link { name, url, .. } => format!("Name: {}\nURL: {}", name, url),
        Resource::Directory { name, url, .. } => format!("Name: {}\nURL: {}", name, url),
        Resource::PhysicalBook { title, pages, .. } => format!("Title: {}\nPages: {}", title, pages),
        Resource::PdfFile { path, .. } => build_pdf_metadata(&path.to_string_lossy()),
    }
}

fn resource_type_str(resource: &Resource) -> &'static str {
    match resource {
        Resource::YouTubeVideo { .. } => TYPE_VIDEO,
        Resource::YouTubePlaylist { .. } => TYPE_PLAYLIST,
        Resource::Link { .. } => TYPE_LINK,
        Resource::Directory { .. } => TYPE_DIR,
        Resource::PhysicalBook { .. } => TYPE_BOOK,
        Resource::PdfFile { .. } => TYPE_PDF,
    }
}

fn resource_display_name(resource: &Resource) -> &str {
    match resource {
        Resource::YouTubeVideo { name, .. } => name,
        Resource::YouTubePlaylist { name, .. } => name,
        Resource::Link { name, .. } => name,
        Resource::Directory { name, .. } => name,
        Resource::PhysicalBook { title, .. } => title,
        Resource::PdfFile { name, .. } => name,
    }
}

fn retag_resource(conn: &Connection, resource: &Resource) -> Result<()> {
    println!("[{}] {}", resource.id(), resource_display_name(resource));
    apply_type_tags(conn, resource_type_str(resource), resource.id())?;
    prompt_and_apply_topic_tags(conn, resource.id(), &resource_metadata(resource))?;
    println!();
    Ok(())
}

fn ids_for_tag(conn: &Connection, tag: &str) -> Result<Vec<i64>> {
    let mut stmt = conn.prepare(
        "SELECT rt.resource_id FROM resource_tags rt JOIN tags t ON t.id = rt.tag_id WHERE t.name = ?1",
    )?;
    let ids: Vec<i64> = stmt
        .query_map(params![tag], |r| r.get(0))?
        .filter_map(|r| r.ok())
        .collect();
    Ok(ids)
}

pub fn cmd_retag(target: Option<String>, all: bool) -> Result<()> {
    let conn = open_db()?;
    let resources = load_all_resources(&conn)?;

    if let Some(ref t) = target {
        if let Ok(id) = t.parse::<i64>() {
            let resource = resources
                .iter()
                .find(|r| r.id() == id)
                .ok_or_else(|| anyhow::anyhow!("No resource with id {}", id))?;
            return retag_resource(&conn, resource);
        }

        // treat as tag name
        let ids = ids_for_tag(&conn, t)?;
        if ids.is_empty() {
            anyhow::bail!("No resources found with tag '{}'", t);
        }
        println!("Retagging {} resource(s) with tag '{}'...\n", ids.len(), t);
        for resource in resources.iter().filter(|r| ids.contains(&r.id())) {
            retag_resource(&conn, resource)?;
        }
        return Ok(());
    }

    let to_tag: Vec<&Resource> = if all {
        resources.iter().collect()
    } else {
        resources
            .iter()
            .filter(|r| !has_topic_tags(&conn, r.id()).unwrap_or(true))
            .collect()
    };

    if to_tag.is_empty() {
        println!("All resources have topic tags. Use `luck retag <id>` or `luck retag --all`.");
        return Ok(());
    }

    println!("{} resource(s) to tag...\n", to_tag.len());
    for resource in to_tag {
        retag_resource(&conn, resource)?;
    }

    Ok(())
}
