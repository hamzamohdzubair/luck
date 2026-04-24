use anyhow::Result;
use rusqlite::{Connection, params};

use crate::db::open_db;
use crate::llm::{auto_apply_topic_tags, build_pdf_metadata, prompt_and_apply_topic_tags};
use crate::resources::{Resource, TYPE_BOOK, TYPE_DIR, TYPE_LINK, TYPE_PDF, TYPE_PLAYLIST, TYPE_VIDEO, load_all_resources, scan_pdfs};
use crate::tags::{TYPE_TAG_MAP, apply_type_tags};
use crate::utils::wsl_path;

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
        Resource::PdfFolder { path, .. } => build_pdf_metadata(&path.to_string_lossy()),
        Resource::PdfFile { name, .. } => format!("Title: {}", name),
    }
}

fn resource_type_str(resource: &Resource) -> &'static str {
    match resource {
        Resource::YouTubeVideo { .. } => TYPE_VIDEO,
        Resource::YouTubePlaylist { .. } => TYPE_PLAYLIST,
        Resource::Link { .. } => TYPE_LINK,
        Resource::Directory { .. } => TYPE_DIR,
        Resource::PhysicalBook { .. } => TYPE_BOOK,
        Resource::PdfFolder { .. } | Resource::PdfFile { .. } => TYPE_PDF,
    }
}

fn resource_display_name(resource: &Resource) -> &str {
    match resource {
        Resource::YouTubeVideo { name, .. } => name,
        Resource::YouTubePlaylist { name, .. } => name,
        Resource::Link { name, .. } => name,
        Resource::Directory { name, .. } => name,
        Resource::PhysicalBook { title, .. } => title,
        Resource::PdfFolder { name, .. } | Resource::PdfFile { name, .. } => name,
    }
}

fn retag_resource(conn: &Connection, resource: &Resource, force: bool) -> Result<()> {
    println!("[{}] {}", resource.id(), resource_display_name(resource));
    apply_type_tags(conn, resource_type_str(resource), resource.id())?;
    if force {
        auto_apply_topic_tags(conn, resource.id(), &resource_metadata(resource))?;
    } else {
        prompt_and_apply_topic_tags(conn, resource.id(), &resource_metadata(resource))?;
    }
    println!();
    Ok(())
}

/// Expand a PDF folder resource into individual file resources (no topic tags applied — each
/// file will be picked up by the untagged filter and tagged individually).
/// Returns the number of new resources created. The folder entry is removed on success.
fn expand_pdf_folder(conn: &Connection, folder_id: i64, path: &std::path::Path) -> Result<usize> {
    let resolved = wsl_path(path);
    if !resolved.is_dir() {
        return Ok(0);
    }
    let pdfs = scan_pdfs(&resolved);
    if pdfs.is_empty() {
        return Ok(0);
    }
    let mut created = 0usize;
    for pdf in &pdfs {
        let pdf_path = pdf.to_string_lossy().to_string();
        let exists: bool = conn.query_row(
            "SELECT COUNT(*) FROM resources WHERE type='pdf' AND path=?1",
            params![pdf_path], |r| r.get::<_, i64>(0),
        ).unwrap_or(0) > 0;
        if exists { continue; }
        let name = pdf.file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| pdf_path.clone());
        conn.execute(
            "INSERT INTO resources (type, name, path) VALUES ('pdf', ?1, ?2)",
            params![name, pdf_path],
        )?;
        let new_id = conn.last_insert_rowid();
        apply_type_tags(conn, TYPE_PDF, new_id)?;
        created += 1;
    }
    if created > 0 {
        conn.execute("DELETE FROM resource_tags WHERE resource_id=?1", params![folder_id])?;
        conn.execute("DELETE FROM resources WHERE id=?1", params![folder_id])?;
        println!("Expanded PDF folder into {} individual files.", created);
    }
    Ok(created)
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

pub fn cmd_retag(target: Option<String>, all: bool, force: bool) -> Result<()> {
    let conn = open_db()?;

    // Expand any accessible PDF folders into individual file resources so each
    // PDF can be tagged on its own. Folders that aren't mounted are left as-is.
    for resource in load_all_resources(&conn)? {
        if let Resource::PdfFolder { id, path, .. } = resource {
            expand_pdf_folder(&conn, id, &path)?;
        }
    }

    let resources = load_all_resources(&conn)?;

    if let Some(ref t) = target {
        if let Ok(id) = t.parse::<i64>() {
            let resource = resources
                .iter()
                .find(|r| r.id() == id)
                .ok_or_else(|| anyhow::anyhow!("No resource with id {}", id))?;
            return retag_resource(&conn, resource, force);
        }

        // treat as tag name
        let ids = ids_for_tag(&conn, t)?;
        if ids.is_empty() {
            anyhow::bail!("No resources found with tag '{}'", t);
        }
        println!("Retagging {} resource(s) with tag '{}'...\n", ids.len(), t);
        for resource in resources.iter().filter(|r| ids.contains(&r.id())) {
            retag_resource(&conn, resource, force)?;
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
        retag_resource(&conn, resource, force)?;
    }

    Ok(())
}
