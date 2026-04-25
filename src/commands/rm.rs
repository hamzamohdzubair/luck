use anyhow::Result;
use rusqlite::{Connection, params};

pub fn rm(conn: &Connection, id: i64) -> Result<()> {
    let row: Option<(String, Option<String>)> = conn
        .query_row("SELECT type, path FROM resources WHERE id = ?1", params![id], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .ok();
    let Some((rtype, path)) = row else {
        anyhow::bail!("No resource with ID {} found.", id);
    };
    let label = match rtype.as_str() {
        "playlist" => "playlist",
        "video"    => "video",
        "pdf" if path.as_deref().map(|p| p.to_lowercase().ends_with(".pdf")).unwrap_or(false)
                   => "PDF file",
        "pdf"      => "PDF folder",
        "book"     => "book",
        "link"     => "link",
        _          => "resource",
    };
    conn.execute("DELETE FROM resource_tags WHERE resource_id = ?1", params![id])?;
    conn.execute("DELETE FROM resources WHERE id = ?1", params![id])?;
    println!("Removed {} #{}.", label, id);
    Ok(())
}
