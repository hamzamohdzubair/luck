use anyhow::Result;
use rusqlite::{Connection, params};

pub fn rm(conn: &Connection, id: i64) -> Result<()> {
    let rtype: Option<String> = conn
        .query_row("SELECT type FROM resources WHERE id = ?1", params![id], |r| r.get(0))
        .ok();
    let label = match rtype.as_deref() {
        Some("playlist") => "playlist",
        Some("video")    => "video",
        Some("pdf")      => "PDF folder",
        Some("book")     => "book",
        Some("link")     => "link",
        Some(_)          => "resource",
        None             => anyhow::bail!("No resource with ID {} found.", id),
    };
    conn.execute("DELETE FROM resource_tags WHERE resource_id = ?1", params![id])?;
    conn.execute("DELETE FROM resources WHERE id = ?1", params![id])?;
    println!("Removed {} #{}.", label, id);
    Ok(())
}
