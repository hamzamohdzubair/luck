use anyhow::Result;
use rusqlite::{Connection, params};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use crate::resources::{migrate_expand_pdf_folders, migrate_expand_pdf_folders_v2};
use crate::tags::{TYPE_TAG_MAP, DEFAULT_TOPIC_TAGS};

pub fn get_db_path() -> Result<PathBuf> {
    let dir = dirs::data_dir()
        .ok_or_else(|| anyhow::anyhow!("Could not find data directory"))?
        .join("luck");
    fs::create_dir_all(&dir)?;
    Ok(dir.join("luck.db"))
}

pub fn open_db() -> Result<Connection> {
    let path = get_db_path()?;
    let conn = Connection::open(&path)?;
    conn.execute_batch("
        CREATE TABLE IF NOT EXISTS resources (
            id          INTEGER PRIMARY KEY,
            type        TEXT NOT NULL,
            name        TEXT NOT NULL,
            url         TEXT,
            path        TEXT,
            pages       INTEGER,
            video_count INTEGER,
            pick_count  INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS tags (
            id     INTEGER PRIMARY KEY,
            name   TEXT NOT NULL UNIQUE,
            weight REAL NOT NULL DEFAULT 1.0
        );
        CREATE TABLE IF NOT EXISTS resource_tags (
            resource_id INTEGER NOT NULL,
            tag_id      INTEGER NOT NULL,
            PRIMARY KEY (resource_id, tag_id)
        );
        CREATE TABLE IF NOT EXISTS pdf_scan_cache (
            path        TEXT PRIMARY KEY,
            pdf_count   INTEGER NOT NULL,
            total_pages INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS yt_duration_cache (
            url           TEXT PRIMARY KEY,
            duration_secs INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pdf_file_cache (
            path  TEXT PRIMARY KEY,
            pages INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS migrations (
            name TEXT PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS tracked_folders (
            id             INTEGER PRIMARY KEY,
            path           TEXT NOT NULL UNIQUE,
            added_at       INTEGER NOT NULL DEFAULT (strftime('%s','now')),
            last_synced_at INTEGER
        );
    ")?;

    seed_tags(&conn)?;

    // type_tag_backfill is superseded by flatten_resources
    conn.execute(
        "INSERT OR IGNORE INTO migrations (name) VALUES ('type_tag_backfill')",
        [],
    )?;

    flatten_resources(&conn)?;
    migrate_dir_resources(&conn)?;
    cleanup_dir_tag(&conn)?;
    migrate_expand_pdf_folders(&conn)?;
    migrate_expand_pdf_folders_v2(&conn)?;

    Ok(conn)
}

pub fn seed_tags(conn: &Connection) -> Result<()> {
    let type_tag_names: Vec<&str> = TYPE_TAG_MAP.iter().map(|(n, _)| *n).collect();
    for name in type_tag_names.iter().chain(DEFAULT_TOPIC_TAGS.iter()) {
        conn.execute(
            "INSERT OR IGNORE INTO tags (name, weight) VALUES (?1, 1.0)",
            params![name],
        )?;
    }
    Ok(())
}

fn flatten_resources(conn: &Connection) -> Result<()> {
    let needed = conn.execute(
        "INSERT OR IGNORE INTO migrations (name) VALUES ('flatten_resources')",
        [],
    )? > 0;
    if !needed {
        return Ok(());
    }

    // Create legacy tables if they exist on disk but weren't created this session
    conn.execute_batch("
        CREATE TABLE IF NOT EXISTS yt_playlists (
            id INTEGER PRIMARY KEY, name TEXT NOT NULL, url TEXT NOT NULL UNIQUE, video_count INTEGER
        );
        CREATE TABLE IF NOT EXISTS yt_videos (
            id INTEGER PRIMARY KEY, name TEXT NOT NULL, url TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS pdf_folders (
            id INTEGER PRIMARY KEY, path TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS physical_books (
            id INTEGER PRIMARY KEY, title TEXT NOT NULL, pages INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS links (
            id INTEGER PRIMARY KEY, name TEXT NOT NULL, url TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS table_pick_counts (
            table_id INTEGER PRIMARY KEY, pick_count INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS structured_books (
            id INTEGER PRIMARY KEY, title TEXT NOT NULL, sections TEXT NOT NULL
        );
    ")?;

    // Migrate rows from legacy type tables into the unified resources table
    conn.execute_batch("
        INSERT OR IGNORE INTO resources (type, name, url, video_count)
            SELECT 'playlist', name, url, video_count FROM yt_playlists;
        INSERT OR IGNORE INTO resources (type, name, url)
            SELECT 'video', name, url FROM yt_videos;
        INSERT OR IGNORE INTO resources (type, name, path)
            SELECT 'pdf', path, path FROM pdf_folders;
        INSERT OR IGNORE INTO resources (type, name, pages)
            SELECT 'book', title, pages FROM physical_books;
        INSERT OR IGNORE INTO resources (type, name, url)
            SELECT 'link', name, url FROM links;
    ")?;

    // Migrate resource_tags if it still uses the old (resource_table_id, resource_id) schema
    let has_old_schema: bool = conn.query_row(
        "SELECT COUNT(*) FROM pragma_table_info('resource_tags') WHERE name='resource_table_id'",
        [],
        |r| r.get::<_, i64>(0),
    ).unwrap_or(0) > 0;

    if has_old_schema {
        // Build (old_table_id, old_row_id) -> new resources.id mapping
        let mut id_map: HashMap<(u32, i64), i64> = HashMap::new();

        let mappings: &[(&str, u32, &str)] = &[
            ("SELECT p.id, r.id FROM yt_playlists p JOIN resources r ON r.type='playlist' AND r.url=p.url", 1, ""),
            ("SELECT v.id, r.id FROM yt_videos v JOIN resources r ON r.type='video' AND r.url=v.url", 2, ""),
            ("SELECT f.id, r.id FROM pdf_folders f JOIN resources r ON r.type='pdf' AND r.path=f.path", 3, ""),
            ("SELECT b.id, r.id FROM physical_books b JOIN resources r ON r.type='book' AND r.name=b.title AND r.pages=b.pages", 4, ""),
            ("SELECT l.id, r.id FROM links l JOIN resources r ON r.type='link' AND r.url=l.url", 5, ""),
        ];
        for (sql, tid, _) in mappings {
            let mut stmt = conn.prepare(sql)?;
            let rows: Vec<(i64, i64)> = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                .filter_map(|r| r.ok()).collect();
            for (old_id, new_id) in rows {
                id_map.insert((*tid, old_id), new_id);
            }
        }

        // Read all old resource_tags
        let old_rt: Vec<(u32, i64, i64)> = {
            let mut stmt = conn.prepare(
                "SELECT resource_table_id, resource_id, tag_id FROM resource_tags",
            )?;
            let rows: Vec<_> = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                .filter_map(|r| r.ok()).collect();
            rows
        };

        // Create new resource_tags table with correct schema
        conn.execute_batch("
            CREATE TABLE resource_tags_new (
                resource_id INTEGER NOT NULL,
                tag_id      INTEGER NOT NULL,
                PRIMARY KEY (resource_id, tag_id)
            );
        ")?;

        for (tid, rid, tag_id) in old_rt {
            if let Some(&new_id) = id_map.get(&(tid, rid)) {
                conn.execute(
                    "INSERT OR IGNORE INTO resource_tags_new (resource_id, tag_id) VALUES (?1, ?2)",
                    params![new_id, tag_id],
                )?;
            }
        }

        conn.execute_batch("
            DROP TABLE resource_tags;
            ALTER TABLE resource_tags_new RENAME TO resource_tags;
        ")?;
    }

    // Drop legacy tables
    conn.execute_batch("
        DROP TABLE IF EXISTS yt_playlists;
        DROP TABLE IF EXISTS yt_videos;
        DROP TABLE IF EXISTS pdf_folders;
        DROP TABLE IF EXISTS physical_books;
        DROP TABLE IF EXISTS links;
        DROP TABLE IF EXISTS table_pick_counts;
        DROP TABLE IF EXISTS structured_books;
    ")?;

    Ok(())
}

fn migrate_dir_resources(conn: &Connection) -> Result<()> {
    let needed = conn.execute(
        "INSERT OR IGNORE INTO migrations (name) VALUES ('migrate_dir_resources')",
        [],
    )? > 0;
    if !needed {
        return Ok(());
    }

    // Convert Google Drive/Docs links stored as 'link' type to 'dir'
    conn.execute(
        "UPDATE resources SET type='dir' \
         WHERE type='link' AND (url LIKE '%drive.google.com%' OR url LIKE '%docs.google.com%')",
        [],
    )?;

    // Remove type tags (link/pdf/book/physical) that no longer apply to dir resources
    let dir_ids: Vec<i64> = {
        let mut stmt = conn.prepare("SELECT id FROM resources WHERE type='dir'")?;
        let v: Vec<i64> = stmt.query_map([], |r| r.get(0))?
            .filter_map(|r| r.ok())
            .collect();
        v
    };

    for id in &dir_ids {
        for tag_name in &["link", "pdf", "book", "physical"] {
            if let Ok(tid) = conn.query_row(
                "SELECT id FROM tags WHERE name=?1", params![tag_name], |r| r.get::<_, i64>(0),
            ) {
                conn.execute(
                    "DELETE FROM resource_tags WHERE resource_id=?1 AND tag_id=?2",
                    params![id, tid],
                )?;
            }
        }
    }

    Ok(())
}

fn cleanup_dir_tag(conn: &Connection) -> Result<()> {
    let needed = conn.execute(
        "INSERT OR IGNORE INTO migrations (name) VALUES ('cleanup_dir_tag')",
        [],
    )? > 0;
    if !needed {
        return Ok(());
    }
    // Remove "dir" from resource_tags and tags table — it's a type, not a filterable tag
    if let Ok(tid) = conn.query_row(
        "SELECT id FROM tags WHERE name='dir'", [], |r| r.get::<_, i64>(0),
    ) {
        conn.execute("DELETE FROM resource_tags WHERE tag_id=?1", params![tid])?;
        conn.execute("DELETE FROM tags WHERE id=?1", params![tid])?;
    }
    Ok(())
}

pub fn count_all_resources(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row("SELECT COUNT(*) FROM resources", [], |r| r.get(0))?)
}

pub fn increment_pick_count(conn: &Connection, resource_id: i64) -> Result<()> {
    conn.execute(
        "UPDATE resources SET pick_count = pick_count + 1 WHERE id = ?1",
        params![resource_id],
    )?;
    Ok(())
}

pub fn get_tracked_folders(conn: &Connection) -> Result<Vec<(i64, String)>> {
    let mut stmt = conn.prepare("SELECT id, path FROM tracked_folders ORDER BY id")?;
    let v: Vec<(i64, String)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .filter_map(|r| r.ok())
        .collect();
    Ok(v)
}
