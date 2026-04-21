use anyhow::Result;
use rusqlite::Connection;
use std::collections::HashMap;

use crate::db::count_all_resources;
use crate::tags::{LEAF_TYPE_TAGS, TYPE_TAG_MAP};
use crate::utils::format_hm;

pub struct TagStat {
    pub id: i64,
    pub name: String,
    pub is_type_tag: bool,
    pub is_leaf_type: bool,
    pub weight: f64,
    pub count: i64,
    pub entries: String,
    pub picks: i64,
    pub curr_prob: f64,
    pub whatif_prob: f64,
    pub uniform_weight: Option<f64>,
}

fn entries_desc(conn: &Connection, name: &str, count: i64) -> String {
    match name {
        "video" => {
            let secs: i64 = conn.query_row(
                "SELECT COALESCE(SUM(d.duration_secs),0) FROM resources r \
                 JOIN yt_duration_cache d ON d.url=r.url WHERE r.type='video'",
                [], |r| r.get(0),
            ).unwrap_or(0);
            if secs > 0 { format!("{}vid, {}", count, format_hm(secs as u64)) }
            else { format!("{}vid", count) }
        }
        "playlist" => {
            let videos: i64 = conn.query_row(
                "SELECT COALESCE(SUM(video_count),0) FROM resources WHERE type='playlist'",
                [], |r| r.get(0),
            ).unwrap_or(0);
            let secs: i64 = conn.query_row(
                "SELECT COALESCE(SUM(d.duration_secs),0) FROM resources r \
                 JOIN yt_duration_cache d ON d.url=r.url WHERE r.type='playlist'",
                [], |r| r.get(0),
            ).unwrap_or(0);
            if secs > 0 { format!("{}pl, {}vid, {}", count, videos, format_hm(secs as u64)) }
            else { format!("{}pl, {}vid", count, videos) }
        }
        "youtube" => {
            let standalone_vids: i64 = conn.query_row(
                "SELECT COUNT(*) FROM resources WHERE type='video'", [], |r| r.get(0),
            ).unwrap_or(0);
            let pls: i64 = conn.query_row(
                "SELECT COUNT(*) FROM resources WHERE type='playlist'", [], |r| r.get(0),
            ).unwrap_or(0);
            let pl_vids: i64 = conn.query_row(
                "SELECT COALESCE(SUM(video_count),0) FROM resources WHERE type='playlist'",
                [], |r| r.get(0),
            ).unwrap_or(0);
            let total_vids = standalone_vids + pl_vids;
            let secs: i64 = conn.query_row(
                "SELECT COALESCE(SUM(d.duration_secs),0) FROM resources r \
                 JOIN yt_duration_cache d ON d.url=r.url",
                [], |r| r.get(0),
            ).unwrap_or(0);
            if secs > 0 { format!("{}vid, {}pl, {}", total_vids, pls, format_hm(secs as u64)) }
            else { format!("{}vid, {}pl", total_vids, pls) }
        }
        "pdf" => {
            let docs: i64 = conn.query_row(
                "SELECT COALESCE(SUM(pdf_count),0) FROM pdf_scan_cache", [], |r| r.get(0),
            ).unwrap_or(0);
            let pages: i64 = conn.query_row(
                "SELECT COALESCE(SUM(total_pages),0) FROM pdf_scan_cache", [], |r| r.get(0),
            ).unwrap_or(0);
            if docs > 0 { format!("{}dir, {}doc, {}pg", count, docs, pages) }
            else { format!("{}dir", count) }
        }
        "physical" => {
            let pages: i64 = conn.query_row(
                "SELECT COALESCE(SUM(pages),0) FROM resources WHERE type='book'",
                [], |r| r.get(0),
            ).unwrap_or(0);
            format!("{}bk, {}pg", count, pages)
        }
        "book" => {
            let dirs: i64 = conn.query_row(
                "SELECT COUNT(*) FROM resources WHERE type='pdf'", [], |r| r.get(0),
            ).unwrap_or(0);
            let docs: i64 = conn.query_row(
                "SELECT COALESCE(SUM(pdf_count),0) FROM pdf_scan_cache", [], |r| r.get(0),
            ).unwrap_or(0);
            let bks: i64 = conn.query_row(
                "SELECT COUNT(*) FROM resources WHERE type='book'", [], |r| r.get(0),
            ).unwrap_or(0);
            let pages: i64 = conn.query_row(
                "SELECT COALESCE(SUM(total_pages),0) FROM pdf_scan_cache", [], |r| r.get(0),
            ).unwrap_or(0) + conn.query_row(
                "SELECT COALESCE(SUM(pages),0) FROM resources WHERE type='book'",
                [], |r| r.get(0),
            ).unwrap_or(0);
            if docs > 0 { format!("{}dir, {}doc, {}bk, {}pg", dirs, docs, bks, pages) }
            else { format!("{}dir, {}bk, {}pg", dirs, bks, pages) }
        }
        "link" => format!("{}lnk", count),
        _ => count.to_string(),
    }
}

pub fn load_tag_stats(conn: &Connection) -> Result<Vec<TagStat>> {
    let mut stmt = conn.prepare("SELECT id, name, weight FROM tags ORDER BY name")?;
    let mapped = stmt.query_map([], |r| Ok((r.get::<_,i64>(0)?, r.get::<_,String>(1)?, r.get::<_,f64>(2)?)))?;
    let tags: Vec<(i64, String, f64)> = mapped.collect::<rusqlite::Result<_>>()?;

    let mut tag_resources: HashMap<i64, Vec<i64>> = HashMap::new();
    let mut resource_tag_ids: HashMap<i64, Vec<i64>> = HashMap::new();
    let mut stmt2 = conn.prepare("SELECT tag_id, resource_id FROM resource_tags")?;
    let mapped2 = stmt2.query_map([], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
    })?;
    for row in mapped2 {
        let (tag_id, rid) = row?;
        tag_resources.entry(tag_id).or_default().push(rid);
        resource_tag_ids.entry(rid).or_default().push(tag_id);
    }

    // picks per tag: sum of pick_count for all resources tagged with each tag
    let mut tag_picks: HashMap<i64, i64> = HashMap::new();
    let mut stmt3 = conn.prepare(
        "SELECT rt.tag_id, COALESCE(SUM(r.pick_count), 0) \
         FROM resource_tags rt JOIN resources r ON r.id = rt.resource_id \
         GROUP BY rt.tag_id",
    )?;
    let mapped3 = stmt3.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?;
    for row in mapped3 {
        let (tag_id, picks) = row?;
        tag_picks.insert(tag_id, picks);
    }

    let total_items = count_all_resources(conn)?;
    let weight_map: HashMap<i64, f64> = tags.iter().map(|(id, _, w)| (*id, *w)).collect();

    let resource_eff: HashMap<i64, f64> = resource_tag_ids
        .iter()
        .map(|(k, tag_ids)| {
            let eff: f64 = tag_ids
                .iter()
                .map(|tid| weight_map.get(tid).copied().unwrap_or(1.0))
                .product();
            (*k, eff)
        })
        .collect();

    let tagged_count = resource_eff.len() as i64;
    let total_eff: f64 =
        resource_eff.values().sum::<f64>() + (total_items - tagged_count) as f64;

    let leaf_set: std::collections::HashSet<&str> = LEAF_TYPE_TAGS.iter().copied().collect();
    let type_tag_names: std::collections::HashSet<&str> =
        TYPE_TAG_MAP.iter().map(|(n, _)| *n).collect();

    let max_leaf_count = tags
        .iter()
        .filter(|(_, name, _)| leaf_set.contains(name.as_str()))
        .map(|(id, _, _)| tag_resources.get(id).map_or(0, |v| v.len() as i64))
        .max()
        .unwrap_or(1);

    let mut stats: Vec<TagStat> = tags
        .iter()
        .map(|(id, name, weight)| {
            let resources = tag_resources.get(id).map(|v| v.as_slice()).unwrap_or(&[]);
            let count = resources.len() as i64;

            let tag_eff_sum: f64 = resources
                .iter()
                .map(|rid| resource_eff.get(rid).copied().unwrap_or(1.0))
                .sum();

            let curr_prob = if total_eff > 0.0 { tag_eff_sum / total_eff } else { 0.0 };
            let whatif_prob = if total_items > 0 { count as f64 / total_items as f64 } else { 0.0 };

            let is_type_tag = type_tag_names.contains(name.as_str());
            let is_leaf_type = leaf_set.contains(name.as_str());

            let uniform_weight = if is_leaf_type && count > 0 {
                Some(max_leaf_count as f64 / count as f64)
            } else {
                None
            };

            let entries = if is_type_tag {
                entries_desc(conn, name, count)
            } else {
                count.to_string()
            };

            let picks = tag_picks.get(id).copied().unwrap_or(0);

            TagStat { id: *id, name: name.clone(), is_type_tag, is_leaf_type, weight: *weight, count, entries, picks, curr_prob, whatif_prob, uniform_weight }
        })
        .collect();

    stats.sort_by(|a, b| match (a.is_type_tag, b.is_type_tag) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ if a.is_type_tag => match (a.is_leaf_type, b.is_leaf_type) {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            _ => a.name.cmp(&b.name),
        },
        _ => a.name.cmp(&b.name),
    });

    Ok(stats)
}

pub fn cmd_stats(conn: &Connection) -> Result<()> {
    let stats = load_tag_stats(conn)?;
    if stats.is_empty() {
        println!("No tags yet.");
        return Ok(());
    }

    let tag_w     = stats.iter().map(|s| s.name.len() + 1).max().unwrap_or(5).max(5);
    let entries_w = "Entries".len().max(stats.iter().map(|s| s.entries.len()).max().unwrap_or(0));
    let picks_w   = "Picks".len().max(stats.iter().map(|s| s.picks.to_string().len()).max().unwrap_or(0));

    let total_w = tag_w + 2 + entries_w + 2 + picks_w + 2 + 6 + 2 + 7 + 2 + 6 + 2 + 7 + 2;
    let sep = "─".repeat(total_w);

    println!("{}", sep);
    println!(
        " {:<tag_w$}  {:<entries_w$}  {:>picks_w$}  {:>6}  {:>7}  {:>6}  {:>7}",
        "TAG", "ENTRIES", "PICKS", "WEIGHT", "CURR%", "BASE%", "UNIF-W",
        tag_w = tag_w, entries_w = entries_w, picks_w = picks_w
    );

    let print_row = |s: &TagStat| {
        let tag_label = format!("#{}", s.name);
        let unif = s
            .uniform_weight
            .map_or("  -  ".to_string(), |w| format!("{:>6.2}", w));
        println!(
            " {:<tag_w$}  {:<entries_w$}  {:>picks_w$}  {:>6.2}  {:>6.1}%  {:>5.1}%  {}",
            tag_label, s.entries, s.picks, s.weight, s.curr_prob * 100.0, s.whatif_prob * 100.0, unif,
            tag_w = tag_w, entries_w = entries_w, picks_w = picks_w
        );
    };

    let type_stats: Vec<_> = stats.iter().filter(|s| s.is_type_tag).collect();
    let topic_stats: Vec<_> = stats.iter().filter(|s| !s.is_type_tag).collect();

    if !type_stats.is_empty() {
        println!("{}", sep);
        println!(" Type Tags");
        println!("{}", sep);
        for s in &type_stats { print_row(s); }
    }
    if !topic_stats.is_empty() {
        println!("{}", sep);
        println!(" Topic Tags");
        println!("{}", sep);
        for s in &topic_stats { print_row(s); }
    }
    println!("{}", sep);
    Ok(())
}
