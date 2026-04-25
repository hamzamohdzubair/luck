use anyhow::Result;
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState},
    Frame, Terminal,
};
use rusqlite::{Connection, params};
use std::collections::HashMap;
use std::io;

use crate::commands::stats::{load_tag_stats, TagStat};
use crate::db::count_all_resources;

pub struct WeightsState {
    pub stats: Vec<TagStat>,
    pub table_state: TableState,
    pub editing: bool,
    pub edit_buf: String,
    pub dirty: bool,
    pub status: String,
    resource_tag_ids: HashMap<i64, Vec<i64>>,
    total_items: i64,
}

impl WeightsState {
    // Visual row index of the separator between type and topic tags, if both exist.
    pub fn separator_visual_idx(&self) -> Option<usize> {
        let tc = self.stats.iter().filter(|s| s.is_type_tag).count();
        if tc > 0 && tc < self.stats.len() { Some(tc) } else { None }
    }

    // Translate visual row index → stats slice index (None for the separator row).
    pub fn visual_to_stats(&self, visual: usize) -> Option<usize> {
        match self.separator_visual_idx() {
            None => Some(visual),
            Some(sep) if visual == sep => None,
            Some(sep) if visual > sep => Some(visual - 1),
            _ => Some(visual),
        }
    }

    // Total number of visual rows (stats + optional separator).
    pub fn visual_row_count(&self) -> usize {
        self.stats.len() + self.separator_visual_idx().map_or(0, |_| 1)
    }

    // Currently selected stats index (adjusted past the separator).
    pub fn selected_idx(&self) -> Option<usize> {
        self.table_state.selected().and_then(|v| self.visual_to_stats(v))
    }

    pub fn recompute_probs(&mut self) {
        let weight_map: HashMap<i64, f64> =
            self.stats.iter().map(|s| (s.id, s.weight)).collect();

        let resource_eff: HashMap<i64, f64> = self
            .resource_tag_ids
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
        let total_eff =
            resource_eff.values().sum::<f64>() + (self.total_items - tagged_count) as f64;

        let mut tag_res: HashMap<i64, Vec<i64>> = HashMap::new();
        for (rid, tag_ids) in &self.resource_tag_ids {
            for tag_id in tag_ids {
                tag_res.entry(*tag_id).or_default().push(*rid);
            }
        }

        for stat in &mut self.stats {
            let eff_sum: f64 = tag_res
                .get(&stat.id)
                .map(|rs| {
                    rs.iter()
                        .map(|rid| resource_eff.get(rid).copied().unwrap_or(1.0))
                        .sum()
                })
                .unwrap_or(0.0);
            stat.curr_prob = if total_eff > 0.0 { eff_sum / total_eff } else { 0.0 };
        }
    }
}

pub fn load_weights_state(conn: &Connection) -> Result<WeightsState> {
    let stats = load_tag_stats(conn)?;

    let mut resource_tag_ids: HashMap<i64, Vec<i64>> = HashMap::new();
    let mut stmt = conn.prepare("SELECT resource_id, tag_id FROM resource_tags")?;
    let mapped = stmt.query_map([], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
    })?;
    for row in mapped {
        let (rid, tag_id) = row?;
        resource_tag_ids.entry(rid).or_default().push(tag_id);
    }

    let total_items = count_all_resources(conn)?;

    let mut table_state = TableState::default();
    if !stats.is_empty() {
        table_state.select(Some(0));
    }

    Ok(WeightsState {
        stats,
        table_state,
        editing: false,
        edit_buf: String::new(),
        dirty: false,
        status: String::new(),
        resource_tag_ids,
        total_items,
    })
}

pub fn render_weights(f: &mut Frame, state: &mut WeightsState) {
    let area = f.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(5), Constraint::Length(2)])
        .split(area);

    let selected_stats = state.selected_idx();
    let sep_visual = state.separator_visual_idx();

    let mut rows: Vec<Row> = Vec::new();
    for (i, s) in state.stats.iter().enumerate() {
        if sep_visual == Some(i) {
            rows.push(
                Row::new(vec![
                    Cell::from(" ─── topic tags").style(
                        Style::default().fg(Color::DarkGray).add_modifier(Modifier::DIM),
                    ),
                    Cell::from(""),
                    Cell::from(""),
                    Cell::from(""),
                    Cell::from(""),
                    Cell::from(""),
                ])
                .style(Style::default().fg(Color::DarkGray)),
            );
        }
        let weight_cell = if state.editing && selected_stats == Some(i) {
            format!("[{}▌]", state.edit_buf)
        } else {
            format!("{:.2}", s.weight)
        };
        let unif = s
            .uniform_weight
            .map_or("  -  ".to_string(), |w| format!("{:.2}", w));
        rows.push(Row::new(vec![
            Cell::from(format!("#{}", s.name)),
            Cell::from(format!("{:>5}", s.count)),
            Cell::from(format!("{:>7}", weight_cell)),
            Cell::from(format!("{:>5.1}%", s.curr_prob * 100.0)),
            Cell::from(format!("{:>5.1}%", s.whatif_prob * 100.0)),
            Cell::from(format!("{:>6}", unif)),
        ]));
    }

    let header = Row::new(vec![
        Cell::from("TAG").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("COUNT").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("WEIGHT").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("CURR%").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("BASE%").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("UNIF-W").style(Style::default().add_modifier(Modifier::BOLD)),
    ]);

    let widths = [
        Constraint::Min(16),
        Constraint::Length(6),
        Constraint::Length(9),
        Constraint::Length(7),
        Constraint::Length(7),
        Constraint::Length(8),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(" Tag Weights "))
        .row_highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("► ");

    f.render_stateful_widget(table, chunks[0], &mut state.table_state);

    let help = if state.editing {
        format!(" Editing — [Enter] confirm  [Esc] cancel  │  {}", state.status)
    } else {
        let dirty = if state.dirty { "● " } else { "" };
        format!(
            " {}[↑↓] navigate  [e] edit weight  [s] save  [q] quit  │  {}",
            dirty, state.status
        )
    };
    let para = Paragraph::new(help).block(Block::default().borders(Borders::TOP));
    f.render_widget(para, chunks[1]);
}

fn save_weights(conn: &Connection, stats: &[TagStat]) -> Result<()> {
    for s in stats {
        conn.execute(
            "UPDATE tags SET weight = ?1 WHERE id = ?2",
            params![s.weight, s.id],
        )?;
    }
    Ok(())
}

pub fn cmd_weights(conn: &Connection) -> Result<()> {
    let mut state = load_weights_state(conn)?;

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = (|| -> Result<()> {
        loop {
            terminal.draw(|f| render_weights(f, &mut state))?;

            if event::poll(std::time::Duration::from_millis(100))? {
                if let Event::Key(key) = event::read()? {
                    if state.editing {
                        match key.code {
                            KeyCode::Esc => {
                                state.editing = false;
                                state.edit_buf.clear();
                                state.status = "Cancelled.".to_string();
                            }
                            KeyCode::Enter => {
                                match state.edit_buf.trim().parse::<f64>() {
                                    Ok(w) if w >= 0.0 => {
                                        if let Some(idx) = state.selected_idx() {
                                            state.stats[idx].weight = w;
                                            state.recompute_probs();
                                            state.dirty = true;
                                            state.status = format!(
                                                "#{} → {:.2}",
                                                state.stats[idx].name, w
                                            );
                                        }
                                        state.editing = false;
                                        state.edit_buf.clear();
                                    }
                                    _ => {
                                        state.status = "Invalid weight (must be ≥ 0).".to_string();
                                    }
                                }
                            }
                            KeyCode::Backspace => { state.edit_buf.pop(); }
                            KeyCode::Char(c) if c.is_ascii_digit() || c == '.' => {
                                state.edit_buf.push(c);
                            }
                            _ => {}
                        }
                    } else {
                        match key.code {
                            KeyCode::Char('q') | KeyCode::Esc => break,
                            KeyCode::Char('s') => {
                                save_weights(conn, &state.stats)?;
                                state.dirty = false;
                                state.status = "Saved.".to_string();
                            }
                            KeyCode::Char('e') | KeyCode::Enter => {
                                if let Some(idx) = state.selected_idx() {
                                    state.edit_buf = format!("{:.2}", state.stats[idx].weight);
                                    state.editing = true;
                                    state.status.clear();
                                }
                            }
                            KeyCode::Down => {
                                let total = state.visual_row_count();
                                if total > 0 {
                                    let sep = state.separator_visual_idx();
                                    let next = state.table_state.selected().map_or(0, |v| {
                                        let mut n = (v + 1) % total;
                                        if sep == Some(n) { n = (n + 1) % total; }
                                        n
                                    });
                                    state.table_state.select(Some(next));
                                }
                            }
                            KeyCode::Up => {
                                let total = state.visual_row_count();
                                if total > 0 {
                                    let sep = state.separator_visual_idx();
                                    let prev = state.table_state.selected().map_or(0, |v| {
                                        let p = if v == 0 { total - 1 } else { v - 1 };
                                        if sep == Some(p) { if p == 0 { total - 1 } else { p - 1 } } else { p }
                                    });
                                    state.table_state.select(Some(prev));
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        Ok(())
    })();

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    if state.dirty {
        eprintln!("Unsaved changes discarded. Use [s] to save before quitting.");
    }

    result
}
