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
    pub fn selected_idx(&self) -> Option<usize> {
        self.table_state.selected()
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

    let selected = state.table_state.selected();

    let rows: Vec<Row> = state
        .stats
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let weight_cell = if state.editing && selected == Some(i) {
                format!("[{}▌]", state.edit_buf)
            } else {
                format!("{:.2}", s.weight)
            };
            let unif = s
                .uniform_weight
                .map_or("  -  ".to_string(), |w| format!("{:.2}", w));
            Row::new(vec![
                Cell::from(format!("#{}", s.name)),
                Cell::from(format!("{:>5}", s.count)),
                Cell::from(format!("{:>7}", weight_cell)),
                Cell::from(format!("{:>5.1}%", s.curr_prob * 100.0)),
                Cell::from(format!("{:>5.1}%", s.whatif_prob * 100.0)),
                Cell::from(format!("{:>6}", unif)),
            ])
        })
        .collect();

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
                                let n = state.stats.len();
                                if n > 0 {
                                    let next = state.selected_idx().map(|i| (i + 1) % n).unwrap_or(0);
                                    state.table_state.select(Some(next));
                                }
                            }
                            KeyCode::Up => {
                                let n = state.stats.len();
                                if n > 0 {
                                    let prev = state
                                        .selected_idx()
                                        .map(|i| if i == 0 { n - 1 } else { i - 1 })
                                        .unwrap_or(0);
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
