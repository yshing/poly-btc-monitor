use std::collections::HashMap;

use anyhow::Result;
use rusqlite::{params, Connection};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct TargetWallet {
    pub wallet: String,
    pub rank: i32,
    pub confidence: String,
    pub expected_win_rate: f64,
    pub avg_roi: f64,
    pub total_pnl: f64,
    pub recommended_follow_size: f64,
}

#[derive(Debug, Clone)]
pub struct Market {
    pub slug: String,
    pub condition_id: String,
    pub outcome_one: String,
    pub outcome_two: String,
    pub token_one: String,
    pub token_two: String,
    pub winning_outcome: Option<String>,
}

pub struct Db {
    conn: Connection,
}

impl Db {
    pub fn new(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        Ok(Self { conn })
    }

    pub fn load_targets(&self, min_confidence_rank: u8, top_n: usize) -> Result<Vec<TargetWallet>> {
        let mut stmt = self.conn.prepare(
            "SELECT wallet, rank, confidence, expected_win_rate, avg_roi, total_pnl, recommended_follow_size \
             FROM copy_trade_signals \
             ORDER BY rank ASC"
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(TargetWallet {
                wallet: row.get(0)?,
                rank: row.get(1)?,
                confidence: row.get(2)?,
                expected_win_rate: row.get(3)?,
                avg_roi: row.get(4)?,
                total_pnl: row.get(5)?,
                recommended_follow_size: row.get(6)?,
            })
        })?;

        let mut wallets = Vec::new();
        for row in rows {
            wallets.push(row?);
        }

        let rank_map = |c: &str| match c.to_uppercase().as_str() {
            "LOW" => 1,
            "MEDIUM" => 2,
            "HIGH" => 3,
            _ => 1,
        };

        wallets.retain(|w| rank_map(&w.confidence) >= min_confidence_rank);
        wallets.truncate(top_n);
        Ok(wallets)
    }

    pub fn load_markets(&self) -> Result<HashMap<String, Market>> {
        let mut stmt = self.conn.prepare(
            "SELECT slug, condition_id, outcome_one, outcome_two, token_one, token_two, winning_outcome \
             FROM markets"
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(Market {
                slug: row.get(0)?,
                condition_id: row.get(1)?,
                outcome_one: row.get(2)?,
                outcome_two: row.get(3)?,
                token_one: row.get(4)?,
                token_two: row.get(5)?,
                winning_outcome: row.get(6)?,
            })
        })?;

        let mut map = HashMap::new();
        for row in rows {
            let m = row?;
            map.insert(m.token_one.clone(), m.clone());
            map.insert(m.token_two.clone(), m);
        }
        Ok(map)
    }

    pub fn get_state(&self, key: &str) -> Result<Option<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT value FROM pipeline_state WHERE key = ?1")?;
        let mut rows = stmt.query(params![key])?;
        if let Some(row) = rows.next()? {
            Ok(Some(row.get(0)?))
        } else {
            Ok(None)
        }
    }

    pub fn set_state(&self, key: &str, value: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO pipeline_state (key, value) VALUES (?1, ?2) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }
}
