"""Database connection and schema initialization for stockdb2."""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "stock2.db"


def get_connection() -> sqlite3.Connection:
    """Return a SQLite connection with Row factory enabled."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create required tables and indexes if they do not exist."""
    conn = get_connection()
    try:
        conn.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS stocks (
                symbol TEXT PRIMARY KEY,
                name TEXT,
                industry TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS daily_metrics (
                date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                name TEXT,
                industry TEXT,
                price REAL,
                turnover_pct REAL,
                current_volume REAL,
                net_inflow_100m REAL,
                relative_flow_pct REAL,
                large_flow_pct REAL,
                free_float_market_cap_100m REAL,
                free_float_shares_100m REAL,
                change_pct REAL,
                speed_pct REAL,
                volume_ratio REAL,
                streak_days INTEGER,
                turnover_amount_100m REAL,
                inflow_amount_100m REAL,
                outflow_amount_100m REAL,
                trade_volume_100m REAL,
                inflow_volume_100m REAL,
                outflow_volume_100m REAL,
                large_inflow_100m REAL,
                source_filename TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (date, symbol),
                FOREIGN KEY (symbol) REFERENCES stocks(symbol)
            );

            CREATE INDEX IF NOT EXISTS idx_daily_metrics_date
                ON daily_metrics(date);

            CREATE INDEX IF NOT EXISTS idx_daily_metrics_symbol
                ON daily_metrics(symbol);

            CREATE INDEX IF NOT EXISTS idx_daily_metrics_industry
                ON daily_metrics(industry);

            CREATE INDEX IF NOT EXISTS idx_daily_metrics_symbol_date
                ON daily_metrics(symbol, date);
            """
        )
        conn.commit()
    finally:
        conn.close()
