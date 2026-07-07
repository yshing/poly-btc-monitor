"""Flask web dashboard for Polymarket BTC Up/Down 5m analysis.

Run with:
    source .venv/bin/activate && python -m src.web.app
"""

import os
import sqlite3
from dataclasses import dataclass
from typing import Any

from flask import Flask, render_template

from src import db

app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), "templates"),
    static_folder=os.path.join(os.path.dirname(__file__), "static"),
)


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {k: row[k] for k in row.keys()}


@app.route("/")
def index():
    with db.get_conn() as conn:
        summary = {
            "markets": conn.execute("SELECT COUNT(*) AS c FROM markets").fetchone()["c"],
            "resolved": conn.execute(
                "SELECT COUNT(*) AS c FROM markets WHERE resolved=1"
            ).fetchone()["c"],
            "trades": conn.execute("SELECT COUNT(*) AS c FROM trades").fetchone()["c"],
            "wallets": conn.execute(
                "SELECT COUNT(*) AS c FROM wallet_stats"
            ).fetchone()["c"],
        }
        top_winners = [
            _row_to_dict(r)
            for r in conn.execute(
                """
                SELECT * FROM wallet_stats
                WHERE total_invested > 0
                ORDER BY total_pnl DESC
                LIMIT 10
                """
            ).fetchall()
        ]
        top_losers = [
            _row_to_dict(r)
            for r in conn.execute(
                """
                SELECT * FROM wallet_stats
                WHERE total_invested > 0 AND markets_traded >= 10
                ORDER BY total_pnl ASC
                LIMIT 10
                """
            ).fetchall()
        ]
    return render_template(
        "index.html", summary=summary, winners=top_winners, losers=top_losers
    )


@app.route("/plan")
def plan():
    with db.get_conn() as conn:
        signals = [
            _row_to_dict(r)
            for r in conn.execute(
                """
                SELECT * FROM copy_trade_signals
                ORDER BY rank ASC
                """
            ).fetchall()
        ]
    return render_template("plan.html", signals=signals)


@app.route("/wallet/<address>")
def wallet(address: str):
    addr = address.lower()
    with db.get_conn() as conn:
        stats_row = conn.execute(
            "SELECT * FROM wallet_stats WHERE wallet = ?", (addr,)
        ).fetchone()
        if stats_row is None:
            return render_template("wallet.html", wallet=address, stats=None, trades=[])
        stats = _row_to_dict(stats_row)
        trades = [
            _row_to_dict(r)
            for r in conn.execute(
                """
                SELECT t.*, m.outcome_one, m.outcome_two, m.winning_outcome
                FROM trades t
                JOIN markets m ON t.market_slug = m.slug
                WHERE t.proxy_wallet = ?
                ORDER BY t.timestamp DESC
                LIMIT 200
                """,
                (addr,),
            ).fetchall()
        ]
    return render_template("wallet.html", wallet=address, stats=stats, trades=trades)


@app.route("/wallets")
def wallets():
    page = 0
    page_size = 100
    sort = "total_pnl"
    order = "DESC"
    with db.get_conn() as conn:
        rows = [
            _row_to_dict(r)
            for r in conn.execute(
                f"""
                SELECT * FROM wallet_stats
                WHERE total_invested > 0
                ORDER BY {sort} {order}
                LIMIT ? OFFSET ?
                """,
                (page_size, page * page_size),
            ).fetchall()
        ]
        total = conn.execute(
            "SELECT COUNT(*) AS c FROM wallet_stats WHERE total_invested > 0"
        ).fetchone()["c"]
    return render_template(
        "wallets.html", wallets=rows, page=page, page_size=page_size, total=total
    )


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
