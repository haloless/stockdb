"""Data import pipeline for stockdb2."""

from typing import Dict, List, Tuple

from .dbconn import get_connection
from .dbutils import (
    clean_symbol,
    infer_date_from_filename,
    to_amount_100m,
    to_float,
    to_net_inflow_100m,
    to_nullable_text,
    parse_table_from_bytes,
)


def import_uploaded_files(files) -> Dict[str, object]:
    """Import one or more uploaded files into SQLite."""
    summaries: List[Dict[str, object]] = []
    total_inserted = 0
    total_errors = 0

    for uploaded in files:
        file_summary = {
            "filename": uploaded.filename,
            "date": None,
            "inserted": 0,
            "skipped": 0,
            "errors": [],
        }
        try:
            target_date = infer_date_from_filename(uploaded.filename)
            file_summary["date"] = target_date
            file_bytes = uploaded.read()
            rows = parse_table_from_bytes(file_bytes)
            inserted, skipped, errors = _upsert_rows(rows, target_date, uploaded.filename)
            file_summary["inserted"] = inserted
            file_summary["skipped"] = skipped
            file_summary["errors"] = errors
            total_inserted += inserted
            total_errors += len(errors)
        except Exception as exc:  # broad by design for upload summary robustness
            file_summary["errors"].append(str(exc))
            total_errors += 1
        summaries.append(file_summary)

    return {
        "ok": total_errors == 0,
        "total_files": len(files),
        "total_inserted": total_inserted,
        "total_errors": total_errors,
        "files": summaries,
    }


def _upsert_rows(rows: List[Dict[str, object]], target_date: str, source_filename: str) -> Tuple[int, int, List[str]]:
    conn = get_connection()
    inserted = 0
    skipped = 0
    errors: List[str] = []

    try:
        for idx, row in enumerate(rows, start=1):
            symbol = clean_symbol(row.get("代码"))
            if not symbol:
                skipped += 1
                continue

            normalized = _normalize_row(row, target_date, source_filename)
            if normalized is None:
                skipped += 1
                continue

            normalized["symbol"] = symbol

            try:
                _upsert_stock(conn, normalized)
                _upsert_daily(conn, normalized)
                inserted += 1
            except Exception as exc:  # pragma: no cover - runtime db safety
                errors.append(f"第{idx}行({symbol})入库失败: {exc}")

        conn.commit()
    finally:
        conn.close()

    return inserted, skipped, errors


def _normalize_row(row: Dict[str, object], target_date: str, source_filename: str) -> Dict[str, object]:
    name = to_nullable_text(row.get("名称"))
    industry = to_nullable_text(row.get("细分行业")) or "ETF"

    net_inflow_100m = to_net_inflow_100m(row.get("主力净额"))
    relative_flow_pct = to_float(row.get("主力占比%"))
    large_flow_pct = to_float(row.get("攻击波%"))
    price = to_float(row.get("现价"))

    turnover_amount_100m = _calc_turnover_amount_100m(net_inflow_100m, relative_flow_pct)
    inflow_amount_100m = _calc_half_sum(turnover_amount_100m, net_inflow_100m, True)
    outflow_amount_100m = _calc_half_sum(turnover_amount_100m, net_inflow_100m, False)

    trade_volume_100m = _safe_div(turnover_amount_100m, price)
    inflow_volume_100m = _safe_div(inflow_amount_100m, price)
    outflow_volume_100m = _safe_div(outflow_amount_100m, price)
    large_inflow_100m = _calc_percent_amount(turnover_amount_100m, large_flow_pct)

    return {
        "date": target_date,
        "name": name,
        "industry": industry,
        "price": price,
        "turnover_pct": to_float(row.get("换手%")),
        "current_volume": to_float(row.get("现量")),
        "net_inflow_100m": net_inflow_100m,
        "relative_flow_pct": relative_flow_pct,
        "large_flow_pct": large_flow_pct,
        "free_float_market_cap_100m": to_amount_100m(row.get("流通市值")),
        "free_float_shares_100m": to_amount_100m(row.get("流通股(亿)")) or to_float(row.get("流通股(亿)")),
        "change_pct": to_float(row.get("涨幅%")),
        "speed_pct": to_float(row.get("涨速%")),
        "volume_ratio": to_float(row.get("量比")),
        "streak_days": _to_int(row.get("连涨天")),
        "turnover_amount_100m": turnover_amount_100m,
        "inflow_amount_100m": inflow_amount_100m,
        "outflow_amount_100m": outflow_amount_100m,
        "trade_volume_100m": trade_volume_100m,
        "inflow_volume_100m": inflow_volume_100m,
        "outflow_volume_100m": outflow_volume_100m,
        "large_inflow_100m": large_inflow_100m,
        "source_filename": source_filename,
    }


def _upsert_stock(conn, row: Dict[str, object]) -> None:
    conn.execute(
        """
        INSERT INTO stocks (symbol, name, industry, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(symbol) DO UPDATE SET
            name=excluded.name,
            industry=excluded.industry,
            updated_at=CURRENT_TIMESTAMP
        """,
        (row["symbol"], row["name"], row["industry"]),
    )


def _upsert_daily(conn, row: Dict[str, object]) -> None:
    conn.execute(
        """
        INSERT INTO daily_metrics (
            date, symbol, name, industry,
            price, turnover_pct, current_volume,
            net_inflow_100m, relative_flow_pct, large_flow_pct,
            free_float_market_cap_100m, free_float_shares_100m,
            change_pct, speed_pct, volume_ratio, streak_days,
            turnover_amount_100m, inflow_amount_100m, outflow_amount_100m,
            trade_volume_100m, inflow_volume_100m, outflow_volume_100m,
            large_inflow_100m, source_filename, updated_at
        ) VALUES (
            :date, :symbol, :name, :industry,
            :price, :turnover_pct, :current_volume,
            :net_inflow_100m, :relative_flow_pct, :large_flow_pct,
            :free_float_market_cap_100m, :free_float_shares_100m,
            :change_pct, :speed_pct, :volume_ratio, :streak_days,
            :turnover_amount_100m, :inflow_amount_100m, :outflow_amount_100m,
            :trade_volume_100m, :inflow_volume_100m, :outflow_volume_100m,
            :large_inflow_100m, :source_filename, CURRENT_TIMESTAMP
        )
        ON CONFLICT(date, symbol) DO UPDATE SET
            name=excluded.name,
            industry=excluded.industry,
            price=excluded.price,
            turnover_pct=excluded.turnover_pct,
            current_volume=excluded.current_volume,
            net_inflow_100m=excluded.net_inflow_100m,
            relative_flow_pct=excluded.relative_flow_pct,
            large_flow_pct=excluded.large_flow_pct,
            free_float_market_cap_100m=excluded.free_float_market_cap_100m,
            free_float_shares_100m=excluded.free_float_shares_100m,
            change_pct=excluded.change_pct,
            speed_pct=excluded.speed_pct,
            volume_ratio=excluded.volume_ratio,
            streak_days=excluded.streak_days,
            turnover_amount_100m=excluded.turnover_amount_100m,
            inflow_amount_100m=excluded.inflow_amount_100m,
            outflow_amount_100m=excluded.outflow_amount_100m,
            trade_volume_100m=excluded.trade_volume_100m,
            inflow_volume_100m=excluded.inflow_volume_100m,
            outflow_volume_100m=excluded.outflow_volume_100m,
            large_inflow_100m=excluded.large_inflow_100m,
            source_filename=excluded.source_filename,
            updated_at=CURRENT_TIMESTAMP
        """,
        row,
    )


def _calc_turnover_amount_100m(net_inflow_100m, relative_flow_pct):
    if net_inflow_100m is None or relative_flow_pct is None or relative_flow_pct == 0:
        return None
    return net_inflow_100m / (relative_flow_pct / 100.0)


def _calc_half_sum(turnover_amount_100m, net_inflow_100m, is_inflow):
    if turnover_amount_100m is None or net_inflow_100m is None:
        return None
    if is_inflow:
        return (turnover_amount_100m + net_inflow_100m) / 2.0
    return (turnover_amount_100m - net_inflow_100m) / 2.0


def _calc_percent_amount(amount_100m, pct):
    if amount_100m is None or pct is None:
        return None
    return amount_100m * pct / 100.0


def _safe_div(numerator, denominator):
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def _to_int(raw_value):
    value = to_float(raw_value)
    if value is None:
        return None
    return int(value)
