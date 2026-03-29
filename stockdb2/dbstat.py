"""Statistics and time-series queries for stockdb2."""

from collections import defaultdict
from typing import Dict, List, Optional

from .dbconn import get_connection
from .dbutils import parse_csv_values, rolling_stats

_ALLOWED_METRICS = {
    "net_inflow_100m",
    "relative_flow_pct",
    "large_flow_pct",
    "turnover_amount_100m",
    "inflow_amount_100m",
    "outflow_amount_100m",
    "trade_volume_100m",
    "large_inflow_100m",
}


DEFAULT_TS_METRICS = [
    "net_inflow_100m",
    "relative_flow_pct",
    "large_flow_pct",
    "turnover_amount_100m",
    "inflow_amount_100m",
    "outflow_amount_100m",
    "trade_volume_100m",
]


def get_meta() -> Dict[str, object]:
    """Return available symbols, industries, and date span."""
    conn = get_connection()
    try:
        stocks = [dict(row) for row in conn.execute("SELECT symbol, name, industry FROM stocks ORDER BY symbol")]
        industries = [
            row["industry"]
            for row in conn.execute(
                "SELECT DISTINCT industry FROM stocks WHERE industry IS NOT NULL AND industry <> '' ORDER BY industry"
            )
        ]
        date_row = conn.execute("SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM daily_metrics").fetchone()
        return {
            "stocks": stocks,
            "industries": industries,
            "date_range": {
                "start": date_row["min_date"] if date_row else None,
                "end": date_row["max_date"] if date_row else None,
            },
        }
    finally:
        conn.close()


def query_timeseries(args) -> Dict[str, object]:
    """Query filtered daily rows and optional rolling stats."""
    symbols = parse_csv_values(args.get("symbols"))
    industries = parse_csv_values(args.get("industries"))
    start_date = args.get("start_date")
    end_date = args.get("end_date")
    window = int(args.get("window", 1) or 1)

    metrics = parse_csv_values(args.get("metrics"))
    if not metrics:
        metrics = list(DEFAULT_TS_METRICS)
    metrics = [metric for metric in metrics if metric in _ALLOWED_METRICS]

    sql = """
        SELECT
            date, symbol, name, industry,
            net_inflow_100m, relative_flow_pct, large_flow_pct,
            turnover_amount_100m, inflow_amount_100m, outflow_amount_100m,
            trade_volume_100m, large_inflow_100m,
            free_float_market_cap_100m, free_float_shares_100m
        FROM daily_metrics
    """
    where_clauses = []
    params: List[object] = []

    if symbols:
        where_clauses.append("symbol IN ({})".format(",".join("?" for _ in symbols)))
        params.extend(symbols)

    if industries:
        where_clauses.append("industry IN ({})".format(",".join("?" for _ in industries)))
        params.extend(industries)

    if start_date:
        where_clauses.append("date >= ?")
        params.append(start_date)

    if end_date:
        where_clauses.append("date <= ?")
        params.append(end_date)

    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)

    sql += " ORDER BY symbol, date"

    conn = get_connection()
    try:
        rows = [dict(row) for row in conn.execute(sql, params)]
    finally:
        conn.close()

    if rows:
        rows = _attach_cumulative(rows, metrics)

    if window > 1 and rows:
        rows = _attach_rolling(rows, metrics, window)

    return {
        "metrics": metrics,
        "window": window,
        "rows": rows,
        "count": len(rows),
    }


def query_stats(args) -> Dict[str, object]:
    """Query aggregated cumulative stats by stock or industry with relative values."""
    symbols = parse_csv_values(args.get("symbols"))
    industries = parse_csv_values(args.get("industries"))
    start_date = args.get("start_date")
    end_date = args.get("end_date")
    group_by = args.get("group_by", "stock")

    if group_by not in ("stock", "industry"):
        group_by = "stock"

    dimension = "industry" if group_by == "industry" else "symbol"
    dimension_name = "industry" if group_by == "industry" else "name"

    # Query cumulative stats
    sql = f"""
        SELECT
            {dimension} AS group_key,
            {dimension_name} AS group_name,
            COUNT(*) AS days,
            SUM(net_inflow_100m) AS cum_net_inflow_100m,
            SUM(turnover_amount_100m) AS cum_turnover_amount_100m,
            SUM(trade_volume_100m) AS cum_trade_volume_100m
        FROM daily_metrics
    """
    where_clauses = []
    params: List[object] = []

    if symbols:
        where_clauses.append("symbol IN ({})".format(",".join("?" for _ in symbols)))
        params.extend(symbols)

    if industries:
        where_clauses.append("industry IN ({})".format(",".join("?" for _ in industries)))
        params.extend(industries)

    if start_date:
        where_clauses.append("date >= ?")
        params.append(start_date)

    if end_date:
        where_clauses.append("date <= ?")
        params.append(end_date)

    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)

    sql += f" GROUP BY {dimension} ORDER BY cum_net_inflow_100m DESC"

    conn = get_connection()
    try:
        rows = [dict(row) for row in conn.execute(sql, params)]
        
        # Get last date's data for each group to calculate relative values
        if rows:
            # Create a list of group keys
            group_keys = [row["group_key"] for row in rows]
            
            # Query last date for each group
            placeholder = ",".join(["?"] * len(group_keys))
            
            # Build dynamic WHERE clauses for date range
            date_conditions = []
            last_date_params = group_keys.copy()
            
            if start_date:
                date_conditions.append(f"date >= ?")
                last_date_params.append(start_date)
            
            if end_date:
                date_conditions.append(f"date <= ?")
                last_date_params.append(end_date)
            
            # Construct the full WHERE clause
            where_clause = f"{dimension} IN ({placeholder})"
            if date_conditions:
                where_clause += " AND " + " AND ".join(date_conditions)
            
            last_date_sql = f"""
                SELECT DISTINCT
                    {dimension} AS group_key,
                    date,
                    free_float_market_cap_100m,
                    free_float_shares_100m
                FROM daily_metrics
                WHERE {where_clause}
                ORDER BY {dimension}, date DESC
            """
            
            last_date_rows = list(conn.execute(last_date_sql, last_date_params))
            
            # Create a dictionary to map group_key to last date's data
            last_date_data = {}
            current_group = None
            for row in last_date_rows:
                row_dict = dict(row)
                if row_dict["group_key"] != current_group:
                    last_date_data[row_dict["group_key"]] = row_dict
                    current_group = row_dict["group_key"]
            
            # Calculate relative values for each row
            for row in rows:
                group_key = row["group_key"]
                last_data = last_date_data.get(group_key, {})
                
                # Get last date's values
                last_market_cap = last_data.get("free_float_market_cap_100m")
                last_shares = last_data.get("free_float_shares_100m")
                
                # Get cumulative values
                cum_net_inflow = row.get("cum_net_inflow_100m")
                cum_turnover = row.get("cum_turnover_amount_100m")
                cum_volume = row.get("cum_trade_volume_100m")
                
                # Calculate relative values with proper None and zero checks
                if (last_market_cap is not None and last_market_cap > 0 and 
                    cum_net_inflow is not None):
                    row["cum_net_inflow_rel"] = cum_net_inflow / last_market_cap * 100
                else:
                    row["cum_net_inflow_rel"] = None
                
                if (last_market_cap is not None and last_market_cap > 0 and 
                    cum_turnover is not None):
                    row["cum_turnover_rel"] = cum_turnover / last_market_cap * 100
                else:
                    row["cum_turnover_rel"] = None
                
                if (last_shares is not None and last_shares > 0 and 
                    cum_volume is not None):
                    row["cum_volume_rel"] = cum_volume / last_shares * 100
                else:
                    row["cum_volume_rel"] = None
                
    finally:
        conn.close()

    return {
        "group_by": group_by,
        "rows": rows,
        "count": len(rows),
    }


def _attach_rolling(rows: List[Dict[str, object]], metrics: List[str], window: int) -> List[Dict[str, object]]:
    grouped: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for row in rows:
        symbol = str(row.get("symbol", ""))
        grouped[symbol].append(row)

    updated_rows: List[Dict[str, object]] = []
    for symbol, symbol_rows in grouped.items():
        for metric in metrics:
            series: List[Optional[float]] = []
            for item in symbol_rows:
                value = item.get(metric)
                if isinstance(value, (int, float)):
                    series.append(float(value))
                else:
                    series.append(None)
            rolls = rolling_stats(series, window)
            for idx, roll in enumerate(rolls):
                symbol_rows[idx][f"{metric}_roll_avg"] = roll["avg"]
                symbol_rows[idx][f"{metric}_roll_sum"] = roll["sum"]
        updated_rows.extend(symbol_rows)

    updated_rows.sort(key=lambda item: (item["symbol"], item["date"]))
    return updated_rows


def _attach_cumulative(rows: List[Dict[str, object]], metrics: List[str]) -> List[Dict[str, object]]:
    grouped: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for row in rows:
        symbol = str(row.get("symbol", ""))
        grouped[symbol].append(row)

    updated_rows: List[Dict[str, object]] = []
    for symbol, symbol_rows in grouped.items():
        for metric in metrics:
            running_sum = 0.0
            has_value = False
            for item in symbol_rows:
                value = item.get(metric)
                if isinstance(value, (int, float)):
                    running_sum += float(value)
                    has_value = True
                    item[f"{metric}_cum"] = running_sum
                else:
                    item[f"{metric}_cum"] = running_sum if has_value else None
        updated_rows.extend(symbol_rows)

    updated_rows.sort(key=lambda item: (item["symbol"], item["date"]))
    return updated_rows
