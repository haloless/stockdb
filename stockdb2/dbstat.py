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
    
    # New filter parameters
    sort_order = args.get("sort_order")
    limit_count = args.get("limit_count")
    limit_count = int(limit_count) if limit_count and limit_count.isdigit() else None
    positive_days = args.get("positive_days")
    positive_days = int(positive_days) if positive_days and positive_days.isdigit() else None
    
    # Chart parameters to determine correct sorting metric
    use_cumulative = args.get("use_cumulative", "false").lower() == "true"
    use_relative = args.get("use_relative", "false").lower() == "true"

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
    
    # Apply new filters if specified
    filtered_symbols = None
    
    # Group rows by symbol
    rows_by_symbol = defaultdict(list)
    for row in rows:
        symbol = row["symbol"]
        rows_by_symbol[symbol].append(row)
    
    # 1. Filter symbols with positive net inflow for N consecutive days ending at end_date
    if positive_days:
        valid_symbols = set()
        
        # For each symbol, check if it has N consecutive positive days ending at end_date
        for symbol, symbol_rows in rows_by_symbol.items():
            # Sort rows by date descending
            sorted_rows = sorted(symbol_rows, key=lambda x: x["date"], reverse=True)
            
            # Find the end_date row or the latest date
            target_date = end_date or sorted_rows[0]["date"] if sorted_rows else None
            
            if not target_date:
                continue
            
            # Collect consecutive days ending at target_date
            consecutive_positive = 0
            for row in sorted_rows:
                if row["date"] > target_date:
                    continue
                    
                if row["net_inflow_100m"] is not None and row["net_inflow_100m"] > 0:
                    consecutive_positive += 1
                else:
                    break
                    
                if consecutive_positive >= positive_days:
                    break
            
            if consecutive_positive >= positive_days:
                valid_symbols.add(symbol)
        
        filtered_symbols = valid_symbols
    
    # 2. Sort symbols based on end_date data and limit count
    if sort_order and limit_count:
        # Create a list of symbols with their end_date net_inflow_100m
        symbol_end_values = []
        
        for symbol, symbol_rows in rows_by_symbol.items():
            # Check if symbol is already filtered out
            if filtered_symbols and symbol not in filtered_symbols:
                continue
                
            # Sort rows by date descending
            sorted_rows = sorted(symbol_rows, key=lambda x: x["date"], reverse=True)
            
            # Find the end_date row or the latest date
            target_date = end_date or sorted_rows[0]["date"] if sorted_rows else None
            
            if not target_date:
                continue
            
            # Find the row for the target date
            end_row = None
            for row in sorted_rows:
                if row["date"] == target_date:
                    end_row = row
                    break
            
            if end_row:
                # Determine the correct metric key based on chart parameters
                if use_cumulative:
                    metric_key = "net_inflow_100m_cum"
                elif window > 1:
                    metric_key = "net_inflow_100m_roll_avg"
                else:
                    metric_key = "net_inflow_100m"
                    
                # Get the value using the determined metric key
                metric_value = end_row.get(metric_key)
                
                if metric_value is not None:
                    # Apply relative value calculation if requested
                    if use_relative:
                        # Use the same logic as frontend for relative value calculation
                        free_float_market_cap = end_row.get("free_float_market_cap_100m")
                        if free_float_market_cap is not None and free_float_market_cap > 0:
                            # For net_inflow metrics, normalize by free float market cap
                            metric_value = (metric_value / free_float_market_cap) * 100
                        else:
                            # Skip if cannot calculate relative value
                            continue
                    
                    symbol_end_values.append((symbol, metric_value))
        
        # Sort symbols based on the selected metric and order
        if sort_order == "asc":
            symbol_end_values.sort(key=lambda x: x[1])
        else:  # desc
            symbol_end_values.sort(key=lambda x: x[1], reverse=True)

        # Get top N symbols as a list to preserve order
        top_symbols_list = [symbol for symbol, _ in symbol_end_values[:limit_count]]
        top_symbols = set(top_symbols_list)
        
        # Apply AND logic with previous filters
        if filtered_symbols:
            # Get the intersection while preserving order
            filtered_symbols_list = [symbol for symbol in top_symbols_list if symbol in filtered_symbols]
            filtered_symbols = set(filtered_symbols_list)
        else:
            filtered_symbols = top_symbols
            filtered_symbols_list = top_symbols_list
    
    # Apply filtered symbols to rows if any filters were applied
    if filtered_symbols:
        rows = [row for row in rows if row["symbol"] in filtered_symbols]
        
        # If sorting was applied, sort the rows by symbol in the sorted order
        if sort_order and limit_count:
            # Use the filtered_symbols_list which maintains the sorted order but only includes symbols that passed all filters
            symbol_order_list = filtered_symbols_list
            
            # Create a symbol-to-index mapping for quick lookup
            symbol_order = {symbol: index for index, symbol in enumerate(symbol_order_list)}
            
            # Sort rows by the symbol order, then by date
            rows.sort(key=lambda x: (symbol_order.get(x["symbol"], float('inf')), x["date"]))

    return {
        "metrics": metrics,
        "window": window,
        "rows": rows,
        "count": len(rows),
        "filtered_symbols_count": len(filtered_symbols) if filtered_symbols else len(rows_by_symbol),
    }


def query_stats(args) -> Dict[str, object]:
    """Query aggregated cumulative stats by stock or industry with relative values."""
    symbols = parse_csv_values(args.get("symbols"))
    industries = parse_csv_values(args.get("industries"))
    start_date = args.get("start_date")
    end_date = args.get("end_date")
    group_by = args.get("group_by", "stock")
    
    # Handle positive days filter parameter
    positive_days = args.get("positive_days")
    positive_days = int(positive_days) if positive_days and positive_days.isdigit() else None

    if group_by not in ("stock", "industry"):
        group_by = "stock"

    dimension = "industry" if group_by == "industry" else "symbol"
    dimension_name = "industry" if group_by == "industry" else "name"
    
    # Handle positive days filter if specified
    filtered_symbols = None
    
    if positive_days and group_by == "stock":
        # First, get all symbols that have N consecutive positive days ending at end_date
        conn = get_connection()
        try:
            # Query all symbols and their daily data to check consecutive days
            daily_sql = """
                SELECT symbol, date, net_inflow_100m
                FROM daily_metrics
            """
            
            where_clauses = []
            daily_params = []
            
            if symbols:
                where_clauses.append(f"symbol IN ({','.join(['?'] * len(symbols))})")
                daily_params.extend(symbols)
            
            if industries:
                where_clauses.append(f"industry IN ({','.join(['?'] * len(industries))})")
                daily_params.extend(industries)
            
            if start_date:
                where_clauses.append("date >= ?")
                daily_params.append(start_date)
            
            if end_date:
                where_clauses.append("date <= ?")
                daily_params.append(end_date)
            
            if where_clauses:
                daily_sql += " WHERE " + " AND ".join(where_clauses)
            
            daily_sql += " ORDER BY symbol, date DESC"
            
            daily_rows = [dict(row) for row in conn.execute(daily_sql, daily_params)]
            
            # Group daily rows by symbol
            daily_rows_by_symbol = defaultdict(list)
            for row in daily_rows:
                daily_rows_by_symbol[row["symbol"]].append(row)
            
            # Filter symbols with N consecutive positive days ending at end_date
            valid_symbols = set()
            for symbol, symbol_rows in daily_rows_by_symbol.items():
                if not symbol_rows:
                    continue
                    
                # Find the target date (end_date or latest date)
                target_date = end_date or symbol_rows[0]["date"]
                
                consecutive_positive = 0
                
                for row in symbol_rows:
                    if row["date"] > target_date:
                        continue
                    
                    if row["net_inflow_100m"] is not None and row["net_inflow_100m"] > 0:
                        consecutive_positive += 1
                    else:
                        break
                    
                    if consecutive_positive >= positive_days:
                        valid_symbols.add(symbol)
                        break
            
            if valid_symbols:
                filtered_symbols = valid_symbols
            else:
                # No symbols meet the criteria, return empty result
                return {
                    "group_by": group_by,
                    "rows": [],
                    "count": 0
                }
        finally:
            conn.close()

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
    params = []

    # Apply filtered symbols if we have them from the positive days check
    if filtered_symbols:
        where_clauses.append(f"symbol IN ({','.join(['?'] * len(filtered_symbols))})")
        params.extend(filtered_symbols)
    elif symbols:
        where_clauses.append(f"symbol IN ({','.join(['?'] * len(symbols))})")
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
