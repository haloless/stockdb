from stockdb2 import dbstat

# Test with different chart parameters
print("Test 1: Raw data (no cumulative, window=1)")
result1 = dbstat.query_timeseries({
    'sort_order': 'desc',
    'limit_count': '5',
    'use_cumulative': 'false',
    'window': '1'
})

print("Top 3 symbols:")
seen_symbols1 = set()
for row in result1['rows'][:10]:
    symbol = row['symbol']
    if symbol not in seen_symbols1:
        print(f"{symbol} - {row['name']}: {row['net_inflow_100m']}")
        seen_symbols1.add(symbol)
        if len(seen_symbols1) >= 3:
            break

print("\nTest 2: Cumulative data")
result2 = dbstat.query_timeseries({
    'sort_order': 'desc',
    'limit_count': '5',
    'use_cumulative': 'true',
    'window': '1'
})

print("Top 3 symbols:")
seen_symbols2 = set()
for row in result2['rows'][:10]:
    symbol = row['symbol']
    if symbol not in seen_symbols2:
        print(f"{symbol} - {row['name']}: {row['net_inflow_100m_cum']}")
        seen_symbols2.add(symbol)
        if len(seen_symbols2) >= 3:
            break

print("\nTest 3: Rolling average (window=3)")
result3 = dbstat.query_timeseries({
    'sort_order': 'desc',
    'limit_count': '5',
    'use_cumulative': 'false',
    'window': '3'
})

print("Top 3 symbols:")
seen_symbols3 = set()
for row in result3['rows'][-10:]:  # Check last 10 rows for window data
    symbol = row['symbol']
    if symbol not in seen_symbols3:
        if 'net_inflow_100m_roll_avg' in row:
            print(f"{symbol} - {row['name']}: {row['net_inflow_100m_roll_avg']}")
        seen_symbols3.add(symbol)
        if len(seen_symbols3) >= 3:
            break
