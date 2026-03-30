from stockdb2 import dbstat

test_cases = [
    ("Raw data", {'use_cumulative': 'false', 'use_relative': 'false'}),
    ("Cumulative data", {'use_cumulative': 'true', 'use_relative': 'false'}),
    ("Relative data (raw)", {'use_cumulative': 'false', 'use_relative': 'true'}),
    ("Cumulative + Relative data", {'use_cumulative': 'true', 'use_relative': 'true'})
]

for test_name, params in test_cases:
    print(f"\n=== {test_name} ===")
    
    result = dbstat.query_timeseries({
        'sort_order': 'desc',
        'limit_count': '5',
        'window': '1',
        **params
    })
    
    print("Top 3 symbols:")
    seen_symbols = set()
    for row in result['rows'][:10]:
        symbol = row['symbol']
        if symbol not in seen_symbols:
            # Determine which metric was used for sorting
            if params['use_cumulative'] == 'true':
                metric_value = row.get('net_inflow_100m_cum')
                metric_name = 'Cumulative'
            else:
                metric_value = row.get('net_inflow_100m')
                metric_name = 'Raw'
                
            if params['use_relative'] == 'true' and metric_value is not None:
                # Calculate what the relative value would be to verify
                market_cap = row.get('free_float_market_cap_100m')
                if market_cap and market_cap > 0:
                    relative_value = (metric_value / market_cap) * 100
                    print(f"{symbol} - {row['name']}: {relative_value:.4f}% (based on {metric_name})")
                else:
                    print(f"{symbol} - {row['name']}: [Relative value calculation not possible]")
            else:
                print(f"{symbol} - {row['name']}: {metric_value}")
                
            seen_symbols.add(symbol)
            if len(seen_symbols) >= 3:
                break
