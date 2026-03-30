from stockdb2 import dbstat

# Test with different consecutive days values
result1 = dbstat.query_stats({
    'positive_days': '3',
    'group_by': 'stock',
    'start_date': '2025-09-01',
    'end_date': '2025-09-15'
})

result2 = dbstat.query_stats({
    'positive_days': '5',
    'group_by': 'stock',
    'start_date': '2025-09-01',
    'end_date': '2025-09-15'
})

print('Test result:')
print(f'  With 3 consecutive days: {result1["count"]} stocks')
print(f'  With 5 consecutive days: {result2["count"]} stocks')

# Test with no positive days filter
result3 = dbstat.query_stats({
    'group_by': 'stock',
    'start_date': '2025-09-01',
    'end_date': '2025-09-15'
})
print(f'  With no filter: {result3["count"]} stocks')
