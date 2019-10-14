
from datetime import datetime
import pandas as pd
import sqlite3

from .dbconn import get_db_conn
from .dbschema import STOCK, HISTORY

def drop_tables():
    try:
        conn = get_db_conn()
        cursor = conn.cursor()
        cursor.execute("drop table if exists STOCK;")
        cursor.execute("drop table if exists HISTORY;")
        # cursor.close()
        conn.commit()
    except:
        conn.rollback()
        raise
####

def create_table():
    try:
        conn = get_db_conn()
        cursor = conn.cursor()

        cursor.execute(STOCK.to_sql_create())
        cursor.execute(HISTORY.to_sql_create())

        cursor.execute("create index SYMBOLINDEX on HISTORY(SYMBOL)")
        cursor.execute("create index TODAYINDEX on HISTORY(TODAY)")

        # cursor.close()
        conn.commit()
    except:
        conn.rollback()
        raise
####

################################################################################

def conv_null(val):
    if isinstance(val, str) and val.strip()=='--':
        # we need NULL value in this case
        return None
    else:
        return val
####

def conv_zh_num(x):
    '''return in 亿'''
    if isinstance(x, int) or isinstance(x, float):
        return float(x) / 1e8
    elif isinstance(x, str):
        if x[-1]=='万': return float(x[0:-1]) / 1e4
        if x[-1]=='亿': return float(x[0:-1])
    else:
        return x
####

def conv_trunc_last(x):
    if isinstance(x, str):
        return float(x[0:-1])
    else:
        return x
####

################################################################################

def read_excel_data(excelfilename, today=None):
    if not today:
        today = datetime.now().strftime("%Y-%m-%d")
    print(today)

    # we need skip the last row because it is '数据来源:通达信'
    df = pd.read_excel(excelfilename, skipfooter=1, dtype={'代码':str, '细分行业':str},
                       converters={'净流入':conv_zh_num, '大宗流入':conv_zh_num, '每股收益':conv_trunc_last})
    
    # add a today column
    df[HISTORY.TODAY.showname] = pd.Series([today]*len(df), index=df.index)
    return df
####

def sql_exec_many(sql, data):
    try:
        conn = get_db_conn()
        cursor = conn.cursor()
        cursor.executemany(sql, data)
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        # conn.close()
        pass
####

def extract_columns(df, keys):
    data = []
    for _, row in df.iterrows():
        if not row[STOCK.INDUSTRY.showname] or row[STOCK.INDUSTRY.showname]=='nan':
            print("will skip invalid row: " + str(row))
            continue
        values = tuple(conv_null(row[k]) for k in keys)
        data.append(values)
    return data
####

def insert_stock(df):
    keys = [c.showname for c in STOCK.columns()]
    # print(keys)

    data = extract_columns(df, keys)
    # print(len(data))
    
    insert_sql = f"insert or ignore into STOCK values ({','.join('?' for _ in keys)});"
    # print(insert_sql)
    
    sql_exec_many(insert_sql, data)
####

def delete_history(today):
    try:
        conn = get_db_conn()
        cursor = conn.cursor()
        cursor.execute("delete from HISTORY where TODAY=?", (today,))
        # cursor.close()
        conn.commit()
    except:
        conn.rollback()
        raise
####

def insert_history(df):
    # some columns may miss in input data, so need check if they are available
    keys = [c.showname for c in HISTORY.columns() if c.showname in df]
    cols = [c.dbname for c in HISTORY.columns() if c.showname in df]
    # print(keys)

    data = extract_columns(df, keys)
    print(len(data))
    
    # insert data
    insert_sql = f"insert into HISTORY ({','.join(cols)}) values ({','.join('?' for _ in keys)});"
    print(insert_sql)
    sql_exec_many(insert_sql, data)

####

# 代码	名称	现价	涨幅%	涨速%	卖价	流通股(亿)	市盈(动)	换手%	细分行业
# 相对流量%	大宗流量%	现量	开盘换手Z	连涨天	3日涨幅%	20日涨幅%	60日涨幅%	年初至今%	净流入	大宗流入	贝塔系数	每股收益	每股净资	每股公积	每股未分配	权益比%	净利润率%	研发费用(亿)	员工人数
def insert_data(excelfilename, today):
    df = read_excel_data(excelfilename, today)
    
    # add into stock list if not exist
    insert_stock(df)
    
    # we first delete all history date on this day
    delete_history(today)

    # then re-insert data
    insert_history(df)

    # derive other data
    # 成交量 = 流通股 * 换手
    # 成交量 = 流入 + 流出
    # 相对流量 = (流入 - 流出) / 成交量
    try:
        conn = get_db_conn()
        cursor = conn.cursor()
        cursor.execute("update HISTORY set MARKETCAPITAL = PRICE * CIRCULATESHARES where TODAY=?",(today,))
        cursor.execute("update HISTORY set VOLUME = CIRCULATESHARES * (TURNOVER/100) where TODAY=?",(today,))
        cursor.execute("update HISTORY set INFLOWSHARE = VOLUME * (1+RELATIVEFLOW/100) / 2 where TODAY=?",(today,))
        cursor.execute("update HISTORY set OUTFLOWSHARE = VOLUME * (1-RELATIVEFLOW/100) / 2 where TODAY=?",(today,))
        conn.commit()
    except:
        conn.rollback()
        raise
####
