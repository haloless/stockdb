
import json

import pandas as pd
# import sqlite3

from .dbschema import Field, STOCK, HISTORY
from .dbconn import get_db_conn
from .dbutils import rename_df_cols



def get_grouped_df(begin_date, end_date, fields, symbols=None, industry=None, stats="avg", rename=True):
    '''
fields
'''
    conn = get_db_conn()

    #
    sql_fields = []
    for f in fields:
        s = stats
        if isinstance(f, Field):
            sql_fields.append(f"{s}({f}) as {s}_{f}")
        elif isinstance(f, str):
            sql_fields.append(f"{s}({f}) as {s}_{f}")
    sql_columns = ",".join(sql_fields)

    sql_where_conds = []
    # date range by date
    if begin_date:
        sql_where_conds.append(f"TODAY>='{begin_date}'")
    if end_date:
        sql_where_conds.append(f"TODAY<='{end_date}'")
    # filter by symbol
    if symbols:
        sql_where_conds.append("SYMBOL in (" + ",".join(f"'{s}'" for s in symbols) + ")")
    # filter by industry
    if industry:
        sql_where_conds.append(f"INDUSTRY='{industry}'")

    # build where clause
    if sql_where_conds:
        sql_where = "where " + " and ".join(sql_where_conds)
    else:
        sql_where = ""

    #
    sql_orderby = ""
    # sql_orderby = f"order by {stats}_{HISTORY.BULKINFLOW} desc"

    sql = f"""select
{HISTORY.SYMBOL}, {HISTORY.LOCALNAME}, {sql_columns}
from HISTORY
{sql_where}
group by {HISTORY.SYMBOL}
{sql_orderby}
"""
    print(sql)

    df = pd.read_sql_query(sql, conn)

    if rename:
        newcols = {}
        for f in fields:
            s = stats
            newcols[f"{s}_{f}"] = f.showname
        df = df.rename(columns=newcols)

    return df
####


def main():


    begin_date = "1970-01-01"
    end_date = "2050-01-01"
    fields = [HISTORY.CIRCULATESHARES,
        HISTORY.TURNOVER,
        HISTORY.RELATIVEFLOW,
        HISTORY.NETINFLOW,
        HISTORY.BULKINFLOW,]

    df = get_grouped_df(begin_date=begin_date, end_date=end_date, fields=fields)

    # print(df)
    js = df.to_json(orient="table")

    with open("./stockdb/hoge.js", "w", encoding="utf-8") as fp:
        fp.write("const stockdata=" + js + ";\n")
        #fp.write("const stockcols={" + ",".join(f"'{c.dbname}':'{c.showname}'" for c in HISTORY.columns()) + "};\n")

####

if __name__ == '__main__':
    main()
####

