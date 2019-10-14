
import os

from flask import Flask
from flask import request
from flask import jsonify
from werkzeug.utils import secure_filename

import pandas as pd

import stockdb
import stockdb.dbconn
import stockdb.dbinsert
import stockdb.dbstat
import stockdb.dbutils
from stockdb.dbschema import STOCK, HISTORY

app = Flask(__name__)


@app.route('/cgi-bin/hoge.py')
def hoge():
    return '{"a":3, "b":2}'
####

@app.route('/cgi-bin/droptable.py', methods=['DELETE'])
def drop_table():
    try:
        stockdb.dbinsert.drop_tables()
        return "ok"
    except:
        return "failed"
####

@app.route('/cgi-bin/createtable.py', methods=['PUT'])
def create_table():
    try:
        stockdb.dbinsert.create_table()
        return "ok"
    except:
        return "failed, already exist"
####

@app.route('/cgi-bin/inserthist.py', methods=['POST'])
def insert_hist():
    print(dir(request))
    print(request.files)
    print(request.form)
    if 'input_excel' not in request.files:
        return jsonify("No file")
    if 'input_date' not in request.form:
        return jsonify("No date")
    
    f = request.files['input_excel']
    d = request.form['input_date']
    print(f.filename)
    print(d)

    filename = secure_filename(f.filename)
    filename = os.path.join('excel', filename)
    f.save(filename)

    stockdb.dbinsert.insert_data(filename, d)

    return jsonify("ok")
####


@app.route('/cgi-bin/stocklist.py')
def stocklist():
    conn = stockdb.dbconn.get_db_conn()
    df = pd.read_sql_query("select * from STOCK order by SYMBOL", conn)
    js = df.to_json(orient="table")
    return js
####

@app.route('/cgi-bin/stockhist.py', methods=['POST'])
def stockhist():
    data = request.get_json()
    print(data)
    
    conn = stockdb.dbconn.get_db_conn()
    sql = "select * from HISTORY where SYMBOL='{}'"

    if data["trade_only"]:
        trade_columns = [HISTORY.TODAY, HISTORY.SYMBOL, HISTORY.LOCALNAME, HISTORY.INDUSTRY,
        HISTORY.PRICE, HISTORY.CLOSEPRICE, HISTORY.SELLPRICE,
        HISTORY.RISE, HISTORY.RISESPEED, HISTORY.CIRCULATESHARES,
        HISTORY.PRICEEARNRATIO, HISTORY.TURNOVER,
        HISTORY.RELATIVEFLOW, HISTORY.BULKFLOW, HISTORY.CURRENTFLOW,
        HISTORY.NETINFLOW, HISTORY.BULKINFLOW, HISTORY.BETACOEF,
        HISTORY.MARKETCAPITAL, HISTORY.VOLUME, HISTORY.INFLOWSHARE, HISTORY.OUTFLOWSHARE]
        sql = "select " + ','.join(str(c) for c in trade_columns)
    else:
        sql = "select *"
    sql += " from HISTORY where SYMBOL=?"
    if data["from_date"]:
        from_date = data["from_date"]
        sql += f" and TODAY>='{from_date}'"
    if data["to_date"]:
        to_date = data["to_date"]
        sql += f" and TODAY<='{to_date}'"
    sql += " order by TODAY"

    symbols = data["symbols"]
    dfs_table = []
    dfs_chart = []
    for symbol in symbols:
        # run query to get basic data
        df = pd.read_sql_query(sql, conn, params=(symbol,))
        # enhance some more data, mainly accumulated values
        for c in [HISTORY.VOLUME, HISTORY.INFLOWSHARE, HISTORY.OUTFLOWSHARE]:
            if c.dbname in df:
                acc = df[c.dbname].cumsum()
                df["cum_" + c.dbname] = acc

        dfs_chart.append(df.to_json(orient="split"))

        # rename to readable
        df = stockdb.dbstat.rename_df_cols(df, HISTORY.columns())
        dfs_table.append(df.to_json(orient="table"))
    
    js_table = "{" + ",".join('"'+s+'"' + ":" + d for s,d in zip(symbols, dfs_table)) + "}"
    js_chart = "{" + ",".join('"'+s+'"' + ":" + d for s,d in zip(symbols, dfs_chart)) + "}"

    return '{' + f'"table":{js_table},"chart":{js_chart}' + '}'
####

@app.route('/cgi-bin/stockstat.py', methods=['POST'])
def stockstat():
    data = request.get_json()
    print(data)

    symbols = None
    if data["symbols"]:
        symbols = data["symbols"]
    from_date = None
    if data["from_date"]:
        from_date = data["from_date"]
    to_date = None
    if data["to_date"]:
        to_date = data["to_date"]
    industry = None
    if data["industry"]:
        industry = data["industry"]

    fields = [HISTORY.PRICE, HISTORY.RISE, 
        # HISTORY.RISESPEED, HISTORY.CIRCULATESHARES,
        #HISTORY.PRICEEARNRATIO, HISTORY.TURNOVER,
        # HISTORY.RELATIVEFLOW, HISTORY.BULKFLOW, HISTORY.CURRENTFLOW,
        HISTORY.NETINFLOW, HISTORY.BULKINFLOW,
        # HISTORY.BETACOEF,
        HISTORY.MARKETCAPITAL, HISTORY.VOLUME, HISTORY.INFLOWSHARE, HISTORY.OUTFLOWSHARE]

    df = stockdb.dbstat.get_grouped_df(begin_date=from_date, end_date=to_date, fields=fields,
    symbols=symbols, industry=industry)

    js = df.to_json(orient="table")
    return js
####


@app.route("/cgi-bin/stocksql.py", methods=['POST'])
def stocksql():
    data = request.get_json()
    print(data)

    df = pd.read_sql_query(data["sql"], stockdb.dbconn.get_db_conn())

    js = df.to_json(orient="table")
    return js
####


if __name__ == "__main__":
    app.run()
###

