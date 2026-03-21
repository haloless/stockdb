
# Project Requirement

Check below files for project requirement

## Original request

[Original request](../doc/orig_request.md) describes motivation and high-level requirement of the project.

## Sample data

[Sample data](../excel/20250911.xls) is sample input for importing daily China A-share stock data.
The file name indicates date in `yyyymmdd` format (e.g. 20250911 for above file) of this dataset.

Also can refer to other similar sample files in the same folder.

Note the file format can be:
- true Excel file
- actual delimiter-separated text file with a fake .xls extension, encoding is Simplified Chinese GB2312


# Application design

## general

- conda env as described in copilot instructions file.
- python/flask to provide user-friendly web interface.
- can use pandas if necessary.


## planning

- make sure to make new plan from scratch, do not reuse old code.
- new module `stockdb2`
- new flask app entry `dbapp2.py`
- static file under dir `static/stock2/`
- **DO NOT** refer to existing code (`dbapp.py` and dir `stockdb`). Make your own planning.


## database

- use python sqlite as db backend
- db file `stock2.db`
- stocks symbol `代码` and industry `细分行业` should be saved in a master table.
- relavent fields mentioned in **Original request** should have a separate table with primary keys of `date` and `symbol`.



## data import

- the stock data file should be able to uploaded via upload page, either single file or multiple files.
- the target date of file should be inferred from their filenames. e.g., filename like `自选股20250911` indicates the date of the data is `2025-09-11`.
- be careful with data conversion, e.g.:
  Stock code `代码` column may look like `="603359"`, which should be stripped to symbol name string `603359`.
  Some column may have string `--` indicating empty value.
  Some numerical value can have Chinese character `亿` or `万` which must be properly converted.
- after loading from file, should calculate and populate relevant fields as described in **original requst**.
- the stock data need to be joined with the corresponding date column and saved into sqlite db.

## data presentation

- refer to **Original request**.
- you can decide the frontend tech, but try to keep simple.
- prefer REST-like api between frontend and backend.
- page design should be clean, do not need decorative css styles.
- pages language should be in Simplified Chinese.


Must to have:
- A page to represent fundamental stats calculation like average or cumulative volume stock data.
- A page to show time-series change of key stock data.
- User can specify a list of interested stocks (e.g., directly give symbols or by industries). User can also specify the time range like start/end dates and the length of rolling windows for calculating average or cumulation.





