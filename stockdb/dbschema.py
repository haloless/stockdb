"""DB schema
- STOCK
- HISTORY
"""

# from collections import namedtuple


#
# Field = namedtuple("Field", ["name", "type", "constraint"])
class Field:
    def __init__(self, showname, dbname, dbtype, constraint=None, converter=None, derived=False):
        self.showname = showname
        self.dbname = dbname
        self.dbtype = dbtype
        self.constraint = constraint
        self.converter = converter
        self.derived = derived

    def __str__(self):
        return self.dbname

    def to_sql_create(self):
        return f'{self.dbname} {self.dbtype} {self.constraint or ""}'
####

class Table:

    @classmethod
    def columns(cls):
        return [v for (k, v) in cls.__dict__.items() if isinstance(v, Field)]

    @classmethod
    def column_dbnames(cls):
        return [c.dbname for c in cls.columns()]

    @classmethod
    def column_shownames(cls):
        return [c.showname for c in cls.columns()]

    @classmethod
    def field_by_dbname(cls, dbname):
        for c in cls.columns():
            if c.dbname == dbname:
                return c
        return None

    @classmethod
    def field_by_showname(cls, showname):
        for c in cls.columns():
            if c.showname == showname:
                return c
        return None

    @classmethod
    def to_sql_create(cls):
        cols = ','.join(c.to_sql_create() for c in cls.columns())
        return f"create table {cls.__name__} ({cols});"
####

################################################################################

class STOCK(Table):
    """STOCK table"""
    SYMBOL = Field(showname='代码', dbname='Symbol', dbtype='char(10)', constraint='primary key not null')
    LOCALNAME = Field(showname='名称', dbname='LocalName', dbtype='text', constraint='not null')
    INDUSTRY = Field(showname='细分行业', dbname='Industry', dbtype='text', constraint='not null')
    # RDEXPENSE = Field(showname='研发费用(亿)', dbname='RDExpense', dbtype='real')
    # EMPLOYEENUM = Field(showname='员工人数', dbname='EmployeeNum', dbtype='real')
####

# 代码	名称	现价	涨幅%	涨速%	卖价	流通股(亿)	市盈(动)	换手%	细分行业
# 相对流量%	大宗流量%	现量	开盘换手Z
# 连涨天	3日涨幅%	20日涨幅%	60日涨幅%	年初至今%
# 净流入	大宗流入	贝塔系数	每股收益	每股净资	每股公积	每股未分配	权益比%	净利润率%
# 研发费用(亿)	员工人数
# 昨收	扣非净利润(亿)

class HISTORY(Table):
    """HISTORY table
    净流入=流入量*价格
    """
    # primary fields
    TODAY       = Field(showname='日期', dbname='Today', dbtype='date', constraint='not null')
    SYMBOL      = Field(showname='代码', dbname='Symbol', dbtype='char(10)', constraint='not null')
    LOCALNAME   = Field(showname='名称', dbname='LocalName', dbtype='text', constraint='not null')
    INDUSTRY    = Field(showname='细分行业', dbname='Industry', dbtype='text', constraint='not null')
    #
    PRICE       = Field(showname='现价', dbname='Price', dbtype='real')
    RISE        = Field(showname='涨幅%', dbname='Rise', dbtype='real')
    LISTSHARES  = Field(showname='流通股(亿)', dbname='ListShares', dbtype='real')
    TURNOVERRATE    = Field(showname='换手%', dbname='TurnoverRate', dbtype='real')
    RELATIVEFLOW = Field(showname='相对流量%', dbname='RelativeFlow', dbtype='real')
    BULKFLOW    = Field(showname='大宗流量%', dbname='BulkFlow', dbtype='real')
    NETINFLOW   = Field(showname='净流入', dbname='NetInflow', dbtype='real')
    BULKINFLOW  = Field(showname='大宗流入', dbname='BulkInflow', dbtype='real')
    CURRENTFLOW = Field(showname='现量', dbname='CurrentFlow', dbtype='real')
    RISESPEED   = Field(showname='涨速%', dbname="RiseSpeed", dbtype='real')
    MAINNETINFLOW = Field(showname='主力净额', dbname='MainNetInflow', dbtype='real')
    MAINRATIO   = Field(showname='主力占比%', dbname='MainRatio', dbtype='real')
    AVERAGERISE = Field(showname='均涨幅%', dbname='AverageRise', dbtype='real')
    OFFENSEWAVE = Field(showname='攻击波%', dbname='OffenseWave', dbtype='real')
    CORRECTIVEWAVE = Field(showname='回头波%', dbname='CorrectiveWave', dbtype='real')
    CONTRISEDAYS = Field(showname='连涨天', dbname='ContRiseDays', dbtype='int')
    BUYSELLRATIO = Field(showname='内外比', dbname='BuySellRatio', dbtype='real')
    QUANTITYRATIO = Field(showname='量比', dbname='QuantityRatio', dbtype='real')
    RISEDAY10     = Field(showname='10日涨幅%', dbname='RiseDay10', dbtype='real')
    RISEDAY60     = Field(showname='60日涨幅%', dbname='RiseDay60', dbtype='real')
    RISEYEAR      = Field(showname='一年涨幅%', dbname='RiseDayYear', dbtype='real')
    FROZENTRADERATIO = Field(showname='封成比', dbname='FrozenTradeRatio', dbtype='real')
    NETASSETPERSHARE = Field(showname='每股净资', dbname='NetAssetPerShare', dbtype='real')
    PRICEBOOKRATIO = Field(showname='市净率', dbname='PriceBookRatio', dbtype='real')
    # derived quantities
    MARKETCAPITAL = Field(showname='流通市值(亿)', dbname='MarketCapital', dbtype='real', derived=True)
    TURNOVER    = Field(showname='成交额(亿)', dbname='Turnover', dbtype='real', derived=True)
    INFLOW      = Field(showname='流入额(亿)', dbname='Inflow', dbtype='real', derived=True)
    OUTFLOW     = Field(showname='流出额(亿)', dbname='Outflow', dbtype='real', derived=True)
    VOLUME      = Field(showname='成交量(亿)', dbname='Volume', dbtype='real', derived=True)
    INFLOWSHARE = Field(showname='流入量(亿)', dbname='InflowShare', dbtype='real', derived=True)
    OUTFLOWSHARE= Field(showname='流出量(亿)', dbname='OutflowShare', dbtype='real', derived=True)
####

class HISTORY2(Table):
    TODAY = Field(showname='日期', dbname='Today', dbtype='date', constraint='not null')
    SYMBOL = Field(showname='代码', dbname='Symbol', dbtype='char(10)', constraint='not null')
    #
    CLOSEPRICE = Field(showname='昨收', dbname='ClosePrice', dbtype='real')
    SELLPRICE = Field(showname='卖价', dbname='SellPrice', dbtype='real')
    PRICEEARNRATIO = Field(showname='市盈(动)', dbname='PriceEarnRatio', dbtype='real')

    OPENTURNOVER = Field(showname='开盘换手Z', dbname='OpenTurnover', dbtype='real')
    RISEDAY3 = Field(showname='3日涨幅%', dbname='RiseDay3', dbtype='real')
    RISEDAY20 = Field(showname='20日涨幅%', dbname='RiseDay20', dbtype='real')
    RISEYEARBEGIN = Field(showname='年初至今%', dbname='RiseYearBegin', dbtype='real')

    BETACOEF = Field(showname='贝塔系数', dbname='BetaCoef', dbtype='real')
    EARNPERSHARE = Field(showname='每股收益', dbname='EarnPerShare', dbtype='real')
    ADDPAIDINPERSHARE = Field(showname='每股公积', dbname='AddPaidInPerShare', dbtype='real')
    UNPAIDPERSHARE = Field(showname='每股未分配', dbname='UnpaidPerShare', dbtype='real')
    EQUITYRATIO = Field(showname='权益比%', dbname='EquityRatio', dbtype='real')
    NONEXTRANETPROFIT = Field(showname='扣非净利润(亿)', dbname='NonExtraNetProfit', dbtype='real')
    NETPROFITRATE = Field(showname='净利润率%', dbname='NetProfitRatio', dbtype='real')

####


################################################################################
if __name__ == '__main__':
    print(STOCK.columns())
    print(STOCK.to_sql_create())
    print(HISTORY.columns())
    print(HISTORY.to_sql_create())
####

