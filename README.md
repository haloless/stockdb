# stockdb

# 安装

1. 首先安装Python集成环境Anaconda（这个是现在金融数学分析的标准工具包集合）

    官方下载地址：https://repo.anaconda.com/archive/Anaconda3-2019.07-Windows-x86_64.exe
    
    清华国内镜像：https://mirrors.tuna.tsinghua.edu.cn/anaconda/archive/Anaconda3-2019.07-Windows-x86_64.exe

    下载后双击安装，一般选择为单一用户默认安装即可。

    安装好了之后，可以确认安装路径为：C:\Users\用户名\Anaconda3

1. 下载本应用程序

    压缩包下载地址：https://github.com/haloless/stockdb/archive/master.zip

    下载后解压到方便的地方即可。


# 使用

1. 双击run.bat，应看到提示信息服务器已启动

1. 使用浏览器打开本地网址（最好用Chrome浏览器）http://127.0.0.1:5000/static/index.html
可以看到功能列表。

1. 首先初始化本地数据库
点击“导入股票数据”（http://127.0.0.1:5000/static/stockinsert.html）
然后点击按钮“创建数据表”即可。

1. 其次可以开始导入Excel数据了
在同一页面上选择Excel文件并“执行导入”，应该可以看到OK表示导入成功。

1. 查看股票列表（http://127.0.0.1:5000/static/stocks.html）

1. 股票历史数据（http://127.0.0.1:5000/static/stockhist.html）
在页面上输入关注的股票代码并点击按钮“query”后，可以选择数据项目（价格，涨幅，成交量，累计成交量等）进行图表比较

1. 股票历史统计（http://127.0.0.1:5000/static/stockstat.html）
取股票的历史平均值并进行列表排序。


