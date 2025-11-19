# 🚀 Flight Crawler --- 航班价格定时爬虫系统

一个用于 **自动化批量爬取携程航班信息** 的 Python
爬虫系统，支持定时任务、失败重试、结果入库
MySQL、目标航司过滤、中转航班解析等功能。

## ✨ 功能

### ✔ 1. 航班信息采集

-   支持 **出发地、到达地、起飞日期** 的全量遍历\
-   从携程网页解析如下字段：
    -   航司名称
    -   航班号（支持多段中转，如 A + B）
    -   出发/到达机场
    -   出发/到达时间
    -   航班描述（直飞/中转）
    -   票价（自动处理符号与千分位）

### ✔ 2. 完整解析中转航班

通过 `get_airline_plane_no()` 自动解析多段航班号，例如：

    MU5101 + MU5678

### ✔ 3. 航司过滤功能

默认保留以下目标航司（可扩展）：

  航司代码   航空公司
  ---------- ----------
  MU         东方航空
  MF         厦门航空
  DZ         东海航空
  CX         国泰航空
  KE         大韩航空

### ✔ 4. 数据入库 MySQL（自动重试）

使用 `tenacity` 实现指数回退重试，保证数据稳定写入。

### ✔ 5. 定时任务调度（任务管理器）

`TaskManager` 会： - 首次启动立即爬取 - 之后每天 **00:00\~00:40**
自动执行一次 - 任务超时自动终止并重新执行 - 自动关闭数据库连接

## 📂 项目结构

    project/
    │── crawlerMain.py
    │── db_operations.py
    │── od.csv
    │── requirements.txt
    │── README.md

## 📦 环境依赖

    beautifulsoup4
    DrissionPage
    tenacity
    mysql-connector-python

## ⚙️ 配置说明

### 1. 准备航线文件 od.csv

    PVG,PEK
    SHA,CTU
    CAN,XIY

### 2. 配置数据库（在 db_operations.py 中）

``` python
host = 'localhost'
user = 'root'
password = 'yourpassword'
database = 'flight'
```

## ▶️ 运行方式

### 方式 1：直接启动爬虫（推荐）

    python crawlerMain.py

### 方式 2：服务器后台运行

    nohup python crawlerMain.py > log.txt 2>&1 &

## 🧠 代码核心流程

``` mermaid
flowchart TD
    A[读取 od.csv 航线] --> B[遍历日期与航线]
    B --> C[爬取携程页面]
    C --> D[解析航班信息<br>支持多段、中转]
    D --> E[过滤目标航司]
    E --> F[写入 MySQL<br>失败自动重试]
    F --> G[TaskManager 定时调度]
```

## 🛡 日志与容错机制

-   多处 try/except 防止爬虫中断\
-   使用 tenacity 自动重试\
-   超时任务自动终止\
-   全局 logging 输出

## 📜 License

MIT License

## 🤝 贡献

欢迎提交 PR 或 Issue！
