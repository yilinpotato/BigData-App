# Steam 游戏商店大数据分析 / Steam Store Big Data Analysis

> 大数据应用课程期末项目 · 团队 **罗马（Roma）**
> 成员：罗景楠、马亦麟

基于 Kaggle [Steam Store Games](https://www.kaggle.com/datasets/nikdavis/steam-store-games/data)
数据集（约 27,000 款游戏），使用 **HDFS + PySpark + Spark SQL（+ 可选 Hive）+ Python 可视化**
完成数据采集、清洗、转换、分析与洞察呈现的完整大数据处理流程。

---

## 1. 技术栈

| 环节 | 工具 |
|---|---|
| 数据存储 | HDFS |
| 数据处理 / 分析 | PySpark (DataFrame API) |
| 结构化查询 | Spark SQL |
| 数据仓库（可选） | Hive / HiveQL |
| 可视化 | matplotlib / seaborn / plotly |

## 2. 数据集

数据集共 6 个 CSV，以 `appid` 关联。详见 [dataset/README.md](dataset/README.md)。

| 文件 | 大小 | 行数 | 说明 |
|---|---|---|---|
| `steam.csv` | 5.6M | 27,075 | **主表**：名称、发行日期、开发/发行商、平台、类型、好评/差评数、游玩时长、拥有量区间、价格 |
| `steamspy_tag_data.csv` | 21M | 29,022 | 标签宽表（~370 个标签列，存每个标签投票数） |
| `steam_description_data.csv` | 91M | 174,903 | 游戏描述文本 |
| `steam_media_data.csv` | 88M | 27,332 | 图片/截图/视频链接（嵌套 JSON） |
| `steam_requirements_data.csv` | 35M | 27,319 | 配置要求（含 HTML） |
| `steam_support_info.csv` | 1.9M | 27,136 | 官网/客服链接 |

> 数据获取与入 HDFS 的说明见 [dataset/README.md](dataset/README.md)。

## 3. 仓库结构

```
.
├── README.md              # 本文件
├── PLAN.md                # 项目计划单 / 进度清单
├── docs/                  # 课程要求与评分细则 PDF
├── dataset/               # 原始数据集（CSV）+ 数据说明
├── notebooks/             # 分析 Jupyter notebook (.ipynb)
├── src/                   # PySpark 加载/清洗/分析脚本 (.py)
├── sql/                   # Spark SQL / HiveQL 脚本
├── figures/               # 生成的可视化图表
└── report/                # 项目报告 (PDF / Markdown 源)
```

## 4. 运行环境

单机大数据栈（无需 sudo，用户级安装）。详见 [setup/README.md](setup/README.md)。

| 组件 | 版本 |
|---|---|
| Java | OpenJDK 17 (Temurin) |
| HDFS | Hadoop 3.3.6（单机伪分布式，`hdfs://localhost:9000`） |
| Spark / PySpark | 3.5.5 |
| Python 栈 | pandas, numpy, matplotlib, seaborn, JupyterLab（venv `lab_env`） |

一键启动：

```bash
bash setup/00_install_stack.sh   # 首次安装
bash setup/01_start_hdfs.sh      # 启动 HDFS
bash setup/02_load_data.sh       # 上传 CSV 到 hdfs:///steam/raw
source setup/env.sh              # 加载环境变量
```

## 5. 进度

见 [PLAN.md](PLAN.md)。

## 6. 团队分工

| 成员 | 分工 |
|---|---|
| 罗景楠 | 待补充 |
| 马亦麟 | 待补充 |
