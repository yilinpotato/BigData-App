# 项目计划单 / Project Plan

团队 **罗马（Roma）** · 罗景楠、马亦麟
对照评分细则（报告 50 + 代码 50 + 加分）逐项推进。

## 阶段 0 — 仓库与协作 ✅/🔄
- [x] 审阅数据集、要求 PDF、评分细则 PDF
- [x] 初始化本地 Git 仓库、目录结构、README、计划单
- [ ] 推送到 GitHub 公开仓库 `yilinpotato/BigData-App`（待网络/凭据就绪）

## 阶段 1 — 环境搭建 ✅
- [x] 安装/启动 Hadoop 3.3.6 HDFS（单机伪分布式，Java 17，免 sudo/SSH）
- [x] Spark/PySpark 3.5.5（复用 lab_env），验证从 HDFS 读数据成功
- [ ] （可选）配置 Hive

## 阶段 2 — 数据入 HDFS（对应：HDFS 存储策略 5 分）✅
- [x] 将 6 个 CSV 上传到 HDFS `hdfs:///steam/raw`（`hdfs dfs -put`）
- [x] 存储策略：清洗后写 Parquet（列式+压缩），主表按 `release_year` 分区 → `hdfs:///steam/clean`
- [x] 数据加载模块 `src/common.py`（从 hdfs:// 读取）

## 阶段 3 — 清洗与预处理（对应：预处理 8 分 + 管线设计 10 分）🔄
- [x] 类型转换（日期、数值、布尔）
- [x] 派生字段：好评率、总评价、是否免费、owners 上下界/中位
- [x] 多值字段计数 + 平台布尔（platforms/categories/genres）
- [x] 解析 owners 拥有量区间为数值
- [x] 缺失值/异常值处理（去重、空 name 过滤、负价归零）
- [x] 标签宽表 → 长表（215,633 行，votes>0）→ `hdfs:///steam/clean/tags_long`
- [ ] requirements/media 的 HTML/JSON 解析（按需，深度加分）

## 阶段 4 — 分析问题（对应：分析方法 8 分 + 关键发现 6 分）✅
8 个分析问题已在 `notebooks/steam_analysis.ipynb`（PySpark + Spark SQL）完成并执行出图：
- [x] Q1 年度发行趋势（游戏数量随时间变化）
- [x] Q2 热门类型及其好评率（explode genres）
- [x] Q3 价格分布；价格 vs 评价/拥有量；免费 vs 付费
- [x] Q4 游玩时长 vs 类型（用户粘性）
- [x] Q5 Top 开发商/发行商（数量 & 口碑）
- [x] Q6 平台支持（win/mac/linux）趋势
- [x] Q7 最热门标签（tags_long 聚合）
- [x] Q8 成就数 vs 时长/评分
- 产出: `sql/analysis_queries.sql`、`src/steam_analysis.py`（notebook 导出）

## 阶段 5 — Hive 数据仓库（可选加分，对应：SQL/HiveQL 5 分）
- [ ] 建 Hive 外部表
- [ ] 编写 HiveQL 分析脚本

## 阶段 6 — 可视化（对应：可视化 8 分）✅
- [x] 8 张图（matplotlib/seaborn）存于 `figures/`，notebook 内每图配中文解读

## 阶段 7 — 报告 PDF（对应：报告 50 分）✅
`report/report.md` → `report/report.pdf`（7 页，中文，含 8 图，真实数据）
- [x] 数据集介绍
- [x] HDFS 存储与访问策略
- [x] 预处理步骤
- [x] 分析问题与方法
- [x] 关键发现（引用实测数字）
- [x] 可视化与解读
- [x] 结论与反思
- [x] 整体连贯性 + 团队分工（初稿，待按实际微调）

## 阶段 8 — 加分项（最多 +10）🔄
- [x] Streamlit 仪表盘（含团队名「罗马」），`app/dashboard.py`，8 个分析 Tab + 概览
- [x] 数据导出 `src/export_dashboard_data.py`（Spark 预计算 → `app/data/*.csv`）
- [x] AppTest 验证 0 异常；`app/requirements.txt` 就绪可部署
- [ ] （可选）部署到 Streamlit Cloud 拿公开 URL（需你的账号登录）

## 阶段 9 — 打包提交
- [ ] 整理 notebook + .py + SQL + 报告 PDF
- [ ] 最终自检对照评分细则
