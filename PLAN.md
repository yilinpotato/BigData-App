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

## 阶段 2 — 数据入 HDFS（对应：HDFS 存储策略 5 分）
- [x] 将 6 个 CSV 上传到 HDFS `hdfs:///steam/raw`（`hdfs dfs -put`）
- [ ] 设计存储策略：目录分区、CSV → Parquet 转换、压缩
- [ ] 编写数据加载脚本（从 hdfs:// 读取）

## 阶段 3 — 清洗与预处理（对应：预处理 8 分 + 管线设计 10 分）
- [ ] 类型转换（日期、数值、布尔）
- [ ] 多值字段拆分：platforms / categories / genres / steamspy_tags（`;` 分隔）
- [ ] 解析 owners 拥有量区间为数值
- [ ] 缺失值/异常值处理
- [ ] 标签宽表 → 长表
- [ ] requirements/media 的 HTML/JSON 解析

## 阶段 4 — 分析问题（对应：分析方法 8 分 + 关键发现 6 分）
设计 6–8 个分析问题（PySpark + Spark SQL），候选：
- [ ] Q1 年度发行趋势（游戏数量随时间变化）
- [ ] Q2 热门类型/标签及其好评率
- [ ] Q3 价格分布；价格 vs 评价/拥有量；免费 vs 付费
- [ ] Q4 游玩时长 vs 类型（用户粘性）
- [ ] Q5 Top 开发商/发行商（数量 & 口碑）
- [ ] Q6 平台支持（win/mac/linux）趋势
- [ ] Q7 标签共现 / 最热门标签
- [ ] Q8 成就数 vs 时长/评分

## 阶段 5 — Hive 数据仓库（可选加分，对应：SQL/HiveQL 5 分）
- [ ] 建 Hive 外部表
- [ ] 编写 HiveQL 分析脚本

## 阶段 6 — 可视化（对应：可视化 8 分）
- [ ] 用 matplotlib/seaborn/plotly 出图，每图配解读

## 阶段 7 — 报告 PDF（对应：报告 50 分）
- [ ] 数据集介绍
- [ ] HDFS 存储与访问策略
- [ ] 预处理步骤
- [ ] 分析问题与方法
- [ ] 关键发现
- [ ] 可视化与解读
- [ ] 结论与反思
- [ ] 整体连贯性 + 团队分工

## 阶段 8 — 加分项（最多 +10）
- [ ] Streamlit / Plotly Dash 仪表盘（含团队名「罗马」）
- [ ] （可选）部署公开访问 URL

## 阶段 9 — 打包提交
- [ ] 整理 notebook + .py + SQL + 报告 PDF
- [ ] 最终自检对照评分细则
