# 交互式仪表盘 / Streamlit Dashboard

团队 **罗马（Roma）** 的 Steam 大数据分析仪表盘。

## 架构

Spark/HDFS 负责重计算，仪表盘只读预计算的小结果，因此运行时**不依赖 Spark/HDFS**，可直接部署到云端。

```
HDFS Parquet ──(src/export_dashboard_data.py, Spark)──> app/data/*.csv ──> Streamlit 仪表盘
```

## 本地运行

```bash
# 1) 先用 Spark 生成聚合数据（需 HDFS + 清洗后的 Parquet）
source setup/env.sh
python src/export_dashboard_data.py          # 产出 app/data/*.csv

# 2) 启动仪表盘
streamlit run app/dashboard.py
# 浏览器打开 http://localhost:8501
```

## 部署到 Streamlit Community Cloud（拿公开 URL）

1. 仓库已含 `app/data/*.csv`（预计算结果）与 `app/requirements.txt`，无需 Spark 即可运行。
2. 登录 https://share.streamlit.io ，连接 GitHub 仓库 `yilinpotato/BigData-App`。
3. 主文件填 `app/dashboard.py`，依赖填 `app/requirements.txt`。
4. 部署后即得公开 URL，页面侧边栏含团队名「罗马」。

## 内容

概览指标卡 + 8 个分析 Tab（发行趋势 / 类型 / 价格 / 时长 / 厂商 / 平台 / 标签 / 成就），
每个含交互式 altair 图表与中文解读。
