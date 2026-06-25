"""公共工具：SparkSession 构建 + HDFS 路径常量。

所有分析脚本统一从这里取 Spark 会话，确保配置一致（含含空格 cwd 的规避）。
"""
import os
from pyspark.sql import SparkSession

# --- HDFS 路径 ---
HDFS = "hdfs://localhost:9000"
RAW = f"{HDFS}/steam/raw"        # 原始 CSV
CLEAN = f"{HDFS}/steam/clean"    # 清洗后 Parquet

# 仓库目录含空格，必须把 Spark 的本地落盘指向无空格目录，否则 metastore/临时文件会报错
_LOCAL_DIR = "/home/qqrtq/spark-tmp"
_WAREHOUSE = "file:///home/qqrtq/spark-warehouse"


def get_spark(app_name: str = "steam-bigdata", cores: str = "*") -> SparkSession:
    """返回本地模式 SparkSession。

    Args:
        app_name: Spark 应用名。
        cores: local[N] 的 N，默认 "*"（全部核）。
    """
    os.makedirs(_LOCAL_DIR, exist_ok=True)
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(f"local[{cores}]")
        .config("spark.sql.warehouse.dir", _WAREHOUSE)
        .config("spark.local.dir", _LOCAL_DIR)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "16")  # 单机不需要 200 分区
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
