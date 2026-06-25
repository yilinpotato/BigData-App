"""Hive 数据仓库：用 Spark 的 Hive 支持，在 HDFS 上的清洗 Parquet 之上建外部表，
并用 HiveQL 查询。元数据持久化到 Hive metastore（Derby）。
运行: source setup/env.sh && cd /home/qqrtq/spark-tmp && python <repo>/src/hive_warehouse.py
（在无空格目录运行，metastore_db/derby.log 落在此处）

对应 HiveQL 脚本: sql/hive_setup.hql
"""
from pyspark.sql import SparkSession

HDFS = "hdfs://localhost:9000"


def get_hive_spark():
    return (SparkSession.builder
            .appName("hive_warehouse")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "file:///home/qqrtq/spark-warehouse")
            .config("spark.sql.shuffle.partitions", "16")
            .enableHiveSupport()                 # 启用 Hive 元数据 + HiveQL
            .getOrCreate())


def main():
    spark = get_hive_spark()
    spark.sparkContext.setLogLevel("ERROR")

    # 1) 数仓层：把按年分区的清洗表物化为一份扁平 Parquet（便于 Hive 外部表整表查询）
    (spark.read.parquet(f"{HDFS}/steam/clean/games")
          .write.mode("overwrite").parquet(f"{HDFS}/steam/warehouse/games"))

    # 2) 建数据库 + 外部表（指向 HDFS 上的 Parquet，schema 自动推断）
    spark.sql("CREATE DATABASE IF NOT EXISTS steam")
    spark.sql("DROP TABLE IF EXISTS steam.games")
    spark.sql("DROP TABLE IF EXISTS steam.tags_long")
    spark.sql(f"CREATE TABLE steam.games USING PARQUET LOCATION '{HDFS}/steam/warehouse/games'")
    spark.sql(f"CREATE TABLE steam.tags_long USING PARQUET LOCATION '{HDFS}/steam/clean/tags_long'")

    print("=== SHOW TABLES IN steam ===")
    spark.sql("SHOW TABLES IN steam").show(truncate=False)

    # 2) HiveQL 分析查询
    print("=== 每年发行量 Top（HiveQL）===")
    spark.sql("""
        SELECT release_year, COUNT(*) AS n_games
        FROM steam.games
        WHERE release_year BETWEEN 2014 AND 2019
        GROUP BY release_year ORDER BY n_games DESC
    """).show()

    print("=== 各价格区间口碑（HiveQL）===")
    spark.sql("""
        SELECT CASE WHEN price = 0 THEN 'free' WHEN price < 10 THEN 'low'
                    WHEN price < 30 THEN 'mid' ELSE 'high' END AS tier,
               COUNT(*) AS n, ROUND(AVG(positive_ratio), 3) AS avg_ratio
        FROM steam.games GROUP BY tier ORDER BY avg_ratio DESC
    """).show()

    print("=== 最热门标签 Top 10（HiveQL）===")
    spark.sql("""
        SELECT tag, SUM(votes) AS total_votes
        FROM steam.tags_long GROUP BY tag
        ORDER BY total_votes DESC LIMIT 10
    """).show(truncate=False)

    print("[hive] 外部表已建于 metastore，数据仍在 HDFS（DROP 表不删 HDFS 数据）。")
    spark.stop()


if __name__ == "__main__":
    main()
