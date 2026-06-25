"""数据清洗管线 (Spark, 从 HDFS 读 CSV → 清洗 → 写 Parquet 回 HDFS)。

运行: source setup/env.sh && python src/clean_pipeline.py
产出:
  hdfs:///steam/clean/games        主表(清洗+派生字段), 按 release_year 分区
  hdfs:///steam/clean/tags_long    标签宽表 → 长表 (appid, tag, votes>0)
"""
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import get_spark, RAW, CLEAN


def clean_games(spark):
    """清洗主表 steam.csv：类型转换、派生字段、缺失值处理。"""
    raw = spark.read.option("header", True).csv(f"{RAW}/steam.csv")
    print(f"[games] raw rows = {raw.count()}")

    g = (
        raw
        # --- 类型转换 ---
        .withColumn("appid", F.col("appid").cast(T.IntegerType()))
        .withColumn("release_date", F.to_date("release_date", "yyyy-MM-dd"))
        .withColumn("english", F.col("english").cast(T.IntegerType()))
        .withColumn("required_age", F.col("required_age").cast(T.IntegerType()))
        .withColumn("achievements", F.col("achievements").cast(T.IntegerType()))
        .withColumn("positive_ratings", F.col("positive_ratings").cast(T.LongType()))
        .withColumn("negative_ratings", F.col("negative_ratings").cast(T.LongType()))
        .withColumn("average_playtime", F.col("average_playtime").cast(T.IntegerType()))
        .withColumn("median_playtime", F.col("median_playtime").cast(T.IntegerType()))
        .withColumn("price", F.col("price").cast(T.DoubleType()))
        # --- 派生字段 ---
        .withColumn("release_year", F.year("release_date"))
        .withColumn("total_ratings", F.col("positive_ratings") + F.col("negative_ratings"))
        .withColumn(
            "positive_ratio",
            F.when(
                (F.col("positive_ratings") + F.col("negative_ratings")) > 0,
                F.col("positive_ratings") / (F.col("positive_ratings") + F.col("negative_ratings")),
            ).otherwise(F.lit(None)),
        )
        .withColumn("is_free", (F.col("price") == 0))
        # owners 区间 "10000000-20000000" → 上下界 + 中位估计
        .withColumn("owners_low", F.split("owners", "-").getItem(0).cast(T.LongType()))
        .withColumn("owners_high", F.split("owners", "-").getItem(1).cast(T.LongType()))
        .withColumn("owners_mid", ((F.col("owners_low") + F.col("owners_high")) / 2).cast(T.LongType()))
        # 多值字段计数（用 ; 分隔）
        .withColumn("num_genres", F.size(F.split("genres", ";")))
        .withColumn("num_categories", F.size(F.split("categories", ";")))
        .withColumn("num_platforms", F.size(F.split("platforms", ";")))
        # 平台布尔
        .withColumn("on_windows", F.col("platforms").contains("windows"))
        .withColumn("on_mac", F.col("platforms").contains("mac"))
        .withColumn("on_linux", F.col("platforms").contains("linux"))
    )

    # --- 缺失值 / 异常处理 ---
    # 必须有 appid 和非空 name；价格为负视为异常置 0
    g = (
        g.filter(F.col("appid").isNotNull() & (F.length(F.trim("name")) > 0))
        .withColumn("price", F.when(F.col("price") < 0, F.lit(0.0)).otherwise(F.col("price")))
        .dropDuplicates(["appid"])
    )
    print(f"[games] cleaned rows = {g.count()}")
    return g


def melt_tags(spark):
    """steamspy 标签宽表(~370 列) → 长表 (appid, tag, votes)，仅保留 votes>0。"""
    tags = spark.read.option("header", True).csv(f"{RAW}/steamspy_tag_data.csv")
    tag_cols = [c for c in tags.columns if c != "appid"]
    # 用 stack() 做宽转长；列名含数字开头(如 2d/360_video)需反引号
    pairs = ", ".join([f"'{c}', `{c}`" for c in tag_cols])
    stack_expr = f"stack({len(tag_cols)}, {pairs}) as (tag, votes)"
    long_df = (
        tags.select(F.col("appid").cast(T.IntegerType()), F.expr(stack_expr))
        .withColumn("votes", F.col("votes").cast(T.IntegerType()))
        .filter(F.col("votes") > 0)
    )
    print(f"[tags] long rows (votes>0) = {long_df.count()}")
    return long_df


def main():
    spark = get_spark("clean_pipeline")

    games = clean_games(spark)
    (games.write.mode("overwrite").partitionBy("release_year")
          .parquet(f"{CLEAN}/games"))
    print(f"[write] {CLEAN}/games")

    tags_long = melt_tags(spark)
    (tags_long.write.mode("overwrite").parquet(f"{CLEAN}/tags_long"))
    print(f"[write] {CLEAN}/tags_long")

    # --- 简单数据质量报告 ---
    print("\n=== 数据质量速览 ===")
    games.select(
        F.count("*").alias("games"),
        F.sum(F.col("positive_ratio").isNull().cast("int")).alias("null_pos_ratio"),
        F.sum(F.col("release_year").isNull().cast("int")).alias("null_year"),
        F.round(F.avg("positive_ratio"), 3).alias("avg_pos_ratio"),
        F.sum(F.col("is_free").cast("int")).alias("free_games"),
    ).show()

    spark.stop()


if __name__ == "__main__":
    main()
