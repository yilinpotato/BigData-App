"""用 Spark 把 8 个分析的聚合结果导出为小 CSV/JSON 到 app/data/，
供 Streamlit 仪表盘在运行时直接用 pandas 读取（无需 Spark/HDFS）。
运行: source setup/env.sh && python src/export_dashboard_data.py
"""
import os, json
from pyspark.sql import functions as F
from common import get_spark, CLEAN

OUT = "/home/qqrtq/bigdata app/app/data"
os.makedirs(OUT, exist_ok=True)


def save(df, name):
    df.toPandas().to_csv(f"{OUT}/{name}.csv", index=False)
    print("wrote", name)


def main():
    sp = get_spark("export_dashboard")
    g = sp.read.parquet(f"{CLEAN}/games"); t = sp.read.parquet(f"{CLEAN}/tags_long")
    g.createOrReplaceTempView("games"); t.createOrReplaceTempView("tags_long")
    genre = g.withColumn("genre", F.explode(F.split("genres", ";"))).filter("length(trim(genre))>0")

    # Q1
    save(sp.sql("SELECT release_year, COUNT(*) n_games FROM games "
                "WHERE release_year BETWEEN 1997 AND 2019 GROUP BY release_year ORDER BY release_year"), "q1_release")
    # Q2
    save(genre.groupBy("genre").agg(F.count("*").alias("n"), F.round(F.avg("positive_ratio"),3).alias("avg_ratio"))
              .orderBy(F.desc("n")).limit(12), "q2_genre")
    # Q3
    save(sp.sql("""SELECT CASE WHEN price=0 THEN '0 Free' WHEN price<5 THEN '<5' WHEN price<10 THEN '5-10'
                   WHEN price<20 THEN '10-20' WHEN price<40 THEN '20-40' ELSE '40+' END price_band,
                   COUNT(*) n, ROUND(AVG(positive_ratio),3) avg_ratio, ROUND(AVG(owners_mid),0) avg_owners
                   FROM games GROUP BY price_band"""), "q3_price")
    # Q4
    save(genre.filter("total_ratings>=50").groupBy("genre")
              .agg(F.count("*").alias("n"), F.round(F.avg("average_playtime")/60,2).alias("avg_hours"))
              .filter("n>=30").orderBy(F.desc("avg_hours")).limit(12), "q4_playtime")
    # Q5
    save(sp.sql("SELECT developer, COUNT(*) n FROM games WHERE developer IS NOT NULL "
                "GROUP BY developer ORDER BY n DESC LIMIT 12"), "q5_dev")
    save(sp.sql("SELECT publisher, SUM(positive_ratings) pos FROM games WHERE publisher IS NOT NULL "
                "GROUP BY publisher ORDER BY pos DESC LIMIT 12"), "q5_pub")
    # Q6
    save(sp.sql("SELECT release_year, ROUND(AVG(CAST(on_mac AS INT)),3) mac, ROUND(AVG(CAST(on_linux AS INT)),3) linux "
                "FROM games WHERE release_year BETWEEN 2008 AND 2019 GROUP BY release_year ORDER BY release_year"), "q6_platform")
    # Q7
    save(sp.sql("SELECT tag, SUM(votes) total_votes, COUNT(*) n_games FROM tags_long "
                "GROUP BY tag ORDER BY total_votes DESC LIMIT 20"), "q7_tags")
    # Q8
    save(sp.sql("""SELECT CASE WHEN achievements=0 THEN '0' WHEN achievements<=10 THEN '1-10'
                   WHEN achievements<=50 THEN '11-50' WHEN achievements<=100 THEN '51-100' ELSE '100+' END ach_band,
                   COUNT(*) n, ROUND(AVG(positive_ratio),3) avg_ratio, ROUND(AVG(average_playtime)/60,2) avg_hours
                   FROM games WHERE total_ratings>=50 GROUP BY ach_band"""), "q8_ach")

    # 概览数字
    tot = g.count(); free = g.filter(F.col("is_free")).count()
    summary = {
        "total_games": tot, "free_games": free, "paid_games": tot-free,
        "avg_positive_ratio": round(g.agg(F.avg("positive_ratio")).first()[0], 4),
        "median_price_gbp": g.approxQuantile("price", [0.5], 0)[0],
        "win_pct": round(g.agg(F.avg(F.col("on_windows").cast("int"))).first()[0], 3),
        "mac_pct": round(g.agg(F.avg(F.col("on_mac").cast("int"))).first()[0], 3),
        "linux_pct": round(g.agg(F.avg(F.col("on_linux").cast("int"))).first()[0], 3),
    }
    with open(f"{OUT}/summary.json", "w") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print("summary:", summary)
    sp.stop()


if __name__ == "__main__":
    main()
