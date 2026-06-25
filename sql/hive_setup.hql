-- Hive 数据仓库建表 + 查询脚本（HiveQL）
-- 通过 Spark SQL 的 Hive 支持执行（src/hive_warehouse.py，enableHiveSupport）。
-- 外部表指向 HDFS 上的 Parquet，元数据存于 Hive metastore；DROP 表不删 HDFS 数据。

-- 1) 数据库
CREATE DATABASE IF NOT EXISTS steam;

-- 2) 外部表（schema 自动从 Parquet 推断）
--    games 指向数仓层扁平 Parquet（由分区表 hdfs:///steam/clean/games 物化而来）
DROP TABLE IF EXISTS steam.games;
DROP TABLE IF EXISTS steam.tags_long;
CREATE TABLE steam.games     USING PARQUET LOCATION 'hdfs://localhost:9000/steam/warehouse/games';
CREATE TABLE steam.tags_long USING PARQUET LOCATION 'hdfs://localhost:9000/steam/clean/tags_long';

SHOW TABLES IN steam;

-- 3) 分析查询（HiveQL）
-- 3.1 近年发行量
SELECT release_year, COUNT(*) AS n_games
FROM steam.games
WHERE release_year BETWEEN 2014 AND 2019
GROUP BY release_year
ORDER BY n_games DESC;

-- 3.2 价格档位与口碑
SELECT CASE WHEN price = 0 THEN 'free'
            WHEN price < 10 THEN 'low'
            WHEN price < 30 THEN 'mid'
            ELSE 'high' END AS tier,
       COUNT(*)                     AS n,
       ROUND(AVG(positive_ratio),3) AS avg_ratio
FROM steam.games
GROUP BY tier
ORDER BY avg_ratio DESC;

-- 3.3 最热门标签 Top 10
SELECT tag, SUM(votes) AS total_votes
FROM steam.tags_long
GROUP BY tag
ORDER BY total_votes DESC
LIMIT 10;
