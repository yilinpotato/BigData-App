-- Steam 分析用 Spark SQL 查询合集
-- 视图: games (清洗主表), tags_long (标签长表)。来自 hdfs:///steam/clean/*。
-- 在 notebooks/steam_analysis.ipynb 中通过 spark.sql(...) 调用。

-- Q1. 年度发行趋势
SELECT release_year, COUNT(*) AS n_games
FROM games
WHERE release_year IS NOT NULL AND release_year BETWEEN 1997 AND 2019
GROUP BY release_year
ORDER BY release_year;

-- Q3. 价格分桶 × 好评率 / 拥有量
SELECT CASE
         WHEN price = 0 THEN '0 Free'
         WHEN price < 5 THEN '<5'
         WHEN price < 10 THEN '5-10'
         WHEN price < 20 THEN '10-20'
         WHEN price < 40 THEN '20-40'
         ELSE '40+' END AS price_band,
       COUNT(*)                  AS n,
       ROUND(AVG(positive_ratio),3) AS avg_ratio,
       ROUND(AVG(owners_mid),0)     AS avg_owners
FROM games
GROUP BY price_band;

-- Q5a. 发行游戏数最多的开发商
SELECT developer, COUNT(*) AS n
FROM games
WHERE developer IS NOT NULL
GROUP BY developer
ORDER BY n DESC
LIMIT 12;

-- Q5b. 累计好评最高的发行商
SELECT publisher, SUM(positive_ratings) AS pos
FROM games
WHERE publisher IS NOT NULL
GROUP BY publisher
ORDER BY pos DESC
LIMIT 12;

-- Q6. Mac / Linux 平台支持随年份变化
SELECT release_year,
       AVG(CAST(on_mac AS INT))   AS mac,
       AVG(CAST(on_linux AS INT)) AS linux
FROM games
WHERE release_year BETWEEN 2008 AND 2019
GROUP BY release_year
ORDER BY release_year;

-- Q7. 最受欢迎标签 Top 20（标签长表聚合）
SELECT tag, SUM(votes) AS total_votes, COUNT(*) AS n_games
FROM tags_long
GROUP BY tag
ORDER BY total_votes DESC
LIMIT 20;

-- Q8. 成就数量分桶 × 好评率 / 时长
SELECT CASE
         WHEN achievements = 0 THEN '0'
         WHEN achievements <= 10 THEN '1-10'
         WHEN achievements <= 50 THEN '11-50'
         WHEN achievements <= 100 THEN '51-100'
         ELSE '100+' END AS ach_band,
       COUNT(*)                          AS n,
       ROUND(AVG(positive_ratio),3)      AS avg_ratio,
       ROUND(AVG(average_playtime)/60,2) AS avg_hours
FROM games
WHERE total_ratings >= 50
GROUP BY ach_band;
