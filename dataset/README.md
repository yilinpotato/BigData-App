# 数据集说明 / Dataset

来源：Kaggle [Steam Store Games (Clean dataset)](https://www.kaggle.com/datasets/nikdavis/steam-store-games/data)，
由 SteamSpy 与 Steam Storefront API 抓取，约 27,000 款游戏（2019 年快照）。
所有表以 `appid` / `steam_appid` 关联。

## 文件清单

| 文件 | 行数 | 主要字段 |
|---|---|---|
| `steam.csv` | 27,075 | `appid, name, release_date, english, developer, publisher, platforms, required_age, categories, genres, steamspy_tags, achievements, positive_ratings, negative_ratings, average_playtime, median_playtime, owners, price` |
| `steamspy_tag_data.csv` | 29,022 | `appid` + ~370 个标签列（宽表，值为该标签投票数） |
| `steam_description_data.csv` | 174,903 | `steam_appid, detailed_description, about_the_game, short_description` |
| `steam_media_data.csv` | 27,332 | `steam_appid, header_image, screenshots, background, movies`（screenshots/movies 为嵌套 JSON 字符串） |
| `steam_requirements_data.csv` | 27,319 | `steam_appid, pc_requirements, mac_requirements, linux_requirements, minimum, recommended`（含 HTML） |
| `steam_support_info.csv` | 27,136 | `steam_appid, website, support_url, support_email` |

## 字段处理要点

- **多值字段**用 `;` 分隔：`platforms`、`categories`、`genres`、`steamspy_tags` → 需 `split` 展开。
- **`owners`** 是区间字符串（如 `10000000-20000000`）→ 解析为上下界 / 取中位数。
- **`release_date`** → 转 `date`，可抽取年份做时间序列。
- **`price`** 单位为英镑（£），`0` 表示免费。
- **`positive_ratings` / `negative_ratings`** → 可计算好评率 `pos/(pos+neg)`。
- **标签宽表**：建议 `melt` 成长表 `(appid, tag, votes)` 再过滤 `votes>0`。
- **media / requirements** 字段含 Python-dict 风格字符串与 HTML，需用 `regexp` / JSON 解析。

## 入 HDFS（计划）

```bash
# 1. 在 HDFS 建目录
hdfs dfs -mkdir -p /steam/raw

# 2. 上传原始 CSV
hdfs dfs -put -f *.csv /steam/raw/

# 3. PySpark 读取
#    df = spark.read.option("header", True).csv("hdfs:///steam/raw/steam.csv")
```

> 大文件（`steam_description_data.csv` 91M、`steam_media_data.csv` 88M）若未随仓库提供，
> 可从上方 Kaggle 链接下载后放入本目录。
