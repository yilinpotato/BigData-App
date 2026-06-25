#!/usr/bin/env bash
# 把原始 CSV 上传到 HDFS /steam/raw。
# 注意: 仓库路径含空格("bigdata app")，Hadoop FsShell 对本地空格路径的 URI 解析有问题，
#       因此先用硬链接把 CSV 暴露到无空格的 staging 目录($STAGE)，再 put（硬链接不额外占盘）。
# 用法: bash setup/02_load_data.sh
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
source "$HERE/env.sh"

STAGE="/home/qqrtq/steam_data"
mkdir -p "$STAGE"
for f in "$REPO_ROOT"/dataset/*.csv; do
  ln -f "$f" "$STAGE/$(basename "$f")"   # 硬链接到无空格目录
done

hdfs dfs -mkdir -p /steam/raw
echo "[load] putting CSVs into hdfs:///steam/raw ..."
hdfs dfs -put -f "$STAGE"/*.csv /steam/raw/

echo "=== hdfs:///steam/raw ==="
hdfs dfs -ls -h /steam/raw
