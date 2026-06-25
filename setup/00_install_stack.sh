#!/usr/bin/env bash
# 一键安装大数据栈（无需 sudo）。记录本项目实际使用的安装步骤，便于复现。
#   - Java 17 (Temurin) 已预装于 ~/jdk
#   - Spark 运行时来自已有 venv: ~/lab_env (pyspark 3.5.5 + pandas/matplotlib/seaborn/jupyter)
#   - HDFS 守护进程来自下面下载的 Hadoop 3.3.6 tarball
# 用法: bash setup/00_install_stack.sh
set -euo pipefail
OPT="/home/qqrtq/opt"
mkdir -p "$OPT"

# 1) Hadoop 3.3.6（HDFS 守护进程 + hdfs CLI）
if [ ! -d "$OPT/hadoop-3.3.6" ]; then
  echo "[install] downloading Hadoop 3.3.6 ..."
  curl -L -C - --retry 6 --retry-delay 3 -o "$OPT/hadoop-3.3.6.tar.gz" \
    https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
  tar -xzf "$OPT/hadoop-3.3.6.tar.gz" -C "$OPT"
fi

# 2) Spark 3.5.8 tarball（可选；若不用 lab_env 的 pyspark 时的备选）
# if [ ! -d "$OPT/spark-3.5.8-bin-hadoop3" ]; then
#   curl -L -C - -o "$OPT/spark-3.5.8-bin-hadoop3.tgz" \
#     https://dlcdn.apache.org/spark/spark-3.5.8/spark-3.5.8-bin-hadoop3.tgz
#   tar -xzf "$OPT/spark-3.5.8-bin-hadoop3.tgz" -C "$OPT"
# fi

# 3) 引导 pip（Debian 默认无 ensurepip 时）
if ! python3 -m pip --version >/dev/null 2>&1; then
  echo "[install] bootstrapping pip ..."
  curl -sS -L -o "$OPT/get-pip.py" https://bootstrap.pypa.io/get-pip.py
  python3 "$OPT/get-pip.py" --user
fi

echo "[install] done. 下一步: bash setup/01_start_hdfs.sh && bash setup/02_load_data.sh"
