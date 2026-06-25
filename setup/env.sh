#!/usr/bin/env bash
# 大数据栈环境变量 / Big-data stack environment.
# 用法: source setup/env.sh
# 如在其它机器运行，按需修改下面 4 个根路径即可。

export JAVA_HOME="/home/qqrtq/jdk"                                  # Java 17 (Temurin)
export HADOOP_HOME="/home/qqrtq/opt/hadoop-3.3.6"                   # Hadoop 3.3.6 (HDFS 守护进程)
export LAB_ENV="/home/qqrtq/lab_env"                               # 已有 venv: pyspark 3.5.5 + 数据栈
export SPARK_HOME="$LAB_ENV/lib/python3.10/site-packages/pyspark"  # Spark 运行时来自 pyspark 包
export HADOOP_DATA="/home/qqrtq/hadoopdata"                         # HDFS 本地落盘目录

export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$LAB_ENV/bin:$SPARK_HOME/bin:$HOME/.local/bin:$PATH"

# Spark / PySpark 统一用 lab_env 的 python（含 pandas/matplotlib/seaborn/jupyter）
export PYSPARK_PYTHON="$LAB_ENV/bin/python"
export PYSPARK_DRIVER_PYTHON="$LAB_ENV/bin/python"

# 让 Spark 能在 Java 17 下访问内部模块（Spark 3.5 大多已内置，这里兜底）
export SPARK_SUBMIT_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

echo "[env] JAVA_HOME=$JAVA_HOME"
echo "[env] HADOOP_HOME=$HADOOP_HOME"
echo "[env] SPARK_HOME=$SPARK_HOME"
