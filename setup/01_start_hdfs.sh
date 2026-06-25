#!/usr/bin/env bash
# 启动单机伪分布式 HDFS（首次会自动格式化）。无需 sudo / SSH。
# 用法: bash setup/01_start_hdfs.sh
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/env.sh"

# 1) 应用项目自带的 Hadoop 配置
cp "$HERE/hadoop-conf/core-site.xml" "$HADOOP_CONF_DIR/core-site.xml"
cp "$HERE/hadoop-conf/hdfs-site.xml" "$HADOOP_CONF_DIR/hdfs-site.xml"

# 2) 在 hadoop-env.sh 写入 JAVA_HOME 和 Java 17 所需的 --add-opens（幂等）
ENVSH="$HADOOP_CONF_DIR/hadoop-env.sh"
if ! grep -q "BIGDATA-APP-CONFIG" "$ENVSH" 2>/dev/null; then
cat >> "$ENVSH" <<EOF

# === BIGDATA-APP-CONFIG (Java 17 compatibility) ===
export JAVA_HOME=$JAVA_HOME
export HDFS_NAMENODE_OPTS="\${HDFS_NAMENODE_OPTS} --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
export HDFS_DATANODE_OPTS="\${HDFS_DATANODE_OPTS} --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
EOF
fi

# 3) 首次格式化 NameNode（已格式化则跳过）
mkdir -p "$HADOOP_DATA/nn" "$HADOOP_DATA/dn" "$HADOOP_DATA/tmp"
if [ ! -d "$HADOOP_DATA/nn/current" ]; then
  echo "[hdfs] formatting namenode ..."
  hdfs namenode -format -force -nonInteractive
fi

# 4) 直接以 daemon 方式启动（不走 start-dfs.sh，避免 SSH 依赖）
hdfs --daemon start namenode
hdfs --daemon start datanode
sleep 5

echo "=== jps ==="; jps || true
echo "=== HDFS report ==="; hdfs dfsadmin -report 2>/dev/null | head -15 || true
echo "[hdfs] NameNode UI: http://localhost:9870"
