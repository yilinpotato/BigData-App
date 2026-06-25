# 环境搭建 / Environment Setup

单机大数据栈，**无需 sudo**，全部用户级安装。

## 组件版本

| 组件 | 版本 | 来源 |
|---|---|---|
| Java | OpenJDK 17 (Temurin) | `~/jdk` (预装) |
| HDFS | Hadoop 3.3.6（单机伪分布式） | `~/opt/hadoop-3.3.6`（tarball） |
| Spark / PySpark | 3.5.5 | `~/lab_env`（venv，含 pandas/matplotlib/seaborn/JupyterLab） |

## 一键启动

```bash
# 0) 安装栈（首次；下载 Hadoop tarball，引导 pip）
bash setup/00_install_stack.sh

# 1) 启动单机 HDFS（首次自动格式化 NameNode；不依赖 SSH）
bash setup/01_start_hdfs.sh

# 2) 把 dataset/*.csv 上传到 HDFS:///steam/raw
bash setup/02_load_data.sh

# 3) 之后任何会话先加载环境变量
source setup/env.sh
```

启动后：
- NameNode Web UI: http://localhost:9870
- HDFS 默认地址: `hdfs://localhost:9000`
- 原始数据: `hdfs:///steam/raw/*.csv`

## 关键设计说明

- **不走 `start-dfs.sh`**：直接 `hdfs --daemon start namenode/datanode`，避免 WSL 下对 SSH 到 localhost 的依赖（无需 sshd / 免密钥）。
- **Java 17 兼容**：在 `hadoop-env.sh` 为 NameNode/DataNode 注入 `--add-opens`，解决 JPMS 反射访问限制。
- **空格路径规避**：仓库目录名含空格（`bigdata app`），Hadoop FsShell 处理本地空格路径有 URI 问题；`02_load_data.sh` 用硬链接把 CSV 暴露到无空格目录再 `put`。
- **Spark 运行时复用 `lab_env`**：与课程已有 venv 一致，自带可视化与 Jupyter，避免重复安装。

## 停止 HDFS

```bash
source setup/env.sh
hdfs --daemon stop datanode
hdfs --daemon stop namenode
```
