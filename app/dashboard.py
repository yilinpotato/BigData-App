"""Steam 大数据分析交互式仪表盘 · 团队罗马 (Roma)

运行: streamlit run app/dashboard.py
数据: app/data/*.csv（由 src/export_dashboard_data.py 用 Spark 从 HDFS 预计算导出）。
运行时仅依赖 pandas + streamlit + altair，可部署到 Streamlit Community Cloud。
"""
import os, json
import pandas as pd
import streamlit as st
import altair as alt

DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
def load(name): return pd.read_csv(os.path.join(DATA, f"{name}.csv"))
with open(os.path.join(DATA, "summary.json"), encoding="utf-8") as f:
    S = json.load(f)

st.set_page_config(page_title="Steam 大数据分析 · 团队罗马", page_icon="🎮", layout="wide")

# ---------- 侧边栏 ----------
with st.sidebar:
    st.title("🎮 团队 罗马 (Roma)")
    st.markdown("**成员**：罗景楠 · 马亦麟")
    st.markdown("**课程**：大数据应用 期末项目")
    st.markdown("---")
    st.markdown("**技术栈**\n\nHDFS · PySpark · Spark SQL · Streamlit")
    st.markdown("[GitHub 仓库](https://github.com/yilinpotato/BigData-App)")
    st.caption("数据：Kaggle Steam Store Games（约 2.7 万款游戏）")

# ---------- 标题 + 概览 ----------
st.title("Steam 游戏商店大数据分析")
st.markdown("基于 **HDFS + Spark** 处理约 2.7 万款 Steam 游戏，交互展示 8 个分析维度。")

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("游戏总数", f"{S['total_games']:,}")
c2.metric("免费 / 付费", f"{S['free_games']:,} / {S['paid_games']:,}")
c3.metric("平均好评率", f"{S['avg_positive_ratio']*100:.1f}%")
c4.metric("中位价格", f"£{S['median_price_gbp']}")
c5.metric("Mac / Linux 支持", f"{S['mac_pct']*100:.0f}% / {S['linux_pct']*100:.0f}%")
st.markdown("---")

tabs = st.tabs(["Q1 发行趋势", "Q2 类型", "Q3 价格", "Q4 时长",
                "Q5 厂商", "Q6 平台", "Q7 标签", "Q8 成就"])

# Q1
with tabs[0]:
    st.subheader("年度游戏发行趋势")
    d = load("q1_release")
    st.altair_chart(alt.Chart(d).mark_bar().encode(
        x=alt.X("release_year:O", title="发行年份"),
        y=alt.Y("n_games:Q", title="游戏数量"),
        tooltip=["release_year", "n_games"]).properties(height=380), width="stretch")
    st.info("2014 年后独立游戏井喷，2018 年达峰值约 8,159 款；93% 的游戏在 2014 年及以后发行。")

# Q2
with tabs[1]:
    st.subheader("热门类型数量 vs 平均好评率")
    d = load("q2_genre")
    base = alt.Chart(d).encode(x=alt.X("genre:N", sort="-y", title="类型"))
    bar = base.mark_bar(color="#9ecae1").encode(y=alt.Y("n:Q", title="游戏数量"), tooltip=["genre", "n", "avg_ratio"])
    line = base.mark_line(point=True, color="#d62728").encode(y=alt.Y("avg_ratio:Q", title="平均好评率", scale=alt.Scale(domain=[0, 1])))
    st.altair_chart(alt.layer(bar, line).resolve_scale(y="independent").properties(height=380), width="stretch")
    st.info("Indie/Action/Casual/Adventure 数量领先；各类型平均好评率集中在 0.63–0.73，数量多≠口碑好。")

# Q3
with tabs[2]:
    st.subheader("价格区间 × 好评率 / 拥有量")
    d = load("q3_price"); order = ['0 Free', '<5', '5-10', '10-20', '20-40', '40+']
    base = alt.Chart(d).encode(x=alt.X("price_band:N", sort=order, title="价格区间 (£)"))
    bar = base.mark_bar(color="#9ecae1").encode(y=alt.Y("avg_ratio:Q", title="平均好评率", scale=alt.Scale(domain=[0, 1])), tooltip=["price_band", "n", "avg_ratio", "avg_owners"])
    line = base.mark_line(point=True, color="#d62728").encode(y=alt.Y("avg_owners:Q", title="平均拥有量(估)"))
    st.altair_chart(alt.layer(bar, line).resolve_scale(y="independent").properties(height=380), width="stretch")
    st.info("最低价区(<£5)好评率最低(0.684)、拥有量也最低；中端(£10–40)口碑最佳；免费与精品拥有量最大——低价≠走量。")

# Q4
with tabs[3]:
    st.subheader("各类型平均游玩时长 Top 12（粘性）")
    d = load("q4_playtime")
    st.altair_chart(alt.Chart(d).mark_bar(color="#6a51a3").encode(
        x=alt.X("avg_hours:Q", title="平均游玩时长(小时)"),
        y=alt.Y("genre:N", sort="-x", title="类型"),
        tooltip=["genre", "avg_hours", "n"]).properties(height=420), width="stretch")
    st.info("MMO(≈17.6h)、免费网游、RPG、策略类粘性最强，依赖多人/养成等可重复游玩机制。")

# Q5
with tabs[4]:
    st.subheader("Top 开发商 / 发行商")
    a, b = st.columns(2)
    dev = load("q5_dev"); pub = load("q5_pub")
    a.altair_chart(alt.Chart(dev).mark_bar(color="#2ca02c").encode(
        x=alt.X("n:Q", title="游戏数"), y=alt.Y("developer:N", sort="-x", title=None),
        tooltip=["developer", "n"]).properties(height=380, title="发行游戏数最多"), width="stretch")
    b.altair_chart(alt.Chart(pub).mark_bar(color="#ff7f0e").encode(
        x=alt.X("pos:Q", title="累计好评数"), y=alt.Y("publisher:N", sort="-x", title=None),
        tooltip=["publisher", "pos"]).properties(height=380, title="累计好评最高"), width="stretch")
    st.info("量产工作室游戏数多；而累计口碑高度集中于 Valve（527 万好评）等头部发行商——数量≠影响力。")

# Q6
with tabs[5]:
    st.subheader("Mac / Linux 平台支持率随年份变化")
    d = load("q6_platform").melt("release_year", ["mac", "linux"], "platform", "rate")
    st.altair_chart(alt.Chart(d).mark_line(point=True).encode(
        x=alt.X("release_year:O", title="发行年份"),
        y=alt.Y("rate:Q", title="支持比例", axis=alt.Axis(format="%")),
        color=alt.Color("platform:N", title="平台"),
        tooltip=["release_year", "platform", alt.Tooltip("rate:Q", format=".1%")]).properties(height=380), width="stretch")
    st.info("Windows≈100%；Mac/Linux 支持率在 2013–2015(SteamOS 推广期)见顶后回落——与厂商战略强相关。")

# Q7
with tabs[6]:
    st.subheader("最受欢迎标签 Top 20")
    d = load("q7_tags")
    st.altair_chart(alt.Chart(d).mark_bar(color="#1f77b4").encode(
        x=alt.X("total_votes:Q", title="标签总投票数"),
        y=alt.Y("tag:N", sort="-x", title="标签"),
        tooltip=["tag", "total_votes", "n_games"]).properties(height=520), width="stretch")
    st.info("action/indie/adventure/multiplayer/singleplayer 投票最高，与类型分布互相印证。")

# Q8
with tabs[7]:
    st.subheader("成就数量 × 好评率 / 时长")
    d = load("q8_ach"); order = ['0', '1-10', '11-50', '51-100', '100+']
    base = alt.Chart(d).encode(x=alt.X("ach_band:N", sort=order, title="成就数量区间"))
    bar = base.mark_bar(color="#9ecae1").encode(y=alt.Y("avg_ratio:Q", title="平均好评率", scale=alt.Scale(domain=[0, 1])), tooltip=["ach_band", "n", "avg_ratio", "avg_hours"])
    line = base.mark_line(point=True, color="#d62728").encode(y=alt.Y("avg_hours:Q", title="平均时长(小时)"))
    st.altair_chart(alt.layer(bar, line).resolve_scale(y="independent").properties(height=380), width="stretch")
    st.info("有成就的游戏好评率(0.74–0.78)高于无成就(0.704)，51–100 个时最高；时长随成就数上升。")

st.markdown("---")
st.caption("© 团队罗马 · 数据 2019 快照 · 由 Spark/HDFS 预计算，Streamlit 呈现")
