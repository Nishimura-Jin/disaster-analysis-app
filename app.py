import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import altair as alt
import folium
import pandas as pd
import requests
import streamlit as st
from streamlit_folium import st_folium

APP_TZ = ZoneInfo("Asia/Tokyo")
NOW = datetime.now(APP_TZ)
TODAY = NOW.date()
TARGET_DATE = (TODAY - timedelta(days=1)).isoformat()

DB_PATH = Path(__file__).resolve().parent / "disaster.db"

JMA_URL = "https://www.jma.go.jp/bosai/warning/data/warning/map.json"
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

EVENT_MAP = {
    "14": "大雨",
    "15": "洪水",
    "16": "暴風",
    "17": "大雪",
    "18": "波浪",
    "19": "高潮",
    "20": "雷",
    "21": "強風",
    "22": "乾燥",
    "23": "なだれ",
    "24": "低温",
    "25": "霜",
}

REGION_MAP = {
    "01": "北海道",
    "13": "東京都",
    "23": "愛知県",
    "27": "大阪府",
    "40": "福岡県",
    "47": "沖縄県",
}

PREF_COORDS = {
    "北海道": (43.0642, 141.3469),
    "東京都": (35.6895, 139.6917),
    "大阪府": (34.6937, 135.5023),
    "愛知県": (35.1802, 136.9066),
    "福岡県": (33.5902, 130.4017),
    "沖縄県": (26.2124, 127.6809),
}


# ================= DB =================
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(
            """
        CREATE TABLE IF NOT EXISTS disaster (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observed_date TEXT,
            snapshot_time TEXT,
            region TEXT,
            event_type TEXT,
            status TEXT,
            severity INTEGER
        );
        CREATE TABLE IF NOT EXISTS weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observed_date TEXT,
            region TEXT,
            temperature_max REAL,
            precipitation_sum REAL,
            fetched_at TEXT
        );
        """
        )


init_db()


# ================= データ取得 =================
def fetch_disaster():
    try:
        res = requests.get(JMA_URL, timeout=20)
        res.raise_for_status()
    except Exception as e:
        st.error(f"災害データ取得失敗: {e}")
        return []

    snapshot = NOW.isoformat()
    records = []

    for report in res.json():
        for at in report.get("areaTypes", []):
            for area in at.get("areas", []):
                region = REGION_MAP.get(str(area.get("code"))[:2])
                if not region:
                    continue

                for w in area.get("warnings", []):
                    if w.get("status") == "解除":
                        continue

                    event = EVENT_MAP.get(str(w.get("code")))
                    if event:
                        records.append(
                            (TARGET_DATE, snapshot, region, event, w.get("status"), 1)
                        )
    return records


def fetch_weather():
    start = (TODAY - timedelta(days=8)).isoformat()
    end = TARGET_DATE
    records = []

    for region, (lat, lon) in PREF_COORDS.items():
        try:
            res = requests.get(
                OPEN_METEO_URL,
                params={
                    "latitude": lat,
                    "longitude": lon,
                    "start_date": start,
                    "end_date": end,
                    "daily": "temperature_2m_max,precipitation_sum",
                    "timezone": "Asia/Tokyo",
                },
                timeout=20,
            )
            res.raise_for_status()
            daily = res.json().get("daily", {})
            if not daily.get("time"):
                continue
        except Exception:
            continue

        for d, t, p in zip(
            daily["time"], daily["temperature_2m_max"], daily["precipitation_sum"]
        ):
            records.append((d, region, float(t or 0), float(p or 0), NOW.isoformat()))
    return records


def save_data(disaster, weather):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("BEGIN")
            conn.execute("DELETE FROM disaster WHERE observed_date = ?", (TARGET_DATE,))
            conn.execute("DELETE FROM weather WHERE observed_date >= ?", (TARGET_DATE,))

            if disaster:
                conn.executemany(
                    "INSERT INTO disaster VALUES (NULL,?,?,?,?,?,?)", disaster
                )
            if weather:
                conn.executemany("INSERT INTO weather VALUES (NULL,?,?,?,?,?)", weather)

            conn.commit()
    except Exception as e:
        st.error(f"DB更新失敗: {e}")


# ================= 分析 =================
def classify(score):
    return "危険" if score >= 7 else "注意" if score >= 3 else "安全"


def calc_scores(df):
    df = df.copy()

    df["temperature_max"] = pd.to_numeric(
        df["temperature_max"], errors="coerce"
    ).fillna(0)
    df["precipitation_sum"] = pd.to_numeric(
        df["precipitation_sum"], errors="coerce"
    ).fillna(0)
    df["warning_count"] = pd.to_numeric(df["warning_count"], errors="coerce").fillna(0)

    df["temp_score"] = (df["temperature_max"] >= 35) * 3 + (
        (df["temperature_max"] >= 30) & (df["temperature_max"] < 35)
    ) * 2
    df["rain_score"] = (df["precipitation_sum"] >= 50) * 3 + (
        (df["precipitation_sum"] >= 20) & (df["precipitation_sum"] < 50)
    ) * 2
    df["warn_score"] = (df["warning_count"] > 0) * 3

    df["risk_score"] = df["temp_score"] + df["rain_score"] + df["warn_score"]
    df["risk_level"] = df["risk_score"].apply(classify)

    return df


def build_risk_df(w_df, d_df):
    warning = (
        d_df.groupby(["observed_date", "region"])
        .size()
        .reset_index(name="warning_count")
        if not d_df.empty
        else pd.DataFrame(columns=["observed_date", "region", "warning_count"])
    )

    df = w_df.merge(warning, on=["observed_date", "region"], how="left")
    df["warning_count"] = df["warning_count"].fillna(0)

    return calc_scores(df).drop_duplicates(["observed_date", "region"])


# ================= UI =================
st.title("気象・災害リスク分析ダッシュボード")

if st.button("最新データを取得"):
    d, w = fetch_disaster(), fetch_weather()
    save_data(d, w)
    st.success(f"取得完了：災害 {len(d)}件 / 気象 {len(w)}件")

st.caption(f"最終更新: {NOW.strftime('%Y-%m-%d %H:%M:%S')}")

with sqlite3.connect(DB_PATH) as conn:
    weather_df = pd.read_sql("SELECT * FROM weather", conn)
    disaster_df = pd.read_sql("SELECT * FROM disaster", conn)

if weather_df.empty:
    st.warning("データがありません")
    st.stop()

risk_df = build_risk_df(weather_df, disaster_df)
latest = risk_df[risk_df["observed_date"] == risk_df["observed_date"].max()]


# ================= 警報 =================
st.subheader("現在の警報")

for region in PREF_COORDS.keys():
    d = disaster_df[
        (disaster_df["region"] == region) & (disaster_df["status"] != "解除")
    ]
    if not d.empty:
        latest_warn = d.sort_values("snapshot_time", ascending=False).iloc[0]

        if "特別" in latest_warn["event_type"] or latest_warn["status"] == "発表":
            st.error(
                f"{region}：{latest_warn['event_type']}（{latest_warn['status']}）"
            )
        else:
            st.warning(
                f"{region}：{latest_warn['event_type']}（{latest_warn['status']}）"
            )


# ================= ランキング（色付き） =================
st.subheader("危険度ランキング")


def color_risk(val):
    if val >= 7:
        return "background-color: red; color: white;"
    elif val >= 3:
        return "background-color: orange;"
    return "background-color: lightgreen;"


rank = latest.sort_values("risk_score", ascending=False)

styled = rank.rename(
    columns={
        "region": "地域",
        "risk_score": "リスク",
        "risk_level": "危険度",
        "temp_score": "気温",
        "rain_score": "降水",
        "warn_score": "警報",
    }
)[["地域", "リスク", "危険度", "気温", "降水", "警報"]]

st.dataframe(styled.style.applymap(color_risk, subset=["リスク"]), hide_index=True)


# ================= グラフ =================
st.subheader("地域推移")

regions = sorted(risk_df["region"].unique())
default_index = regions.index("大阪府") if "大阪府" in regions else 0
sel = st.selectbox("地域選択", regions, index=default_index)

chart_df = risk_df[risk_df["region"] == sel].copy()
chart_df["observed_date"] = pd.to_datetime(chart_df["observed_date"])

# ================= 最高気温 =================
st.markdown("### 最高気温の推移")

temp_chart = (
    alt.Chart(chart_df)
    .mark_line()
    .encode(
        x=alt.X(
            "observed_date:T", title="日付", axis=alt.Axis(format="%m/%d", labelAngle=0)
        ),
        y=alt.Y("temperature_max:Q", title="最高気温(℃)"),
        tooltip=[
            alt.Tooltip("observed_date:T", title="日付", format="%Y-%m-%d"),
            alt.Tooltip("temperature_max:Q", title="気温"),
        ],
    )
)

st.altair_chart(temp_chart, use_container_width=True)

# ================= 降水量 =================
st.markdown("### 降水量の推移")

rain_chart = (
    alt.Chart(chart_df)
    .mark_line(color="blue")
    .encode(
        x=alt.X(
            "observed_date:T", title="日付", axis=alt.Axis(format="%m/%d", labelAngle=0)
        ),
        y=alt.Y("precipitation_sum:Q", title="降水量(mm)"),
        tooltip=[
            alt.Tooltip("observed_date:T", title="日付", format="%Y-%m-%d"),
            alt.Tooltip("precipitation_sum:Q", title="降水量"),
        ],
    )
)

st.altair_chart(rain_chart, use_container_width=True)

# ================= リスク =================
st.markdown("### リスクスコアの推移")

risk_chart = (
    alt.Chart(chart_df)
    .mark_line(color="red")
    .encode(
        x=alt.X(
            "observed_date:T", title="日付", axis=alt.Axis(format="%m/%d", labelAngle=0)
        ),
        y=alt.Y("risk_score:Q", title="リスクスコア"),
        tooltip=[
            alt.Tooltip("observed_date:T", title="日付", format="%Y-%m-%d"),
            alt.Tooltip("risk_score:Q", title="リスク"),
        ],
    )
)

st.altair_chart(risk_chart, use_container_width=True)

# ================= 履歴 =================
st.subheader("災害履歴分析")
hist = disaster_df.groupby("region").size().reset_index(name="警報回数")
st.bar_chart(hist.set_index("region"))


# ================= 地図 =================
st.subheader("日本地図")

m = folium.Map(location=[36, 138], zoom_start=5)

for _, r in latest.iterrows():
    if r["region"] not in PREF_COORDS:
        continue

    color = (
        "red" if r["risk_score"] >= 7 else "orange" if r["risk_score"] >= 3 else "green"
    )

    popup_html = f"""
    <b>{r['region']}</b><br>
    危険度：{r['risk_level']}<br>
    警報数：{int(r['warning_count'])}
    """

    folium.CircleMarker(
        location=PREF_COORDS[r["region"]],
        radius=12 if r["warning_count"] > 0 else 6,
        color=color,
        fill=True,
        popup=folium.Popup(popup_html, max_width=250),
    ).add_to(m)

st_folium(m, width=900, height=500)


# ================= 説明 =================
st.subheader("このアプリについて")
st.markdown(
    """
気象データ（Open-Meteo）と警報データ（気象庁）を基に、
気温・降水量・警報の有無からリスクをスコア化しています。
※簡易的な閾値モデルです
"""
)
