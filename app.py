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


def now():
    return datetime.now(APP_TZ)


TODAY = now().date()
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
    "02": "青森県",
    "03": "岩手県",
    "04": "宮城県",
    "05": "秋田県",
    "06": "山形県",
    "07": "福島県",
    "08": "茨城県",
    "09": "栃木県",
    "10": "群馬県",
    "11": "埼玉県",
    "12": "千葉県",
    "13": "東京都",
    "14": "神奈川県",
    "15": "新潟県",
    "16": "富山県",
    "17": "石川県",
    "18": "福井県",
    "19": "山梨県",
    "20": "長野県",
    "21": "岐阜県",
    "22": "静岡県",
    "23": "愛知県",
    "24": "三重県",
    "25": "滋賀県",
    "26": "京都府",
    "27": "大阪府",
    "28": "兵庫県",
    "29": "奈良県",
    "30": "和歌山県",
    "31": "鳥取県",
    "32": "島根県",
    "33": "岡山県",
    "34": "広島県",
    "35": "山口県",
    "36": "徳島県",
    "37": "香川県",
    "38": "愛媛県",
    "39": "高知県",
    "40": "福岡県",
    "41": "佐賀県",
    "42": "長崎県",
    "43": "熊本県",
    "44": "大分県",
    "45": "宮崎県",
    "46": "鹿児島県",
    "47": "沖縄県",
}

PREF_COORDS = {
    "北海道": (43.0642, 141.3469),
    "青森県": (40.8244, 140.74),
    "岩手県": (39.7036, 141.1527),
    "宮城県": (38.2682, 140.8694),
    "秋田県": (39.7186, 140.1024),
    "山形県": (38.2404, 140.3633),
    "福島県": (37.7503, 140.4676),
    "茨城県": (36.3418, 140.4468),
    "栃木県": (36.5658, 139.8836),
    "群馬県": (36.3912, 139.0606),
    "埼玉県": (35.857, 139.6489),
    "千葉県": (35.605, 140.1233),
    "東京都": (35.6895, 139.6917),
    "神奈川県": (35.4478, 139.6425),
    "新潟県": (37.9024, 139.0232),
    "富山県": (36.6953, 137.2113),
    "石川県": (36.5944, 136.6256),
    "福井県": (36.0652, 136.2216),
    "山梨県": (35.6639, 138.5683),
    "長野県": (36.6513, 138.181),
    "岐阜県": (35.3912, 136.7223),
    "静岡県": (34.9769, 138.3831),
    "愛知県": (35.1802, 136.9066),
    "三重県": (34.7303, 136.5086),
    "滋賀県": (35.0045, 135.8686),
    "京都府": (35.0214, 135.7556),
    "大阪府": (34.6937, 135.5023),
    "兵庫県": (34.6913, 135.183),
    "奈良県": (34.6851, 135.8328),
    "和歌山県": (34.226, 135.1675),
    "鳥取県": (35.5039, 134.2383),
    "島根県": (35.4723, 133.0505),
    "岡山県": (34.6618, 133.935),
    "広島県": (34.3963, 132.4596),
    "山口県": (34.1859, 131.4714),
    "徳島県": (34.0658, 134.5593),
    "香川県": (34.3401, 134.0434),
    "愛媛県": (33.8416, 132.7657),
    "高知県": (33.5597, 133.5311),
    "福岡県": (33.5902, 130.4017),
    "佐賀県": (33.2494, 130.2988),
    "長崎県": (32.7448, 129.8737),
    "熊本県": (32.7898, 130.7417),
    "大分県": (33.2382, 131.6126),
    "宮崎県": (31.9111, 131.4239),
    "鹿児島県": (31.5602, 130.5581),
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

    snapshot = now().isoformat()
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
        except Exception as e:
            st.warning(f"{region} の気象データ取得失敗: {e}")
            continue

        for d, t, p in zip(
            daily["time"], daily["temperature_2m_max"], daily["precipitation_sum"]
        ):
            records.append((d, region, float(t or 0), float(p or 0), now().isoformat()))
    return records


def save_data(disaster, weather):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("BEGIN")
        conn.execute("DELETE FROM disaster WHERE observed_date = ?", (TARGET_DATE,))
        conn.execute("DELETE FROM weather WHERE observed_date >= ?", (TARGET_DATE,))

        if disaster:
            conn.executemany("INSERT INTO disaster VALUES (NULL,?,?,?,?,?,?)", disaster)
        if weather:
            conn.executemany("INSERT INTO weather VALUES (NULL,?,?,?,?,?)", weather)

        conn.commit()


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
    df["warning_count"] = pd.to_numeric(
        df.get("warning_count", 0), errors="coerce"
    ).fillna(0)

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


# ================= リスクに応じたコメント生成 =================
def generate_risk_comment(row):
    """リスクスコアの内訳をもとに状況を説明するコメントを生成する"""
    parts = []

    if row["temp_score"] >= 3:
        parts.append("猛暑（35℃以上）")
    elif row["temp_score"] >= 2:
        parts.append("高温（30℃以上）")

    if row["rain_score"] >= 3:
        parts.append("大雨（50mm以上）")
    elif row["rain_score"] >= 2:
        parts.append("強雨（20mm以上）")

    if row["warn_score"] > 0:
        parts.append("気象警報発令中")

    if not parts:
        return "現在リスク要因はありません"

    return "・".join(parts) + " によりリスクが上昇しています"


# ================= UI =================
st.title("災害リスク分析ツール")

if st.button("最新データを取得"):
    d, w = fetch_disaster(), fetch_weather()
    save_data(d, w)
    st.success(f"取得完了：災害 {len(d)}件 / 気象 {len(w)}件")

st.caption(f"最終更新: {now().strftime('%Y-%m-%d %H:%M:%S')}")

with sqlite3.connect(DB_PATH) as conn:
    weather_df = pd.read_sql("SELECT * FROM weather", conn)
    disaster_df = pd.read_sql("SELECT * FROM disaster", conn)

if weather_df.empty:
    st.warning("データがありません")
    st.stop()

risk_df = build_risk_df(weather_df, disaster_df)
latest = risk_df[risk_df["observed_date"] == risk_df["observed_date"].max()]
all_regions = pd.DataFrame({"region": list(PREF_COORDS.keys())})

latest = all_regions.merge(latest, on="region", how="left")

latest["risk_score"] = latest["risk_score"].fillna(0)
latest["temp_score"] = latest["temp_score"].fillna(0)
latest["rain_score"] = latest["rain_score"].fillna(0)
latest["warn_score"] = latest["warn_score"].fillna(0)
latest["warning_count"] = latest["warning_count"].fillna(0)
latest["risk_level"] = latest["risk_level"].fillna("安全")

rank = latest.sort_values("risk_score", ascending=False)

# ================= サマリー =================
st.header("サマリー")

if rank.empty:
    st.warning("データが不足しています")
    st.stop()

top = rank.iloc[0]

st.info(
    f"""
最大リスク地域：{top['region']}（{top['risk_level']}）

▶ 現在の状況：
{generate_risk_comment(top)}

内訳：
・気温スコア：{int(top['temp_score'])}
・降水スコア：{int(top['rain_score'])}
・警報スコア：{int(top['warn_score'])}
・総合リスク：{int(top['risk_score'])}
"""
)

# ================= 警報 =================
st.header("現在の警報")

warned = False
for region in PREF_COORDS.keys():
    d = disaster_df[
        (disaster_df["region"] == region) & (disaster_df["status"] != "解除")
    ]
    if not d.empty:
        latest_warn = d.sort_values("snapshot_time", ascending=False).iloc[0]
        st.warning(f"{region}：{latest_warn['event_type']}（{latest_warn['status']}）")
        warned = True

if not warned:
    st.success("現在、発令中の警報はありません")

# ================= ランキング =================
st.header("リスクランキング")

st.dataframe(
    rank.rename(
        columns={
            "region": "地域",
            "risk_score": "リスク",
            "risk_level": "危険度",
            "temp_score": "気温",
            "rain_score": "降水",
            "warn_score": "警報",
        }
    )[["地域", "リスク", "危険度", "気温", "降水", "警報"]],
    hide_index=True,
)

# ================= グラフ =================
st.header("地域トレンド")

regions = sorted(PREF_COORDS.keys())
default_index = regions.index("大阪府") if "大阪府" in regions else 0
sel = st.selectbox("地域選択", regions, index=default_index)

chart_df = risk_df[risk_df["region"] == sel].copy()
chart_df["observed_date"] = pd.to_datetime(chart_df["observed_date"])

st.subheader(f"直近リスク推移 ／ {sel}")

st.altair_chart(
    alt.Chart(chart_df)
    .mark_line(color="red")
    .encode(
        x=alt.X(
            "observed_date:T",
            title="日付",
            axis=alt.Axis(format="%m/%d", labelAngle=0),
        ),
        y=alt.Y("risk_score:Q", title="リスクスコア"),
        tooltip=[
            alt.Tooltip("observed_date:T", title="日付", format="%Y-%m-%d"),
            alt.Tooltip("risk_score:Q", title="リスクスコア"),
        ],
    ),
    use_container_width=True,
)

# ================= 地図 =================
st.markdown("### 地域ごとのリスク分布（赤いほど危険）")
st.caption("※ 注意以上の地域のみ表示（円の大きさは警報数を表します）")

m = folium.Map(location=[36, 138], zoom_start=5)

for _, r in latest.iterrows():
    if r["risk_score"] < 3:
        continue

    if r["region"] not in PREF_COORDS:
        continue

    color = "red" if r["risk_score"] >= 7 else "orange"

    popup_html = f"""
    <b>{r['region']}</b><br>
    危険度：{r['risk_level']}<br>
    リスク：{int(r['risk_score'])}<br>
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
st.header("仕組み")

st.markdown(
    """
- **JMA**：気象庁から現在の警報情報を取得
- **Open-Meteo**：過去8日分の気象データ（最高気温・降水量）を取得
- **スコア計算**：気温・降水・警報の3指標を数値化して統合評価
    - 気温35℃以上：+3 / 30℃以上：+2
    - 降水50mm以上：+3 / 20mm以上：+2
    - 警報発令中：+3
    - 合計7以上：危険 / 3以上：注意 / 3未満：安全
"""
)
