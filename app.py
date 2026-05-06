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
TODAY = datetime.now(APP_TZ).date()
TARGET_DATE = (TODAY - timedelta(days=1)).isoformat()

DB_PATH = Path(__file__).resolve().parent / "disaster.db"

JMA_URL = "https://www.jma.go.jp/bosai/warning/data/warning/map.json"
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

TEMP_DANGER = 35
TEMP_WARNING = 30
RAIN_DANGER = 50
RAIN_WARNING = 20
RISK_DANGER = 15
RISK_WARNING = 5
RISK_CAUTION = 10
SCORE_MAX = 20
WARN_MIN_SCORE = 5  # 警報あり時の最低スコア
FREEZE_TEMP = 0  # 凍結リスクの気温閾値
TEMP_EXTREME = 38  # 極端な高温閾値

OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

COLOR_DANGER = "#e03131"
COLOR_CAUTION = "#e8590c"
COLOR_WARNING = "#f76707"
COLOR_SAFE = "#2f9e44"

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

BLOCK_MAP = {
    "北海道": "北海道",
    "青森県": "東北",
    "岩手県": "東北",
    "宮城県": "東北",
    "秋田県": "東北",
    "山形県": "東北",
    "福島県": "東北",
    "茨城県": "関東",
    "栃木県": "関東",
    "群馬県": "関東",
    "埼玉県": "関東",
    "千葉県": "関東",
    "東京都": "関東",
    "神奈川県": "関東",
    "新潟県": "中部",
    "富山県": "中部",
    "石川県": "中部",
    "福井県": "中部",
    "山梨県": "中部",
    "長野県": "中部",
    "岐阜県": "中部",
    "静岡県": "中部",
    "愛知県": "中部",
    "三重県": "近畿",
    "滋賀県": "近畿",
    "京都府": "近畿",
    "大阪府": "近畿",
    "兵庫県": "近畿",
    "奈良県": "近畿",
    "和歌山県": "近畿",
    "鳥取県": "中国",
    "島根県": "中国",
    "岡山県": "中国",
    "広島県": "中国",
    "山口県": "中国",
    "徳島県": "四国",
    "香川県": "四国",
    "愛媛県": "四国",
    "高知県": "四国",
    "福岡県": "九州",
    "佐賀県": "九州",
    "長崎県": "九州",
    "熊本県": "九州",
    "大分県": "九州",
    "宮崎県": "九州",
    "鹿児島県": "九州",
    "沖縄県": "沖縄",
}

BLOCK_ORDER = ["北海道", "東北", "関東", "中部", "近畿", "中国", "四国", "九州", "沖縄"]

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


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript("""
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
        """)


init_db()


def fetch_disaster():
    try:
        res = requests.get(JMA_URL, timeout=20)
        res.raise_for_status()
    except Exception as e:
        st.error(f"災害データ取得失敗: {e}")
        return []

    snapshot = datetime.now(APP_TZ).isoformat()
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
    failed = []

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
            failed.append(region)
            continue

        for d, t, p in zip(
            daily["time"], daily["temperature_2m_max"], daily["precipitation_sum"]
        ):
            records.append(
                (
                    d,
                    region,
                    float(t or 0),
                    float(p or 0),
                    datetime.now(APP_TZ).isoformat(),
                )
            )

    if failed:
        st.warning(f"気象データ取得失敗: {', '.join(failed)}")

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


def classify(score):
    if score >= RISK_DANGER:
        return "危険"
    if score >= RISK_CAUTION:
        return "警戒"
    if score >= RISK_WARNING:
        return "注意"
    return "安全"


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

    # 気温スコア（夏：熱中症リスク・冬：凍結リスク）
    df["temp_score"] = (
        (df["temperature_max"] >= TEMP_EXTREME) * 5
        + (
            (df["temperature_max"] >= TEMP_DANGER)
            & (df["temperature_max"] < TEMP_EXTREME)
        )
        * 3
        + (
            (df["temperature_max"] >= TEMP_WARNING)
            & (df["temperature_max"] < TEMP_DANGER)
        )
        * 2
        + ((df["temperature_max"] <= FREEZE_TEMP) & (df["precipitation_sum"] > 0)) * 3
    )

    # 降水スコア
    df["rain_score"] = (df["precipitation_sum"] >= RAIN_DANGER) * 6 + (
        (df["precipitation_sum"] >= RAIN_WARNING)
        & (df["precipitation_sum"] < RAIN_DANGER)
    ) * 3

    # 複合スコア：高温かつ大雨（熱中症・土砂災害の重複リスク）
    df["compound_score"] = (
        (df["temperature_max"] >= TEMP_WARNING)
        & (df["precipitation_sum"] >= RAIN_WARNING)
    ).astype(int) * 3

    # 警報スコア（警報あり時の底上げ）
    df["warn_score"] = (df["warning_count"] > 0).astype(int) * 3

    # 合計スコア（上限クリップ）
    raw_score = (
        df["temp_score"] + df["rain_score"] + df["compound_score"] + df["warn_score"]
    )
    df["risk_score"] = raw_score.clip(upper=SCORE_MAX)

    # 警報あり時は最低スコアを保証
    df.loc[df["warning_count"] > 0, "risk_score"] = df.loc[
        df["warning_count"] > 0, "risk_score"
    ].clip(lower=WARN_MIN_SCORE)

    df["risk_level"] = df["risk_score"].apply(classify)

    return df


def build_risk_df(w_df, d_df):
    # 警報件数を集計（スコアの底上げ補正に使用）
    if not d_df.empty:
        warning = (
            d_df.drop_duplicates(subset=["observed_date", "region", "event_type"])
            .groupby(["observed_date", "region"])
            .size()
            .reset_index(name="warning_count")
        )
    else:
        warning = pd.DataFrame(columns=["observed_date", "region", "warning_count"])

    df = w_df.merge(warning, on=["observed_date", "region"], how="left")
    df["warning_count"] = df["warning_count"].fillna(0)

    return calc_scores(df).drop_duplicates(["observed_date", "region"])


@st.cache_data(ttl=3600)
def fetch_forecast(region, lat, lon):
    """Open-Meteo予報APIで今後7日間のデータを取得"""
    try:
        res = requests.get(
            OPEN_METEO_FORECAST_URL,
            params={
                "latitude": lat,
                "longitude": lon,
                "daily": "temperature_2m_max,precipitation_sum",
                "timezone": "Asia/Tokyo",
                "forecast_days": 7,
            },
            timeout=20,
        )
        res.raise_for_status()
        daily = res.json().get("daily", {})
        if not daily.get("time"):
            return []
    except Exception:
        return []

    records = []
    for d, t, p in zip(
        daily["time"], daily["temperature_2m_max"], daily["precipitation_sum"]
    ):
        records.append(
            {
                "observed_date": d,
                "region": region,
                "temperature_max": float(t or 0),
                "precipitation_sum": float(p or 0),
            }
        )
    return records


def generate_risk_comment(row):
    parts = []

    if row.get("temp_score", 0) >= 5:
        parts.append(f"極端な高温（{TEMP_EXTREME}℃以上）")
    elif row.get("temp_score", 0) >= 3:
        parts.append(f"猛暑（{TEMP_DANGER}℃以上）")
    elif row.get("temp_score", 0) >= 2:
        parts.append(f"高温（{TEMP_WARNING}℃以上）")
    elif row.get("temp_score", 0) >= 3 and row.get("precipitation_sum", 0) > 0:
        parts.append("凍結・積雪リスク（0℃以下）")

    if row.get("rain_score", 0) >= 6:
        parts.append(f"大雨（{RAIN_DANGER}mm以上）")
    elif row.get("rain_score", 0) >= 3:
        parts.append(f"強雨（{RAIN_WARNING}mm以上）")

    if row.get("compound_score", 0) > 0:
        parts.append("高温・大雨の複合リスク")

    if row.get("warn_score", 0) > 0:
        parts.append("気象警報発令中")

    if not parts:
        return "現在リスク要因はありません"

    return "・".join(parts) + " によりリスクが上昇しています"


STATUS_ORDER = {"継続": 0, "発表": 1, "警報から注意報へ移行": 2}


# ページ設定
st.set_page_config(page_title="災害リスク分析ツール", layout="wide")
st.title("災害リスク分析ツール")

# 初回自動取得
with sqlite3.connect(DB_PATH) as conn:
    _check = pd.read_sql("SELECT COUNT(*) as cnt FROM weather", conn)

if _check["cnt"].iloc[0] == 0:
    with st.spinner("初回データを取得しています..."):
        d, w = fetch_disaster(), fetch_weather()
        save_data(d, w)
    st.success(f"初回データ取得完了：災害 {len(d)}件 / 気象 {len(w)}件")

col_btn, col_cap = st.columns([1, 4])
with col_btn:
    if st.button("最新データを取得"):
        d, w = fetch_disaster(), fetch_weather()
        save_data(d, w)
        st.success(f"取得完了：災害 {len(d)}件 / 気象 {len(w)}件")
with col_cap:
    st.caption(f"最終更新: {datetime.now(APP_TZ).strftime('%Y-%m-%d %H:%M:%S')}")

with sqlite3.connect(DB_PATH) as conn:
    weather_df = pd.read_sql("SELECT * FROM weather", conn)
    disaster_df = pd.read_sql("SELECT * FROM disaster", conn)

if weather_df.empty:
    st.warning("データがありません。「最新データを取得」ボタンを押してください。")
    st.stop()

risk_df = build_risk_df(weather_df, disaster_df)
risk_df["block"] = risk_df["region"].map(BLOCK_MAP)

latest = risk_df[risk_df["observed_date"] == risk_df["observed_date"].max()]
all_regions = pd.DataFrame({"region": list(PREF_COORDS.keys())})
latest = all_regions.merge(latest, on="region", how="left")

latest["risk_score"] = latest["risk_score"].fillna(0)
latest["temp_score"] = latest["temp_score"].fillna(0)
latest["rain_score"] = latest["rain_score"].fillna(0)
latest["warning_count"] = latest["warning_count"].fillna(0)
latest["temperature_max"] = latest["temperature_max"].fillna(0)
latest["precipitation_sum"] = latest["precipitation_sum"].fillna(0)
latest["warn_score"] = latest["warn_score"].fillna(0)
latest["compound_score"] = latest["compound_score"].fillna(0)
latest["risk_level"] = latest["risk_level"].fillna("安全")
latest["block"] = latest["region"].map(BLOCK_MAP)

rank = latest.sort_values("risk_score", ascending=False)

n_danger = int((rank["risk_level"] == "危険").sum())
n_caution = int((rank["risk_level"] == "警戒").sum())
n_warning = int((rank["risk_level"] == "注意").sum())
n_safe = int((rank["risk_level"] == "安全").sum())

# タブ構成
tab1, tab2, tab3, tab4 = st.tabs(["概要", "警報", "地域分析", "気象予測"])

# ───────────────────────────────
# タブ1：概要
# ───────────────────────────────
with tab1:
    if rank.empty:
        st.warning("データが不足しています")
        st.stop()

    top = rank.iloc[0]

    # 最大リスク地域
    level_color = (
        COLOR_DANGER
        if top["risk_level"] == "危険"
        else (
            COLOR_CAUTION
            if top["risk_level"] == "警戒"
            else COLOR_WARNING if top["risk_level"] == "注意" else COLOR_SAFE
        )
    )
    st.markdown(
        f"<h3 style='color:{level_color}'>最大リスク地域：{top['region']}（{top['risk_level']}）</h3>",
        unsafe_allow_html=True,
    )
    st.markdown(f"**{generate_risk_comment(top)}**")
    st.caption(
        f"※ 気象データは昨日時点（{TARGET_DATE}）の実績値です　"
        f"気温スコア：{int(top['temp_score'])} ／ "
        f"降水スコア：{int(top['rain_score'])} ／ "
        f"複合スコア：{int(top.get('compound_score', 0))} ／ "
        f"総合リスク：{int(top['risk_score'])}"
    )

    # メトリクス
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("危険", f"{n_danger} 県")
    m2.metric("警戒", f"{n_caution} 県")
    m3.metric("注意", f"{n_warning} 県")
    m4.metric("安全", f"{n_safe} 県")

    st.divider()

    # ドーナツグラフ＋地方ブロック別横棒グラフ
    donut_df = pd.DataFrame(
        {
            "危険度": ["危険", "警戒", "注意", "安全"],
            "件数": [n_danger, n_caution, n_warning, n_safe],
        }
    )

    block_risk = (
        rank.groupby("block")
        .apply(
            lambda x: pd.Series(
                {
                    "危険": (x["risk_level"] == "危険").sum(),
                    "警戒": (x["risk_level"] == "警戒").sum(),
                    "注意": (x["risk_level"] == "注意").sum(),
                    "安全": (x["risk_level"] == "安全").sum(),
                }
            )
        )
        .reset_index()
        .melt(id_vars="block", var_name="危険度", value_name="件数")
    )
    block_risk["block"] = pd.Categorical(
        block_risk["block"], categories=BLOCK_ORDER, ordered=True
    )
    block_risk = block_risk.sort_values("block")

    # 0.5問題対策：危険度が0の行を除外してから表示
    block_risk_filtered = block_risk[block_risk["件数"] > 0]

    bar_base = alt.Chart(block_risk_filtered).encode(
        x=alt.X("件数:Q", title="都道府県数", axis=alt.Axis(tickMinStep=1)),
        y=alt.Y("block:N", title="地方", sort=BLOCK_ORDER),
        color=alt.Color(
            "危険度:N",
            scale=alt.Scale(
                domain=["危険", "警戒", "注意", "安全"],
                range=[COLOR_DANGER, COLOR_CAUTION, COLOR_WARNING, COLOR_SAFE],
            ),
            legend=alt.Legend(title="危険度"),
        ),
        order=alt.Order("危険度:N", sort="descending"),
        tooltip=["block:N", "危険度:N", "件数:Q"],
    )

    bar = bar_base.mark_bar().properties(height=280)

    # ドーナツグラフ：中央に主要な危険度と割合を大きく表示
    total = n_danger + n_caution + n_warning + n_safe
    if n_danger > 0:
        center_level = "危険"
        center_pct = round(n_danger / total * 100)
        center_color = COLOR_DANGER
    elif n_caution > 0:
        center_level = "警戒"
        center_pct = round(n_caution / total * 100)
        center_color = COLOR_CAUTION
    elif n_warning > 0:
        center_level = "注意"
        center_pct = round(n_warning / total * 100)
        center_color = COLOR_WARNING
    else:
        center_level = "安全"
        center_pct = 100
        center_color = COLOR_SAFE

    donut_with_label = (
        alt.Chart(donut_df)
        .mark_arc(innerRadius=80, outerRadius=130)
        .encode(
            theta=alt.Theta("件数:Q"),
            color=alt.Color(
                "危険度:N",
                scale=alt.Scale(
                    domain=["危険", "警戒", "注意", "安全"],
                    range=[COLOR_DANGER, COLOR_CAUTION, COLOR_WARNING, COLOR_SAFE],
                ),
                legend=None,
            ),
            tooltip=["危険度:N", "件数:Q"],
        )
    )

    center_pct_text = (
        alt.Chart(pd.DataFrame({"label": [f"{center_pct}%"]}))
        .mark_text(size=32, fontWeight="bold", color=center_color)
        .encode(text="label:N")
    )

    center_level_text = (
        alt.Chart(pd.DataFrame({"label": [center_level]}))
        .mark_text(size=16, color=center_color, dy=38)
        .encode(text="label:N")
    )

    col_donut, col_bar = st.columns([1, 1])
    with col_donut:
        st.subheader("危険度の分布")
        st.altair_chart(
            (donut_with_label + center_pct_text + center_level_text).properties(
                height=300
            ),
            use_container_width=True,
        )
        total_all = n_danger + n_caution + n_warning + n_safe
        caption_parts = []
        for level, count, color in [
            ("危険", n_danger, COLOR_DANGER),
            ("警戒", n_caution, COLOR_CAUTION),
            ("注意", n_warning, COLOR_WARNING),
            ("安全", n_safe, COLOR_SAFE),
        ]:
            pct = round(count / total_all * 100) if total_all > 0 else 0
            caption_parts.append(
                f"<span style='color:{color}'>●</span> {level} {count}県({pct}%)"
            )
        st.markdown(
            "　".join(caption_parts),
            unsafe_allow_html=True,
        )
    with col_bar:
        st.subheader("地方別の危険度")
        st.altair_chart(bar, use_container_width=True)

    st.divider()
    st.subheader("仕組み")
    st.markdown("""
    - **JMA**：気象庁から現在の警報情報を取得
    - **Open-Meteo（実績）**：過去8日分の気象データ（最高気温・降水量）を取得
    - **Open-Meteo（予報）**：今後7日間の気象予測データを取得
    - **スコア計算**：気温・降水・警報・複合条件の4指標で統合評価（上限20点）
        - 気温38℃以上：+5 / 35℃以上：+3 / 30℃以上：+2 / 0℃以下かつ降水あり：+3
        - 降水50mm以上：+6 / 20mm以上：+3
        - 高温かつ大雨（複合リスク）：+3
        - 警報発令中：+3（最低スコア5を保証）
        - 15以上：危険 / 10以上：警戒 / 5以上：注意 / 5未満：安全
    """)

# ───────────────────────────────
# タブ2：警報
# ───────────────────────────────
with tab2:

    warned = False
    warn_list = []
    for region in PREF_COORDS.keys():
        d = disaster_df[
            (disaster_df["region"] == region) & (disaster_df["status"] != "解除")
        ]
        if not d.empty:
            d_sorted = d.copy()
            d_sorted["status_order"] = d_sorted["status"].map(
                lambda s: STATUS_ORDER.get(s, 99)
            )
            events = d_sorted.sort_values("status_order").drop_duplicates(
                subset=["event_type"]
            )
            events_text = "・".join(
                f"{row['event_type']}（{row['status']}）"
                for _, row in events.iterrows()
            )
            warn_list.append((region, events_text))
            warned = True

    if not warned:
        st.success("現在、発令中の警報はありません")
    else:
        st.info(f"現在 {len(warn_list)} 県で警報・注意報が発令中です")
        # 最初の5件は展開済みで表示、残りは折りたたむ
        for region, events_text in warn_list[:5]:
            st.warning(f"{region}：{events_text}")
        if len(warn_list) > 5:
            with st.expander(f"残り {len(warn_list) - 5} 県を表示"):
                for region, events_text in warn_list[5:]:
                    st.warning(f"{region}：{events_text}")

    if not disaster_df.empty:
        active_warnings = disaster_df[disaster_df["status"] != "解除"]
        if not active_warnings.empty:
            # 同一県・同一種別の重複を排除してから集計（最大47件）
            event_counts = (
                active_warnings.drop_duplicates(subset=["region", "event_type"])
                .groupby("event_type")
                .size()
                .reset_index(name="件数")
                .rename(columns={"event_type": "警報種別"})
                .sort_values("件数", ascending=False)
            )

            st.subheader("現在発令中の警報種別")
            warn_base = alt.Chart(event_counts).encode(
                x=alt.X("件数:Q", title="発令県数", axis=alt.Axis(tickMinStep=1)),
                y=alt.Y("警報種別:N", sort="-x", title=None),
                tooltip=["警報種別:N", "件数:Q"],
            )
            warn_bar = warn_base.mark_bar(color=COLOR_WARNING)
            st.altair_chart(warn_bar, use_container_width=True)
            st.caption(
                "数値は発令中の都道府県数（同一県に複数種別がある場合はそれぞれカウント）"
            )

# ───────────────────────────────
# タブ3：地域分析
# ───────────────────────────────
with tab3:
    st.subheader("リスクランキング")

    # 複合フィルタ
    f1, f2 = st.columns(2)
    with f1:
        filter_block = st.selectbox(
            "地方ブロックで絞り込む",
            ["すべて"] + BLOCK_ORDER,
            index=0,
        )
    with f2:
        filter_level = st.selectbox(
            "危険度で絞り込む",
            ["すべて", "危険", "警戒", "注意", "安全"],
            index=0,
        )

    filtered_rank = rank.copy()
    if filter_block != "すべて":
        filtered_rank = filtered_rank[filtered_rank["block"] == filter_block]
    if filter_level != "すべて":
        filtered_rank = filtered_rank[filtered_rank["risk_level"] == filter_level]

    st.dataframe(
        filtered_rank.assign(
            temperature_max=filtered_rank["temperature_max"].map(lambda x: f"{x:.1f}℃"),
            precipitation_sum=filtered_rank["precipitation_sum"].map(
                lambda x: f"{x:.1f}mm"
            ),
        ).rename(
            columns={
                "region": "地域",
                "block": "地方",
                "risk_score": "リスク",
                "risk_level": "危険度",
                "temperature_max": "最高気温",
                "precipitation_sum": "降水量",
                "compound_score": "複合",
            }
        )[
            ["地域", "地方", "リスク", "危険度", "最高気温", "降水量", "複合"]
        ],
        hide_index=True,
        use_container_width=True,
    )

    st.subheader("地域ごとのリスク分布")
    st.caption("注意以上の地域のみ表示（円の大きさは警報数を表します）")

    m = folium.Map(location=[36, 138], zoom_start=5)

    for _, r in latest.iterrows():
        if r["risk_score"] < RISK_WARNING:
            continue
        if r["region"] not in PREF_COORDS:
            continue

        color = (
            COLOR_DANGER
            if r["risk_score"] >= RISK_DANGER
            else COLOR_CAUTION if r["risk_score"] >= RISK_CAUTION else COLOR_WARNING
        )

        popup_html = f"""
        <b>{r['region']}</b><br>
        地方：{r['block']}<br>
        危険度：{r['risk_level']}<br>
        リスク：{int(r['risk_score'])}<br>
        警報数：{int(r['warning_count'])}
        """

        folium.CircleMarker(
            location=PREF_COORDS[r["region"]],
            radius=12 if r["warning_count"] > 0 else 6,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.7,
            popup=folium.Popup(popup_html, max_width=250),
        ).add_to(m)

    st_folium(m, width=None, height=500, use_container_width=True)

# ───────────────────────────────
# タブ4：気象予測（Plotly）
# ───────────────────────────────
with tab4:
    import plotly.graph_objects as go

    regions = sorted(PREF_COORDS.keys())
    default_index = regions.index("大阪府") if "大阪府" in regions else 0
    sel = st.selectbox("地域選択", regions, index=default_index)

    # 過去データ（DB）
    past_df = risk_df[risk_df["region"] == sel].copy()
    past_df["observed_date"] = pd.to_datetime(past_df["observed_date"])
    past_df = past_df.sort_values("observed_date")

    # 予測データ（API）
    with st.spinner("予測データを取得しています..."):
        lat, lon = PREF_COORDS[sel]
        forecast_records = fetch_forecast(sel, lat, lon)

    if not forecast_records:
        st.warning("予測データの取得に失敗しました")
        forecast_df = pd.DataFrame(
            columns=["observed_date", "temperature_max", "precipitation_sum"]
        )
    else:
        forecast_df = pd.DataFrame(forecast_records)
        forecast_df["observed_date"] = pd.to_datetime(forecast_df["observed_date"])
        forecast_df = forecast_df.drop_duplicates(subset=["observed_date"]).sort_values(
            "observed_date"
        )

    # forecast_dfを今日以降（予測）として扱う
    today_ts = pd.Timestamp(TODAY)
    forecast_today = pd.DataFrame()  # 実績ラインへの結合は行わない
    forecast_future = forecast_df.copy() if not forecast_df.empty else pd.DataFrame()

    # 予測期間の終端日（X軸・vrectと一致させる）
    forecast_end = (
        forecast_future["observed_date"].max()
        if not forecast_future.empty
        else pd.Timestamp(TODAY) + timedelta(days=7)
    )

    # 現在の危険度を取得
    sel_latest = latest[latest["region"] == sel]
    sel_risk_level = (
        sel_latest["risk_level"].values[0] if not sel_latest.empty else "安全"
    )
    sel_risk_score = (
        int(sel_latest["risk_score"].values[0]) if not sel_latest.empty else 0
    )
    level_colors = {
        "危険": COLOR_DANGER,
        "警戒": COLOR_CAUTION,
        "注意": COLOR_WARNING,
        "安全": COLOR_SAFE,
    }
    sel_color = level_colors.get(sel_risk_level, COLOR_SAFE)

    # ───── 気温グラフ ─────
    st.markdown(
        f"**気温の推移と予測 ／ {sel}**　"
        f"<span style='color:{sel_color}; font-weight:bold;'>現在：{sel_risk_level}（スコア {sel_risk_score}）</span>",
        unsafe_allow_html=True,
    )
    past_start = (
        past_df["observed_date"].min().strftime("%m/%d") if not past_df.empty else ""
    )
    past_end = (
        past_df["observed_date"].max().strftime("%m/%d") if not past_df.empty else ""
    )
    forecast_start_str = (
        forecast_future["observed_date"].min().strftime("%m/%d")
        if not forecast_future.empty
        else ""
    )
    forecast_end_str = (
        forecast_future["observed_date"].max().strftime("%m/%d")
        if not forecast_future.empty
        else ""
    )
    st.markdown(
        f"<span style='font-size:0.82em; color:#888;'>"
        f"縦軸：最高気温（℃）　"
        f"<span style='color:{COLOR_WARNING};'>━</span> 日付：{past_start}〜{past_end}　"
        f"<span style='color:#4a90d9;'>- - -</span> 予測：{forecast_start_str}〜{forecast_end_str}　"
        f"<span style='color:#aaa;'>│</span> 縦線＝今日　"
        f"<span style='background:#e8f4fd; padding:0 4px;'>　　</span> 予測期間"
        f"</span>",
        unsafe_allow_html=True,
    )

    temp_min = max(0, past_df["temperature_max"].min() - 5) if not past_df.empty else 0
    temp_max = (
        max(
            past_df["temperature_max"].max() if not past_df.empty else 40,
            forecast_df["temperature_max"].max() if not forecast_df.empty else 0,
        )
        + 5
    )

    fig_temp = go.Figure()

    # 予測期間の背景色
    fig_temp.add_vrect(
        x0=pd.Timestamp(TODAY).isoformat(),
        x1=forecast_end.isoformat(),
        fillcolor="#e8f4fd",
        opacity=0.3,
        layer="below",
        line_width=0,
    )

    # 実績ライン（archiveのみ・本日ラインまで）
    fig_temp.add_trace(
        go.Scatter(
            x=past_df["observed_date"],
            y=past_df["temperature_max"],
            mode="lines",
            line=dict(color=COLOR_WARNING, width=2),
            showlegend=False,
            hovertemplate="日付：%{x|%m/%d}<br>最高気温：%{y:.1f}℃<extra></extra>",
        )
    )

    # 実績終点→予測始点をオレンジ点線でつなぐ
    if not forecast_future.empty and not past_df.empty:
        bridge_temp = pd.concat(
            [
                past_df[["observed_date", "temperature_max"]].tail(1),
                forecast_future[["observed_date", "temperature_max"]].head(1),
            ],
            ignore_index=True,
        )
        fig_temp.add_trace(
            go.Scatter(
                x=bridge_temp["observed_date"],
                y=bridge_temp["temperature_max"],
                mode="lines",
                line=dict(color=COLOR_WARNING, width=2),
                showlegend=False,
                hoverinfo="skip",
            )
        )

    # 予測ライン（今日以降・グレーゾーンから破線で開始）
    if not forecast_future.empty:
        fig_temp.add_trace(
            go.Scatter(
                x=forecast_future["observed_date"],
                y=forecast_future["temperature_max"],
                mode="lines+markers",
                line=dict(color="#4a90d9", width=2, dash="dash"),
                marker=dict(size=7, color="#4a90d9"),
                showlegend=False,
                hovertemplate="日付：%{x|%m/%d}<br>最高気温：%{y:.1f}℃（予測）<extra></extra>",
            )
        )

    # 今日の縦線
    today_x = str(pd.Timestamp(TODAY))
    fig_temp.add_shape(
        type="line",
        x0=today_x,
        x1=today_x,
        y0=0,
        y1=1,
        xref="x",
        yref="paper",
        line=dict(color="#aaaaaa", width=1.5),
    )
    fig_temp.add_annotation(
        x=today_x,
        y=1,
        xref="x",
        yref="paper",
        text="本日",
        showarrow=False,
        font=dict(color="#aaaaaa", size=11),
        yanchor="bottom",
    )

    fig_temp.update_layout(
        height=350,
        margin=dict(l=20, r=20, t=20, b=40),
        yaxis=dict(title=None, range=[temp_min, temp_max]),
        xaxis=dict(
            title=None,
            tickformat="%m/%d",
            dtick=86400000,
            tickangle=-30,
            range=[
                past_df["observed_date"].min() if not past_df.empty else None,
                (
                    forecast_future["observed_date"].max()
                    if not forecast_future.empty
                    else None
                ),
            ],
        ),
        showlegend=False,
        hovermode="x unified",
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    fig_temp.update_xaxes(showgrid=True, gridcolor="#eeeeee")
    fig_temp.update_yaxes(showgrid=True, gridcolor="#eeeeee")

    st.plotly_chart(fig_temp, use_container_width=True)

    # ───── 降水量グラフ ─────
    st.markdown(f"**降水量の推移と予測 ／ {sel}**")
    st.markdown(
        f"<span style='font-size:0.82em; color:#888;'>"
        f"縦軸：降水量（mm）　"
        f"<span style='color:steelblue;'>━</span> 日付：{past_start}〜{past_end}　"
        f"<span style='color:#4a90d9;'>- - -</span> 予測：{forecast_start_str}〜{forecast_end_str}　"
        f"<span style='color:#aaa;'>│</span> 縦線＝今日　"
        f"<span style='background:#e8f4fd; padding:0 4px;'>　　</span> 予測期間"
        f"</span>",
        unsafe_allow_html=True,
    )

    rain_max = (
        max(
            past_df["precipitation_sum"].max() if not past_df.empty else 0,
            forecast_df["precipitation_sum"].max() if not forecast_df.empty else 0,
        )
        + 10
    )

    fig_rain = go.Figure()

    # 予測期間の背景色
    fig_rain.add_vrect(
        x0=pd.Timestamp(TODAY).isoformat(),
        x1=forecast_end.isoformat(),
        fillcolor="#e8f4fd",
        opacity=0.3,
        layer="below",
        line_width=0,
    )

    # 実績ライン（archiveのみ・本日ラインまで）
    fig_rain.add_trace(
        go.Scatter(
            x=past_df["observed_date"],
            y=past_df["precipitation_sum"],
            mode="lines",
            line=dict(color="steelblue", width=2),
            showlegend=False,
            hovertemplate="日付：%{x|%m/%d}<br>降水量：%{y:.1f}mm<extra></extra>",
        )
    )

    # 実績終点→予測始点を青点線でつなぐ
    if not forecast_future.empty and not past_df.empty:
        bridge_rain = pd.concat(
            [
                past_df[["observed_date", "precipitation_sum"]].tail(1),
                forecast_future[["observed_date", "precipitation_sum"]].head(1),
            ],
            ignore_index=True,
        )
        fig_rain.add_trace(
            go.Scatter(
                x=bridge_rain["observed_date"],
                y=bridge_rain["precipitation_sum"],
                mode="lines",
                line=dict(color="steelblue", width=2),
                showlegend=False,
                hoverinfo="skip",
            )
        )

    # 予測ライン（今日以降・グレーゾーンから破線で開始）
    if not forecast_future.empty:
        fig_rain.add_trace(
            go.Scatter(
                x=forecast_future["observed_date"],
                y=forecast_future["precipitation_sum"],
                mode="lines+markers",
                line=dict(color="#4a90d9", width=2, dash="dash"),
                marker=dict(size=7, color="#4a90d9"),
                showlegend=False,
                hovertemplate="日付：%{x|%m/%d}<br>降水量：%{y:.1f}mm（予測）<extra></extra>",
            )
        )

    # 今日の縦線
    fig_rain.add_shape(
        type="line",
        x0=today_x,
        x1=today_x,
        y0=0,
        y1=1,
        xref="x",
        yref="paper",
        line=dict(color="#aaaaaa", width=1.5),
    )
    fig_rain.add_annotation(
        x=today_x,
        y=1,
        xref="x",
        yref="paper",
        text="本日",
        showarrow=False,
        font=dict(color="#aaaaaa", size=11),
        yanchor="bottom",
    )

    fig_rain.update_layout(
        height=350,
        margin=dict(l=20, r=20, t=20, b=40),
        yaxis=dict(title=None, range=[0, rain_max]),
        xaxis=dict(
            title=None,
            tickformat="%m/%d",
            dtick=86400000,
            tickangle=-30,
            range=[
                past_df["observed_date"].min() if not past_df.empty else None,
                (
                    forecast_future["observed_date"].max()
                    if not forecast_future.empty
                    else None
                ),
            ],
        ),
        showlegend=False,
        hovermode="x unified",
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    fig_rain.update_xaxes(showgrid=True, gridcolor="#eeeeee")
    fig_rain.update_yaxes(showgrid=True, gridcolor="#eeeeee")

    st.plotly_chart(fig_rain, use_container_width=True)
