import sqlite3
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import pandas as pd
import requests
import streamlit as st

# =========================================================
# 目的（就活向けにシンプル）
# ① API取得 → ② シンプル保存(INSERT OR IGNORE) → ③ 可視化
# =========================================================

APP_TZ = ZoneInfo("Asia/Tokyo")
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "disaster.db"

JMA_URL = "https://www.jma.go.jp/bosai/warning/data/warning/map.json"

REGION = {
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

EVENT = {
    "14": "大雨",
    "15": "洪水",
    "16": "暴風",
    "17": "大雪",
    "21": "強風",
}

# 日本語フォント（無くても動くようにする）
FONT_PATH = Path(
    r"C:\Users\lunat\OneDrive\ドキュメント\Python\Noto_Sans_JP\NotoSansJP-VariableFont_wght.ttf"
)
if FONT_PATH.exists():
    fm.fontManager.addfont(str(FONT_PATH))
    font_prop = fm.FontProperties(fname=str(FONT_PATH))
    plt.rcParams["font.family"] = font_prop.get_name()
else:
    font_prop = None


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS disasters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_hour TEXT NOT NULL,
                hour INTEGER NOT NULL,
                region TEXT NOT NULL,
                event_type TEXT NOT NULL,
                intensity INTEGER NOT NULL,
                UNIQUE(snapshot_hour, region, event_type)
            )
            """
        )


@st.cache_data(ttl=600)
def fetch_data() -> pd.DataFrame:
    fetched_at = datetime.now(APP_TZ)
    snapshot_hour = fetched_at.replace(minute=0, second=0, microsecond=0)

    try:
        res = requests.get(JMA_URL, timeout=10)
        res.raise_for_status()
        raw = res.json()
    except Exception as exc:
        st.error(f"データ取得失敗: {exc}")
        return pd.DataFrame()

    records: list[dict[str, object]] = []
    for report in raw:
        for area_type in report.get("areaTypes", []):
            for area in area_type.get("areas", []):
                region = REGION.get(str(area.get("code"))[:2], "不明")
                for warning in area.get("warnings", []):
                    if warning.get("status") == "解除":
                        continue

                    event_type = EVENT.get(str(warning.get("code")), "その他")
                    records.append(
                        {
                            "snapshot_hour": snapshot_hour.isoformat(),
                            "hour": snapshot_hour.hour,
                            "region": region,
                            "event_type": event_type,
                            "intensity": 1,
                        }
                    )

    return pd.DataFrame(records)


def save_to_db(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    rows = [
        (
            str(r["snapshot_hour"]),
            int(r["hour"]),
            str(r["region"]),
            str(r["event_type"]),
            int(r["intensity"]),
        )
        for _, r in df.iterrows()
    ]

    with sqlite3.connect(DB_PATH) as conn:
        before = conn.total_changes
        conn.executemany(
            """
            INSERT OR IGNORE INTO disasters
            (snapshot_hour, hour, region, event_type, intensity)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
        after = conn.total_changes

    return after - before


def load_data() -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql("SELECT * FROM disasters", conn)
    return df


def plot_bar(series: pd.Series, title: str, xlabel: str) -> None:
    if series.empty:
        st.info("表示できるデータがありません。")
        return

    fig, ax = plt.subplots()
    series.plot(kind="bar", ax=ax)

    if font_prop is None:
        ax.set_title(title)
        ax.set_xlabel(xlabel)
        ax.set_ylabel("件数")
    else:
        ax.set_title(title, fontproperties=font_prop)
        ax.set_xlabel(xlabel, fontproperties=font_prop)
        ax.set_ylabel("件数", fontproperties=font_prop)

    ax.tick_params(axis="x", rotation=45)
    plt.tight_layout()
    st.pyplot(fig)
    plt.close(fig)


# ======================
# 画面
# ======================
st.set_page_config(page_title="災害リスク分析ダッシュボード", layout="wide")
st.title("災害リスク分析ダッシュボード")
st.caption("気象庁の警報・注意報を取得し、地域別・時間帯別に集計して可視化します。")

init_db()

if st.button("データ更新"):
    df_new = fetch_data()
    inserted = save_to_db(df_new)
    st.success(f"取得: {len(df_new)} 件 / 追加: {inserted} 件")

df = load_data()
if df.empty:
    st.info("データがありません。まずは「データ更新」を押してください。")
    st.stop()

st.write(f"総データ数: {len(df)}")

# フィルタ（シンプル）
regions = sorted(df["region"].unique().tolist())
selected = st.selectbox("地域フィルター", ["すべて"] + regions)
if selected != "すべて":
    df = df[df["region"] == selected]

# 可視化（見せ場）
st.subheader("地域別（最新スナップショット）")
latest_snapshot = df["snapshot_hour"].max()
latest_df = df[df["snapshot_hour"] == latest_snapshot]
plot_bar(
    latest_df.groupby("region").size().sort_values(ascending=False).head(10),
    "地域別（最新）",
    "地域",
)

st.subheader("時間帯別（全期間）")
plot_bar(df.groupby("hour").size(), "時間帯別", "時間帯")

st.subheader("CSVダウンロード")
st.download_button(
    "CSVダウンロード",
    df.to_csv(index=False).encode("utf-8-sig"),
    "disaster_data.csv",
    mime="text/csv",
)
