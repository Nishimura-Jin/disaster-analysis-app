import sqlite3

import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import pandas as pd
import requests
import streamlit as st

# 日本語フォント設定
font_path = r"C:\Users\lunat\OneDrive\ドキュメント\Python\Noto_Sans_JP\NotoSansJP-VariableFont_wght.ttf"
font_prop = fm.FontProperties(fname=font_path)
plt.rcParams["font.family"] = font_prop.get_name()


# 都道府県コード変換
def convert_region(code: str) -> str:
    code = str(code)[:2]

    mapping = {
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

    return mapping.get(code, "不明")


# 警報種別
def convert_event(code: str) -> str:
    mapping = {
        "14": "大雨",
        "15": "洪水",
        "16": "暴風",
        "17": "大雪",
        "21": "強風",
    }
    return mapping.get(str(code), "その他")


# 気象庁データ取得
def fetch_data() -> pd.DataFrame:
    url = "https://www.jma.go.jp/bosai/warning/data/warning/map.json"

    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        raw = res.json()
    except Exception:
        return pd.DataFrame()

    records = []

    for report in raw:
        try:
            dt = pd.to_datetime(report.get("reportDatetime"))
        except Exception:
            continue

        for area_type in report.get("areaTypes", []):
            for area in area_type.get("areas", []):

                region = convert_region(area.get("code"))

                for warning in area.get("warnings", []):
                    if warning.get("status") == "解除":
                        continue

                    records.append(
                        {
                            "datetime": dt,
                            "date": dt.date(),
                            "hour": dt.hour,
                            "region": region,
                            "event_type": convert_event(warning.get("code")),
                            "intensity": 1,
                        }
                    )

    return pd.DataFrame(records)


# DB保存
def save_to_db(df: pd.DataFrame) -> None:
    if df.empty:
        return

    with sqlite3.connect("disaster.db") as conn:
        df.to_sql("disaster", conn, if_exists="append", index=False)


# DB読み込み
def load_data() -> pd.DataFrame:
    try:
        with sqlite3.connect("disaster.db") as conn:
            return pd.read_sql("SELECT * FROM disaster", conn)
    except Exception:
        return pd.DataFrame()


# 地域別集計
def calculate_risk(df: pd.DataFrame) -> pd.Series:
    return df.groupby("region").size().sort_values(ascending=False)


# グラフ表示
def plot_bar(data: pd.Series, title: str) -> None:
    fig, ax = plt.subplots()

    data.plot(kind="bar", ax=ax)

    ax.set_title(title, fontproperties=font_prop)
    ax.set_xlabel("地域", fontproperties=font_prop)
    ax.set_ylabel("件数", fontproperties=font_prop)

    for label in ax.get_xticklabels():
        label.set_fontproperties(font_prop)

    plt.xticks(rotation=0)
    plt.tight_layout()

    st.pyplot(fig)


# ======================
# UI
# ======================
st.title("災害リスク分析ダッシュボード")

st.caption("気象庁の警報データをもとに地域別の発生傾向を可視化します。")

if st.button("データ更新"):
    df_new = fetch_data()
    st.write(f"取得件数: {len(df_new)}")
    save_to_db(df_new)

df = load_data()

if df.empty:
    st.info("データがありません。更新ボタンから取得してください。")
    st.stop()

df = df.tail(500)

st.write(f"データ件数: {len(df)}")

st.subheader("地域別発生件数（上位10）")
region_counts = calculate_risk(df).head(10)
plot_bar(region_counts, "地域別発生件数")

st.subheader("時間帯別発生傾向")
hour_counts = df.groupby("hour").size()
plot_bar(hour_counts, "時間帯別発生数")

st.subheader("リスクランキング")
risk = calculate_risk(df)
st.write(risk.head(10))

st.write(f"最も発生が多い地域: {risk.index[0]}")
