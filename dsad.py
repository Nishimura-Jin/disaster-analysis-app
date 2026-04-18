import sqlite3
from pathlib import Path

import pandas as pd

DB_PATH = Path(
    r"C:\Users\lunat\OneDrive\ドキュメント\Python\disaster-analysis-app\disaster.db"
)

with sqlite3.connect(DB_PATH) as conn:
    weather = pd.read_sql("SELECT * FROM weather", conn)
    disaster = pd.read_sql("SELECT * FROM disaster", conn)

print("===================================")

print("\n【weather 日付】")
print(weather["observed_date"].value_counts().sort_index())

print("\n【disaster 日付】")
print(disaster["observed_date"].value_counts().sort_index())

print("\n===================================")

print("\n【地域ごとの件数（weather）】")
print(weather["region"].value_counts())

print("\n【地域ごとの件数（disaster）】")
print(disaster["region"].value_counts())

print("\n===================================")

print("\n【JOIN確認】")
merged = weather.merge(disaster, on=["observed_date", "region"], how="inner")
print("JOIN件数:", len(merged))
