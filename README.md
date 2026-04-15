# 災害リスク分析ダッシュボード（日本）

気象庁の警報データを取得し、地域別・時間帯別の発生傾向を可視化するダッシュボードです。

Streamlitを用いて簡易的な分析UIを提供しています。

---

## ■ 概要

本アプリは、気象庁が公開している警報データを取得し、
SQLiteに保存した上で以下の分析を行います。

- 地域別の警報発生件数
- 時間帯別の発生傾向
- リスクランキング（発生頻度ベース）

---

## ■ 使用技術

- Python 3.x
- Streamlit
- Pandas
- Matplotlib
- SQLite
- Requests

---

## ■ データソース

- 気象庁 防災情報XML / JSON API  
  https://www.jma.go.jp/bosai/warning/data/warning/map.json

---

## ■ 使い方

### 1. 必要ライブラリのインストール

以下のコマンドをターミナルで実行して、環境を構築してください。

```bash
pip install streamlit pandas matplotlib requests
```

### 2. アプリ起動

```bash
streamlit run app.py
```

### 3. 操作
「データ更新」ボタンで最新データ取得
自動でSQLiteに保存
各種分析グラフを表示