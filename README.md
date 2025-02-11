# 台灣鐵路局公開資料JSON繪製鐵路運行圖(SVG格式)程式(TRA_time_space_diagram)

|項目|內容|
|---|---|
|Author|呂卓勳 Cho-Hsun Lu|
|E-mail|billy1125@gmail.com|
|版本|1.021|

## 致謝

* nedwu(https://github.com/nedwu) 的程式碼。
* 黃祈恩(https://www.facebook.com/profile.php?id=100051647619113&sk=about) 幫忙將台鐵車站代碼基本資料的 CSV 檔案更正
* 施佩佶(https://www.facebook.com/profile.php?id=100009938435817) 於 2020.10.16 通報 6655、6657 車次錯誤問題

感謝以上網友對於本程式的建議與錯誤回報，您的協助能讓本程式更臻完整，感謝！

## 緣起

鐵道愛好者通常都會使用**鐵路運行圖(Railway Time-Space Diagram)**，幫助研究鐵道相關事務，雖然有人會出版紙本台鐵鐵路運行圖，但是當台鐵改點，紙本的資訊參考價值就會變低。所以當我知道台鐵已經將時刻資訊以 JSON 公開於網路上之後，便試圖以 Python 開發一個繪製台灣鐵路運行圖的程式。

## 限制

本程式鐵路運行圖均來自於台鐵 open data 進行分析整理與繪製。然而**公開資料不等於實際台鐵的運行計畫**，僅是旅客所需的列車到站與離站時間資料，列車的待避或會車狀況無法在運行圖中展示，因此會出現運行圖與實際運行有所差異的現象。因此實際鐵路運行情況，請以台鐵所公布資訊為準。

## 程式功能

本程式用於將台鐵公開之時刻表 JSON 格式檔案 (以下稱台鐵 JSON)，轉換為鐵路運行圖，運行圖格式為可縮放向量圖形（Scalable Vector Graphics, SVG）。並且提供一個批次下載台鐵 JSON 之簡易程式。

## 執行語言與所需相關套件

目前本程式以 Python 3.9 開發, due to 'RETURNING' keyword in sqlite3.

## Usage

- Setup virtual environment

```fish
python3 -m venv env
```

- Activate virtual environment
May differ from your shell environment

```fish
source env/bin/active.fish
```

- Install needed libraries

```fish
pip install -r requirement.txt
```

### To download all three needed files:
The downloaded files would be in `JSON`

```
python download_json.py
```

For more detail:
```
python download_json.py -h
```

### To ONLY build the database
After download needed files

```
python construct_db_from_json.py
```

This would
* Set up a SQLite database as `db.sqlite` file
* Store the data into database

You may change the parameter to keep the database as a file

For more detail:
```
python construct_db_from_json.py -h
```

### To draw diagrams
After download needed files

```
python from_svg.py
```

This would
* Set up a SQLite database in the memory

* Load the data

    OR

* Use a prepared database

* Query from the database
* Prepare the SVG
* Save the SVG to `OUTPUT` in HTML

For more detail:
```
python from_svg.py -h
```

> 附註：台鐵每日均提供當日至 45 天內每日之時刻表資料，以 JSON 格式提供。

## 閱讀運行圖之方法

本程式所轉換之運行圖，檔案副檔名為 **.html**，請使用瀏覽器直接開啟檔案。

目前為止，本程式所轉換之運行圖於 Google Chrome、Mozilla Firefox、Opera、Apple Safari 均能正常顯示，至於其他瀏覽器尚未實地測試，若有問題也歡迎回報。


## Data source

- Route

    https://data.gov.tw/dataset/6999

- Station

    https://data.gov.tw/dataset/33425

- Timetable

    https://data.gov.tw/dataset/6138
