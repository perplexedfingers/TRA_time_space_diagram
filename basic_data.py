import csv
from itertools import groupby, tee
from operator import itemgetter

LINES = ('LINE_WN', 'LINE_WM', 'LINE_WSEA', 'LINE_WS', 'LINE_P', 'LINE_S',
         'LINE_T', 'LINE_N', 'LINE_I', 'LINE_PX', 'LINE_NW', 'LINE_J', 'LINE_SL')


class GlobalVariables:
    def __init__(self):
        # 處理所有車站基本資訊(Stations.csv)
        with open('CSV/Stations.csv', newline='', encoding='utf8') as csvfile:
            reader = csv.reader(csvfile)
            self.Stations = {row[0]: row for row in reader}

        # 時間轉換(Locate.csv)
        with open('CSV/Locate.csv', newline='', encoding='big5') as csvfile:
            reader = csv.reader(csvfile)
            self.TimeLocation = {row[0]: row[2] for row in reader}

        # 處理所有車站基本資訊(Category.csv)
        with open('CSV/Category.csv', newline='', encoding='utf8') as csvfile:
            reader = csv.reader(csvfile)
            self.Category = (row for row in reader)
            list_csv = ((row[0], row[1], row[2], row[3])  # line, ID, name, relative distance
                        for row in self.Category)

            fetch_kind = itemgetter(0)
            for_background, another = tee(groupby(sorted(filter(lambda r: fetch_kind(r) in LINES, list_csv),
                                                         key=fetch_kind),
                                                  key=fetch_kind))
            self.LinesStationsForBackground = {  # 各營運路線車站於運行圖中的位置，包含廢站、號誌站等車站
                kind: [[line, id_, name, relative_distance]
                       for line, id_, name, relative_distance in info]
                for kind, info in for_background}
            self.LinesStations = {  # 各營運路線車站於運行圖中的位置，用於運行線的繪製
                kind: {line: [float(relative_distance), name]
                       for line, id_, name, relative_distance in info}
                for kind, info in another}
