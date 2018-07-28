# 台鐵運行圖 Python 版
import sys
import os
from os import walk
import io
import queue
import threading

# 自訂class與module
import data_transfer as dt
import read_tra_json as tra_json
import build_diagram as bd
from progessbar import progress

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 建立佇列
queueTrains = queue.Queue()

class Job(threading.Thread):
    def __init__(self, queue, dt, svg_draw_job):
        threading.Thread.__init__(self)
        self.queue = queue
        self.dt = dt
        self.svg_draw_job = svg_draw_job

    def run(self):
        while self.queue.qsize() > 0:
            # 取得新的資料
            train_no = self.queue.get()

            # 處理資料
            train_id = train_no['Train']  # 車次號
            car_class = train_no['CarClass']  # 車種
            line = train_no['Line']  # 山線1、海線2、其他0
            over_night_stn = train_no['OverNightStn']  # 跨午夜車站
            line_dir = train_no['LineDir']  # 順逆行

            list_start_end_station = self.dt.find_train_stations(train_no)  # 查詢台鐵時刻表中，特定車次所有停靠車站

            # print(list_start_end_station)

            list_passing_stations = self.dt.find_stations(list_start_end_station, train_no['Line'],
                                                   train_no['LineDir'])  # 查詢特定車次所有停靠與通過車站

            # print(list_passing_stations)

            midnight_km = 0

            if over_night_stn != '0':
              midnight_km = self.dt.midnight_train(list_start_end_station, list_passing_stations, over_night_stn)  # 跨夜車次處理

            train_time_space = self.dt.train_time_to_stations(list_start_end_station, list_passing_stations,
                                                       midnight_km)  # 估算特定車次通過車站通過時間

            temp = []

            if 'End' in list_start_end_station:  # 環島車次處理(例如51、52車次)，將停靠車站拆成三段分別處理(台北-八堵、八堵-竹南、竹南-台北)
              temp = self.dt.roundabout_train(train_time_space, line_dir)
            else:
              temp.append(train_time_space)

            #  print(train_time_space)

            self.svg_draw_job.draw(temp, train_id, car_class, line)

class svg_draw():
    def __init__(self, website_svg_location, filename, version):
        self.svg_output_WN = bd.Draw(website_svg_location + '/west_link_north/WESTNORTH_', filename.split('.')[0], 'LINE_WN', version, 3000)
        self.svg_output_WS = bd.Draw(website_svg_location + '/west_link_south/WESTSOUTH_', filename.split('.')[0], 'LINE_WS', version, 3000)
        self.svg_output_WM = bd.Draw(website_svg_location + '/west_link_moutain/WESTMOUNTAIN_', filename.split('.')[0], 'LINE_WM', version, 2000)
        self.svg_output_WSEA = bd.Draw(website_svg_location + '/west_link_sea/WESTSEA_', filename.split('.')[0], 'LINE_WSEA', version, 2000)
        self.svg_output_P = bd.Draw(website_svg_location + '/pingtung/PINGTUNG_', filename.split('.')[0], 'LINE_P', version, 2000)
        self.svg_output_S = bd.Draw(website_svg_location + '/south_link/SOUTHLINK_', filename.split('.')[0], 'LINE_S', version, 2000)
        self.svg_output_T = bd.Draw(website_svg_location + '/taitung/TAITUNG_', filename.split('.')[0], 'LINE_T', version, 2000)
        self.svg_output_N = bd.Draw(website_svg_location + '/north_link/NORTHLINK_', filename.split('.')[0], 'LINE_N', version, 2000)
        self.svg_output_I = bd.Draw(website_svg_location + '/yilan/YILAN_', filename.split('.')[0], 'LINE_I', version, 2000)
        self.svg_output_PX = bd.Draw(website_svg_location + '/pingxi/PINGXI_', filename.split('.')[0], 'LINE_PX', version, 1250)
        self.svg_output_NW = bd.Draw(website_svg_location + '/neiwan/NEIWAN_', filename.split('.')[0], 'LINE_NW', version, 1250)
        self.svg_output_J = bd.Draw(website_svg_location + '/jiji/JIJI_', filename.split('.')[0], 'LINE_J', version, 1250)
        self.svg_output_SL = bd.Draw(website_svg_location + '/shalun/SHALUN_', filename.split('.')[0], 'LINE_SL', version, 650)
        
    def draw(self, list_of_train_time_space, train_id, car_class, line):
        for item in list_of_train_time_space:
            self.svg_output_WN.draw_trains(item, train_id, car_class, line)
            self.svg_output_WS.draw_trains(item, train_id, car_class, line)
            self.svg_output_WM.draw_trains(item, train_id, car_class, line)
            self.svg_output_WSEA.draw_trains(item, train_id, car_class, line)
            self.svg_output_P.draw_trains(item, train_id, car_class, line)
            self.svg_output_S.draw_trains(item, train_id, car_class, line)
            self.svg_output_T.draw_trains(item, train_id, car_class, line)
            self.svg_output_N.draw_trains(item, train_id, car_class, line)
            self.svg_output_I.draw_trains(item, train_id, car_class, line)
            self.svg_output_PX.draw_trains(item, train_id, car_class, line)
            self.svg_output_NW.draw_trains(item, train_id, car_class, line)
            self.svg_output_J.draw_trains(item, train_id, car_class, line)
            self.svg_output_SL.draw_trains(item, train_id, car_class, line)
            
    def output(self):
        txt_output = ""

        txt_output += self.svg_output_WN.save()
        txt_output += self.svg_output_WS.save()
        txt_output += self.svg_output_WM.save()
        txt_output += self.svg_output_WSEA.save()
        txt_output += self.svg_output_P.save()
        txt_output += self.svg_output_S.save()
        txt_output += self.svg_output_T.save()
        txt_output += self.svg_output_N.save()
        txt_output += self.svg_output_I.save()
        txt_output += self.svg_output_PX.save()
        txt_output += self.svg_output_NW.save()
        txt_output += self.svg_output_J.save()
        txt_output += self.svg_output_SL.save()

        return txt_output

# 程式執行段
def main(argv_json_location, argv_website_svg_location, argv_train_no):

    version = '1.0b'
    txt_output = ''
    json_files = []

    check_output_folder(argv_website_svg_location)
    
    print('台鐵JSON轉檔運行圖程式 - 版本：' + version + '\n')

    for root, dirs, files in walk(argv_json_location + '/'):
        for item in files:
            json_files.append(item)

    for filename in json_files:
        
        if filename.split('.')[1] == 'json':
            txt_output += '台鐵JSON時刻表檔案日期：' + filename.split('.')[0] + '\n'
            print('台鐵JSON時刻表檔案日期：' + filename.split('.')[0])

            count = 0

            data = tra_json.read_json(filename)
            trains = tra_json.find_trains(data, argv_train_no) # 特定車次的基本資料
            total = len(trains)

            for train_no in trains:  # 將所有車次放入佇列之中
                queueTrains.put(train_no)

            svg_draw_job = svg_draw(argv_website_svg_location, filename, version)

            job1 = Job(queueTrains, dt, svg_draw_job)
            job2 = Job(queueTrains, dt, svg_draw_job)

            # 讓 Worker 開始處理資料
            job1.start()
            job2.start()

            while queueTrains.qsize() > 0:
                progress(queueTrains.qsize(), total, '')

            # 等待所有 Worker 結束
            job1.join()
            job2.join()

            txt_output = svg_draw_job.output()

            txt_output += '完成 ok \n'

            # for train_no in trains:
            #
            #     count += 1
            #     progress(count, total, '')
            #
            #     train_id = train_no['Train'] # 車次號
            #     car_class = train_no['CarClass'] # 車種
            #     line = train_no['Line'] # 山線1、海線2、其他0
            #     over_night_stn = train_no['OverNightStn'] # 跨午夜車站
            #     line_dir = train_no['LineDir'] # 順逆行
            #
            #     list_start_end_station = dt.find_train_stations(train_no) # 查詢台鐵時刻表中，特定車次所有停靠車站
            #
            #     # print(list_start_end_station)
            #
            #     list_passing_stations = dt.find_stations(list_start_end_station, train_no['Line'], train_no['LineDir']) # 查詢特定車次所有停靠與通過車站
            #
            #     # print(list_passing_stations)
            #
            #     midnight_km = 0
            #
            #     if over_night_stn != '0':
            #         midnight_km = dt.midnight_train(list_start_end_station, list_passing_stations, over_night_stn) # 跨夜車次處理
            #
            #     train_time_space = dt.train_time_to_stations(list_start_end_station, list_passing_stations, midnight_km) # 估算特定車次通過車站通過時間
            #
            #     temp = []
            #
            #     if 'End' in list_start_end_station: # 環島車次處理(例如51、52車次)，將停靠車站拆成三段分別處理(台北-八堵、八堵-竹南、竹南-台北)
            #         temp = dt.roundabout_train(train_time_space, line_dir)
            #     else:
            #         temp.append(train_time_space)
            #
            #     #  print(train_time_space)
            #
            #     svg_draw_job.draw(temp, train_id, car_class, line)
            #
            #
            # txt_output = svg_draw_job.output()
            #
            # txt_output += '完成 ok \n'

            # if os.path.exists('JSON/' + filename):
            #     os.remove('JSON/' + filename)

    f = open('log.txt', 'w', encoding='utf-8')
    f.write(txt_output)
    f.close()

def check_output_folder(path):
    folders = ['west_link_north', 'west_link_south', 'west_link_moutain', 'west_link_sea',
               'pingtung', 'south_link', 'taitung', 'north_link', 'yilan',
               'pingxi', 'neiwan', 'jiji', 'shalun']

    output_folder = os.listdir(path)

    diff = list(set(folders).difference(set(output_folder)))

    if len(diff) > 0:
        for item in diff:
            os.makedirs(path + '/' + item)


if __name__ == "__main__":
    if len(sys.argv) == 4:
        argv_json_location = sys.argv[1]  # 參數1: JSON 檔位置
        argv_website_svg_location = sys.argv[2]  # 參數2: 運行圖檔案存檔位置
        argv_train_no = sys.argv[3]  # 參數3: 特定車次繪製
    else:
        argv_json_location = 'JSON'
        argv_website_svg_location = 'OUTPUT'
        argv_train_no = 'ALL'

    if argv_train_no == 'ALL':
        argv_train_no = ''

    main(argv_json_location, argv_website_svg_location, argv_train_no)
