# -*- coding: utf-8 -*-
import argparse
import sys
import os
import shutil
import pathlib
import json

import read_tra_json as tra_json
import data_process as dp
import diagram_maker as dm
from progessbar import progress


# sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
version = '1.021'

lines_diagram_setting = {
    'LINE_WN': ('/west_link_north/WESTNORTH_', 'LINE_WN', 3000),
    'LINE_WM': ('/west_link_moutain/WESTMOUNTAIN_', 'LINE_WM', 2000),
    'LINE_WSEA': ('/west_link_sea/WESTSEA_', 'LINE_WSEA', 2000),
    'LINE_WS': ('/west_link_south/WESTSOUTH_', 'LINE_WS', 4000),
    'LINE_P': ('/pingtung/PINGTUNG_', 'LINE_P', 2000),
    'LINE_S': ('/south_link/SOUTHLINK_', 'LINE_S', 2000),
    'LINE_T': ('/taitung/TAITUNG_', 'LINE_T', 2000),
    'LINE_N': ('/north_link/NORTHLINK_', 'LINE_N', 2000),
    'LINE_I': ('/yilan/YILAN_', 'LINE_I', 2000),
    'LINE_PX': ('/pingxi/PINGXI_', 'LINE_PX', 1250),
    'LINE_NW': ('/neiwan/NEIWAN_', 'LINE_NW', 1250),
    'LINE_J': ('/jiji/JIJI_', 'LINE_J', 1250),
    'LINE_SL': ('/shalun/SHALUN_', 'LINE_SL', 650)}

lines_diagram_setting_test = {
    'LINE_WN': ('/WESTNORTH_', 'LINE_WN', 3000),
    'LINE_WM': ('/WESTMOUNTAIN_', 'LINE_WM', 2000),
    'LINE_WSEA': ('/WESTSEA_', 'LINE_WSEA', 2000),
    'LINE_WS': ('/WESTSOUTH_', 'LINE_WS', 4000),
    'LINE_P': ('/PINGTUNG_', 'LINE_P', 2000),
    'LINE_S': ('/SOUTHLINK_', 'LINE_S', 2000),
    'LINE_T': ('/TAITUNG_', 'LINE_T', 2000),
    'LINE_N': ('/NORTHLINK_', 'LINE_N', 2000),
    'LINE_I': ('/YILAN_', 'LINE_I', 2000),
    'LINE_PX': ('/PINGXI_', 'LINE_PX', 1250),
    'LINE_NW': ('/NEIWAN_', 'LINE_NW', 1250),
    'LINE_J': ('/JIJI_', 'LINE_J', 1250),
    'LINE_SL': ('/SHALUN_', 'LINE_SL', 650)}

diagram_hours = (
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
    14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 1, 2, 3, 4, 5, 6)


# 程式執行段
# def main(argv_json_location, argv_website_svg_location, argv_select_trains, move_file):
def main(json_folder: pathlib.Path, svg_folder: pathlib.Path, train_numbers: [int]):
    # json_files = []
    # all_after_midnight_data = []

    check_output_folder(svg_folder)

    json_files = json_folder.glob('*.json')

    for json_file in json_files:
        # 讀取 JSON 檔案，可選擇特定車次(train_numbers)
        with json_file.open() as f:
            timetable = json.load(f)
        if train_numbers:
            all_trains_json = [train for train in timetable['TrainInfos']
                               if train['Train'] in train_numbers]
        else:
            all_trains_json = [train for train in timetable['TrainInfos']]

        for train_info in all_trains_json:
            train_id = train_info['Train']  # 車次號
            car_class = train_info['CarClass']  # 車種
            line = train_info['Line']  # 山線1、海線2、其他0
            direction = train_info['LineDir']  # 順逆行
            overnight_station = train_info['OverNightStn']  # 跨午夜車站

            # 查詢表定台鐵時刻表所有「停靠」車站，可查詢特定車次
            dict_start_end_station = {time_info['Station']: [time_info['ARRTime'], time_info['DEPTime'],
                                                             time_info['Station'], time_info['Order']]
                                      for time_info in train_info['TimeInfos']
                                      # if time_info['station'] not in dict_start_end_station
                                      }
            _dict_start_end_station = dp.find_train_stations(train_info)
            assert dict_start_end_station == _dict_start_end_station

            # 查詢特定車次所有「停靠與通過」車站
            list_passing_stations = dp.find_passing_stations(dict_start_end_station,
                                                             line,
                                                             direction)

            # 推算所有通過車站的時間與位置
            list_train_time_space = dp.estimate_time_space(dict_start_end_station, list_passing_stations)

            for key, value in list_train_time_space[0].items():
                all_trains_data.append([key, train_id, car_class, line, overnight_station, None, value])

            for key, value in list_train_time_space[1].items():
                all_after_midnight_data.append([key, train_id, car_class, line, over_night_stn, "midnight", value])

            for key, value in list_train_time_space[2].items():
                all_trains_data.append(["LINE_WN", train_id, car_class, line,
                                        over_night_stn, key + train_id, value])

        # 繪製運行圖
        dm.TimeSpaceDiagram(lines_diagram_setting,
                            all_trains_data,
                            svg_folder,
                            file_date.split('.')[0],
                            diagram_hours,
                            version)


def check_output_folder(path: pathlib.Path):
    folders = (
        'west_link_north', 'west_link_south', 'west_link_moutain', 'west_link_sea',
        'pingtung', 'south_link', 'taitung', 'north_link', 'yilan',
        'pingxi', 'neiwan', 'jiji', 'shalun')

    for f in folders:
        (path / f).mkdir(exist_ok=True)


# def _print_usage(name):
#     print('usage : ' + name +
#           ' [-d] [-f] [-h] [-i inputfolder] [-o outputfolder] [--delete] [--force] [--help] [--inputfolder inputfolder] [--outputfolder outputfolder] [trainno ...]')
#     exit()


if __name__ == "__main__":
    desc =\
        'Draw train time-space diagram accroding to timetable JSON file.'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-i', type=pathlib.Path, default='JSON', dest='timetable_json_folder',
                        help='Timetable JSON file folder')
    parser.add_argument('-o', type=pathlib.Path, default='OUTPUT', dest='output_svg_folder',
                        help='Output SVG folder')
    parser.add_argument('specified_train_numbers', type=int, nargs='*', help='Multiple specified train number')
    args = parser.parse_args()

    main(json_folder=args.timetable_json_folder, svg_folder=args.output_svg_folder,
         train_numbers=args.specified_train_numbers)
    print('Done')
    # else:
    #     print('************************************')
    #     print('台鐵JSON轉檔運行圖程式 - 版本：' + version)
    #     print('************************************\n')

    #     Parameters = []
    #     action = input('您需要執行特定車次嗎？不需要請直接輸入Enter，或者輸入 "Y"：\n')
    #     if action.lower() == 'y':
    #         select_trains = []
    #         while True:
    #             action = input('請問特定車次號碼？(請輸入車次號，如果有多個車次要選擇，請不斷輸入，要結束直接輸入Enter)：\n')
    #             if action != '':
    #                 select_trains.append(action)
    #             else:
    #                 break
    #         Parameters.append(select_trains)
    #     else:
    #         Parameters.append([])
    #     main(Parameters[0], Parameters[1], Parameters[2], Parameters[3])
