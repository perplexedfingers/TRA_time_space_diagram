import json
import pathlib
import sqlite3
from datetime import timedelta

NEXT_DAY = timedelta(days=1)

CAR_CLASS = {  # copy from developer manual
    '1101': '自強(太,障)',
    '1105': '自強(郵)',
    '1104': '自強(專)',
    '1112': '莒光(專)',
    '1120': '復興',
    '1131': '區間車',
    '1132': '區間快',
    '1140': '普快車',
    '1141': '柴快車',
    '1150': '普通車(專)',
    '1151': '普通車',
    '1152': '行包專車',
    '1134': '兩鐵(專)',
    '1270': '普通貨車',
    '1280': '客迴',
    '1281': '柴迴',
    '12A0': '調車列車',
    '12A1': '單機迴送',
    '12B0': '試運轉',
    '4200': '特種(戰)',
    '5230': '特種(警)',
    '1111': '莒光(障)',
    '1103': '自強(障)',
    '1102': '自強(腳,障)',
    '1100': '自強',
    '1110': '莒光',
    '1121': '復興(專)',
    '1122': '復興(郵)',
    '1113': '莒光(郵)',
    '1282': '臨時客迴',
    '1130': '電車(專)',
    '1133': '電車(郵)',
    '1154': '柴客(專)',
    '1155': '柴客(郵)',
    '1107': '自強(普,障)',
    '1135': '區間車(腳,障)',
    '1108': '自強(PP障)',
    '1114': '莒光(腳)',
    '1115': '莒光(腳,障)',
    '1109': '自強(PP親)',
    '110A': '自強(PP障12)',
    '110B': '自強(E12)',
    '110C': '自強(E3)',
    '110D': '自強(D28)',
    '110E': '自強(D29)',
    '110F': '自強(D31)',
    '1106': '自強(商專)',
}


def create_schema(con: sqlite3.Connection):
    with con:
        con.execute(
            '''
            CREATE TABLE IF NOT EXISTS station
            (
            pk INTEGER PRIMARY KEY AUTOINCREMENT
            ,code TEXT UNIQUE NOT NULL
            ,is_active bool DEFAULT 1
            )
            '''
        )  # custom sqlite type

        con.execute(
            '''
            CREATE TABLE IF NOT EXISTS station_name_cht
            (
            pk INTEGER PRIMARY KEY AUTOINCREMENT
            ,station_fk REFERENCES station ON DELETE CASCADE
            ,name TEXT NOT NULL
            )
            '''
        )

        con.execute(
            '''
            CREATE TABLE IF NOT EXISTS route
            (
            pk INTEGER PRIMARY KEY AUTOINCREMENT
            ,name TEXT NOT NULL UNIQUE
            )
            '''
        )

        con.execute(
            '''
            CREATE TABLE IF NOT EXISTS route_station
            (
            pk INTEGER PRIMARY KEY AUTOINCREMENT
            ,route_fk REFERENCES route ON DELETE CASCADE
            ,station_fk REFERENCES station ON DELETE CASCADE
            ,relative_distance REAL NOT NULL
            )
            '''
        )

        con.execute(
            '''
            CREATE TABLE IF NOT EXISTS train_type
            (
            pk INTEGER PRIMARY KEY AUTOINCREMENT
            ,code TEXT UNIQUE NOT NULL
            )
            '''
        )

        con.execute(
            '''
            CREATE TABLE IF NOT EXISTS train_type_name_cht
            (
            pk INTEGER PRIMARY KEY AUTOINCREMENT
            ,train_type_fk REFERENCES train_type ON DELETE CASCADE
            ,name TEXT NOT NULL
            )
            '''
        )

        con.execute(
            '''
            CREATE TABLE IF NOT EXISTS train
            (
            pk INTEGER PRIMARY KEY AUTOINCREMENT
            ,train_type_fk REFERENCES train_type ON DELETE CASCADE
            ,code TEXT NOT NULL
            )
            '''
        )

        con.execute(
            '''
            CREATE TABLE IF NOT EXISTS timetable
            (
            pk INTEGER PRIMARY KEY AUTOINCREMENT
            ,station_fk REFERENCES train_type ON DELETE CASCADE
            ,train_fk REFERENCES train ON DELETE CASCADE
            ,arrive_time time NOT NULL
            ,departure_time time NOT NULL
            )
            '''
        )  # custom sqlite type


def fill_in_stations(cur: sqlite3.Cursor, station: pathlib.Path):
    with station.open() as f:
        station_json = json.load(f)
    for station in station_json:
        cur.execute(
            'INSERT INTO station (code) VALUES (?)',
            (station['stationCode'],)
        )
        cur.execute(
            'SELECT pk FROM station WHERE code=:code',
            {'code': station['stationCode']})
        station_row = cur.fetchone()
        cur.execute(
            'INSERT INTO station_name_cht (station_fk, name) VALUES (?, ?)',
            (station_row['pk'], station['name'])
        )


def fill_in_routes(cur: sqlite3.Cursor, route: pathlib.Path):
    with route.open() as f:
        route_json = json.load(f)
    for route_info in route_json:
        cur.execute(
            'INSERT OR IGNORE INTO route (name) VALUES (?)',
            (route_info['lineName'],)
        )
        cur.execute(
            'SELECT pk FROM route WHERE name=:name',
            {'name': route_info['lineName']}
        )
        route_row = cur.fetchone()
        cur.execute(
            'SELECT pk FROM station WHERE code=:code',
            {'code': route_info['fkSta']}
        )
        station_row = cur.fetchone()
        if station_row:
            cur.execute(
                'UPDATE station SET is_active=:bool WHERE pk=:pk',
                {'pk': station_row['pk'], 'bool': True}
            )
            cur.execute(
                'INSERT INTO route_station (route_fk, station_fk, relative_distance) VALUES (?, ?, ?)',
                (route_row['pk'], station_row['pk'], float(route_info['staMil']) * 20)
                # Enlarge the gap between stations
            )
        else:
            cur.execute(
                'UPDATE station SET is_active=:bool WHERE code=:code',
                {'code': route_info['fkSta'], 'bool': False}
            )


def get_order(item) -> int:
    return int(item['Order'])


def iso_time_to_timedelta(iso: str) -> timedelta:
    hour, minute, second = iso.split(':')
    return timedelta(hours=int(hour), minutes=int(minute), seconds=int(second))


def fill_in_timetable(cur: sqlite3.Cursor, timetable: pathlib.Path):
    with timetable.open() as f:
        timetable_json = json.load(f)
    for train in timetable_json['TrainInfos']:
        cur.execute(
            'INSERT OR IGNORE INTO train_type (code) VALUES (?)',
            (train['CarClass'],)
        )
        cur.execute(
            'SELECT pk FROM train_type WHERE code=:code',
            {'code': train['CarClass']}
        )
        train_type_row = cur.fetchone()
        cur.execute(
            'INSERT OR IGNORE INTO train_type_name_cht (train_type_fk, name) VALUES (?, ?)',
            (train_type_row['pk'], CAR_CLASS[train['CarClass']])
        )
        cur.execute(
            'INSERT OR IGNORE INTO train (train_type_fk, code) VALUES (?, ?)',
            (train_type_row['pk'], train['Train'])
        )
        cur.execute(
            'SELECT pk FROM train WHERE code=:code',
            {'code': train['Train']}
        )
        train_row = cur.fetchone()
        is_overnight = len(train['OverNightStn']) > 0
        start_time = None
        sql_statement = 'INSERT INTO timetable (station_fk, train_fk, arrive_time, departure_time) VALUES (?, ?, ?, ?)'
        for time_info in sorted(train['TimeInfos'], key=get_order):
            arrive_time = iso_time_to_timedelta(time_info['ARRTime'])
            departure_time = iso_time_to_timedelta(time_info['DEPTime'])
            cur.execute(
                'SELECT pk FROM station WHERE code=:code',
                {'code': time_info['Station']})
            station_row = cur.fetchone()
            if is_overnight:
                if start_time is None:
                    start_time = arrive_time
                elif start_time > arrive_time:
                    cur.execute(
                        sql_statement,
                        (station_row['pk'], train_row['pk'],
                         arrive_time + NEXT_DAY,
                         departure_time + NEXT_DAY
                         )
                    )
            cur.execute(
                sql_statement,
                (station_row['pk'], train_row['pk'],
                 arrive_time,
                 departure_time
                 )
            )


def load_data_from_json(
    cur: sqlite3.Cursor, route: pathlib.Path,
        station: pathlib.Path, timetable: pathlib.Path):
    # must be in this order
    fill_in_stations(cur, station)
    fill_in_routes(cur, route)
    fill_in_timetable(cur, timetable)


def adapt_time(t: timedelta) -> int:
    return round(t.total_seconds())


def convert_time(digits: bytes) -> timedelta:
    number = int(digits)
    seconds = number % 60
    m = number // 60
    hours = m // 60
    minutes = m % 60
    return timedelta(hours=hours, minutes=minutes, seconds=seconds)


def adapt_bool(b: bool) -> int:
    return int(b)


def convert_bool(b: bytes) -> bool:
    return bool(b)


def setup_sqlite(db_location: str = ':memory:') -> sqlite3.Connection:
    sqlite3.register_adapter(timedelta, adapt_time)
    sqlite3.register_converter('time', convert_time)
    sqlite3.register_adapter(bool, adapt_bool)
    sqlite3.register_converter('bool', convert_bool)
    con = sqlite3.connect(db_location, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    return con


if __name__ == '__main__':
    con = setup_sqlite()
    create_schema(con)
    cur = con.cursor()
    load_data_from_json(
        cur,
        pathlib.Path('./JSON/route.json'),
        pathlib.Path('./JSON/station.json'),
        pathlib.Path('./JSON/timetable.json'),
    )
