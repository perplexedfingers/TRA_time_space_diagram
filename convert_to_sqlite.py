from __future__ import annotations

import json
import pathlib
import sqlite3
from datetime import timedelta
from itertools import groupby
from operator import itemgetter
from typing import Union

NEXT_DAY = timedelta(days=1)
FIRST_HOUR = timedelta(hours=1)

CAR_CLASS = {  # copy from developer manual
    '1101': '自強(太,障)',
    '1105': '自強(郵)',
    '1104': '自強(專)',
    '1103': '自強(障)',
    '1102': '自強(腳,障)',
    '1100': '自強',
    '1109': '自強(PP親)',
    '110A': '自強(PP障12)',
    '110B': '自強(E12)',
    '110C': '自強(E3)',
    '110D': '自強(D28)',
    '110E': '自強(D29)',
    '110F': '自強(D31)',
    '1106': '自強(商專)',
    '1107': '自強(普,障)',
    '1108': '自強(PP障)',

    '1110': '莒光',
    '1114': '莒光(腳)',
    '1115': '莒光(腳,障)',
    '1112': '莒光(專)',
    '1111': '莒光(障)',
    '1113': '莒光(郵)',

    '1120': '復興',
    '1121': '復興(專)',
    '1122': '復興(郵)',

    '1131': '區間車',
    '1135': '區間車(腳,障)',
    '1132': '區間快',
    '1134': '兩鐵(專)',
    '1130': '電車(專)',
    '1133': '電車(郵)',

    '1140': '普快車',
    '1141': '柴快車',

    '1150': '普通車(專)',
    '1151': '普通車',
    '1154': '柴客(專)',
    '1155': '柴客(郵)',
    '1152': '行包專車',

    '1270': '普通貨車',

    '1280': '客迴',
    '1282': '臨時客迴',
    '1281': '柴迴',

    '12A0': '調車列車',
    '12A1': '單機迴送',

    '12B0': '試運轉',

    '4200': '特種(戰)',

    '5230': '特種(警)',
}


def create_schema(con: sqlite3.Connection):
    cur = con.cursor()
    cur.executescript(
        '''
        CREATE TABLE IF NOT EXISTS station
        (
        pk INTEGER PRIMARY KEY AUTOINCREMENT
        ,code TEXT UNIQUE NOT NULL
        ,is_active bool DEFAULT 0 NOT NULL
        );

        CREATE TABLE IF NOT EXISTS station_name_cht
        (
        pk INTEGER PRIMARY KEY AUTOINCREMENT
        ,station_fk REFERENCES station ON DELETE CASCADE
        ,name TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS route
        (
        pk INTEGER PRIMARY KEY AUTOINCREMENT
        ,name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS route_station
        (
        pk INTEGER PRIMARY KEY AUTOINCREMENT
        ,route_fk REFERENCES route ON DELETE CASCADE
        ,station_fk REFERENCES station ON DELETE CASCADE
        ,relative_distance REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS train_type
        (
        pk INTEGER PRIMARY KEY AUTOINCREMENT
        ,code TEXT UNIQUE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS train_type_name_cht
        (
        pk INTEGER PRIMARY KEY AUTOINCREMENT
        ,train_type_fk REFERENCES train_type ON DELETE CASCADE
        ,name TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS train
        (
        pk INTEGER PRIMARY KEY AUTOINCREMENT
        ,train_type_fk REFERENCES train_type ON DELETE CASCADE
        ,code TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS timetable
        (
        pk INTEGER PRIMARY KEY AUTOINCREMENT
        ,station_fk REFERENCES train_type ON DELETE CASCADE
        ,train_fk REFERENCES train ON DELETE CASCADE
        ,previous REFERENCE timetable NULL
        ,next REFERENCE timetable NULL
        ,time t_time NOT NULL
        );
        '''
    )


def fill_in_stations(cur: sqlite3.Cursor, station: pathlib.Path):
    with station.open() as f:
        station_json = json.load(f)
    for station in station_json:
        cur.execute(
            'INSERT INTO station (code) VALUES (?) RETURNING pk',
            (station['stationCode'],)
        )
        station_pk = cur.fetchone()['pk']
        cur.execute(
            'INSERT INTO station_name_cht (station_fk, name) VALUES (:pk, :name)',
            {'pk': station_pk, 'name': station['name']}
        )


def fill_in_routes(cur: sqlite3.Cursor, route: pathlib.Path):
    with route.open() as f:
        route_json = json.load(f)
    for line_name, routes in groupby(sorted(route_json, key=itemgetter('lineName')), key=itemgetter('lineName')):
        cur.execute(
            'INSERT INTO route (name) VALUES (?) RETURNING route.pk',
            (line_name,)
        )
        route_pk = cur.fetchone()['pk']
        for route_info in routes:
            cur.execute(
                'SELECT pk FROM station WHERE code=?',
                (route_info['fkSta'],)
            )
            station_row = cur.fetchone()
            if station_row:
                station_pk = station_row['pk']
                cur.execute(
                    'UPDATE station SET is_active=:bool WHERE pk=:pk',
                    {'pk': station_pk, 'bool': True}
                )
                cur.execute(
                    '''
                    INSERT INTO
                        route_station
                        (route_fk, station_fk, relative_distance)
                    VALUES
                        (:route_pk, :station_pk, :distance)
                    ''',
                    {'route_pk': route_pk, 'station_pk': station_pk,
                     'distance': float(route_info['staMil'])}
                )


def get_order(item) -> int:
    return int(item['Order'])


def iso_time_to_timedelta(iso: str) -> timedelta:
    hour, minute, second = iso.split(':')
    return timedelta(hours=int(hour), minutes=int(minute), seconds=int(second))


def fill_in_timetable(cur: sqlite3.Cursor, timetable: pathlib.Path):
    with timetable.open() as f:
        timetable_json = json.load(f)
    for train_type, trains in groupby(
            sorted(timetable_json['TrainInfos'],
                   key=itemgetter('CarClass')),
            key=itemgetter('CarClass')):
        cur.execute(
            'INSERT INTO train_type (code) VALUES (?) RETURNING train_type.pk',
            (train_type,)
        )
        train_type_pk = cur.fetchone()['pk']
        cur.execute(
            'INSERT INTO train_type_name_cht (train_type_fk, name) VALUES (:train_type_pk, :name)',
            {'train_type_pk': train_type_pk, 'name': CAR_CLASS[train_type]}
        )
        for train in trains:
            cur.execute(
                'INSERT OR IGNORE INTO train (train_type_fk, code) VALUES (:train_type_pk, :code) RETURNING train.pk',
                {'train_type_pk': train_type_pk, 'code': train['Train']}
            )
            train_pk = cur.fetchone()['pk']
            over_night_station = train['OverNightStn']
            previous = None
            start_over_night_route = False
            for time_info in sorted(train['TimeInfos'], key=get_order):
                station_code = time_info['Station']
                cur.execute(
                    'SELECT pk FROM station WHERE code=?',
                    (station_code,))
                station_pk = cur.fetchone()['pk']
                for key in ('ARRTime', 'DEPTime'):
                    time_ = iso_time_to_timedelta(time_info[key])
                    if station_code == over_night_station:
                        start_over_night_route = True
                        # it seems that no train would travel/stay for
                        # more than one hour around midnight
                        # so we have this hack
                        if time_ < FIRST_HOUR:
                            time_ += NEXT_DAY
                    elif start_over_night_route:
                        time_ += NEXT_DAY
                    cur.execute(
                        '''
                        INSERT INTO
                            timetable
                            (station_fk, train_fk, time, previous)
                        VALUES
                            (:station_pk, :train_pk, :time, :previous)
                        RETURNING
                            timetable.pk
                        ''',
                        {'station_pk': station_pk, 'train_pk': train_pk, 'time': time_, 'previous': previous}
                    )
                    current = cur.fetchone()['pk']
                    if previous:
                        cur.execute(
                            'UPDATE timetable SET next=:current WHERE pk=:previous',
                            {'current': current, 'previous': previous}
                        )
                    previous = current


def patch_stations(cur):
    cur.execute(
        'SELECT station.pk FROM station WHERE station.code="3330"'
    )
    wuri_pk = cur.fetchone()['pk']
    cur.execute(
        'UPDATE route_station SET relative_distance = 200.5 WHERE station_fk = ?',
        (wuri_pk,)
    )

    cur.execute(
        'SELECT station.pk FROM station WHERE station.code="1180"'
    )
    zhubei_pk = cur.fetchone()['pk']
    cur.execute(
        'UPDATE route_station SET relative_distance = 100.6 WHERE station_fk = ?',
        (zhubei_pk,)
    )

    cur.execute(
        'SELECT station.pk FROM station WHERE station.code="6030"'
    )
    ruiyuan_pk = cur.fetchone()['pk']
    cur.execute(
        'UPDATE route_station SET relative_distance = 300.5 WHERE station_fk = ?',
        (ruiyuan_pk,)
    )


def load_data_from_json(
    con: sqlite3.Connection, route: pathlib.Path,
        station: pathlib.Path, timetable: pathlib.Path):
    cur = con.cursor()
    # Due to database schema, must be in this order
    fill_in_stations(cur, station)
    fill_in_routes(cur, route)
    fill_in_timetable(cur, timetable)
    patch_stations(cur)


def adapt_time(t: timedelta) -> int:
    return round(t.total_seconds())


def convert_time(digits: Union[int, bytes]) -> timedelta:
    return timedelta(seconds=int(digits))


def adapt_bool(b: bool) -> int:
    return int(b)


def convert_bool(b: bytes) -> bool:
    return bool(b)


def setup_sqlite(db_location: str = ':memory:') -> sqlite3.Connection:
    sqlite3.register_adapter(timedelta, adapt_time)
    sqlite3.register_converter('t_time', convert_time)
    sqlite3.register_adapter(bool, adapt_bool)
    sqlite3.register_converter('bool', convert_bool)
    con = sqlite3.connect(db_location, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    return con


if __name__ == '__main__':
    con = setup_sqlite()
    with con:
        create_schema(con)
        load_data_from_json(
            con,
            pathlib.Path('./JSON/route.json'),
            pathlib.Path('./JSON/station.json'),
            pathlib.Path('./JSON/timetable.json'),
        )
