from __future__ import annotations

import argparse
import json
import sqlite3
from collections import namedtuple
from datetime import timedelta
from functools import partial, reduce
from itertools import chain, filterfalse, groupby, tee
from operator import itemgetter
from pathlib import Path
from typing import Callable, Generator, Union

from pypika import Parameter
from pypika import PostgreSQLQuery as Query  # Only this supports 'RETURING'
from pypika import Table

CAR_CLASS = {  # copy from developer manual in timetable webpage
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

    tables =\
        (
            station_table,
            station_name_cht_table,
            route_table,
            route_station_table,
            train_type_table,
            train_type_name_cht_table,
            train_table,
            timetable_table,
        ) =\
        (
            Query.create_table('station'),
            Query.create_table('station_name_cht'),
            Query.create_table('route'),
            Query.create_table('route_station'),
            Query.create_table('train_type'),
            Query.create_table('train_type_name_cht'),
            Query.create_table('train'),
            Query.create_table('timetable')
        )

    station_table.columns(
        ('pk', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
        ('code', 'TEXT UNIQUE NOT NULL'),
        ('is_active', 'INTEGER DEFAULT 0 NOT NULL')
    )

    station_name_cht_table.columns(
        ('pk', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
        ('station_fk', 'REFERENCES station ON DELETE CASCADE'),
        ('name', 'TEXT NOT NULL')
    )

    route_table.columns(
        ('pk', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
        ('name', 'TEXT NOT NULL UNIQUE'),
    )

    route_station_table.columns(
        ('pk', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
        ('route_fk', 'REFERENCES route ON DELETE CASCADE'),
        ('station_fk', 'REFERENCES station ON DELETE CASCADE'),
        ('relative_distance', 'REAL NOT NULL'),
    )

    train_type_table.columns(
        ('pk', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
        ('code', 'TEXT UNIQUE NOT NULL'),
    )

    train_type_name_cht_table.columns(
        ('pk', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
        ('train_type_fk', 'REFERENCES train_type ON DELETE CASCADE'),
        ('name', 'TEXT NOT NULL'),
    )

    train_table.columns(
        ('pk', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
        ('train_type_fk', 'REFERENCES train_type ON DELETE CASCADE'),
        ('code', 'TEXT NOT NULL'),
    )

    timetable_table.columns(
        ('pk', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
        ('station_fk', 'REFERENCES train_type ON DELETE CASCADE'),
        ('train_fk', 'REFERENCES train ON DELETE CASCADE'),
        ('previous', 'REFERENCE timetable NULL'),
        ('next', 'REFERENCE timetable NULL'),
        ('time', 't_time NOT NULL'),
        ('order_', 'INTEGER NOT NULL'),
    )

    cur.executescript(
        ';'.join(t.get_sql() for t in tables)
    )


def fill_in_stations(cur: sqlite3.Cursor, station: Path):
    with station.open() as f:
        station_json = json.load(f)
    insert_station_code = (
        Query.into('station')
        .columns('code').insert(Parameter('?'))
        .returning('pk').get_sql())
    insert_station_name = (
        Query.into('station_name_cht')
        .columns('station_fk', 'name')
        .insert(Parameter(':pk'), Parameter(':name'))
        .get_sql()
    )
    for station in station_json:
        cur.execute(
            insert_station_code, (station['stationCode'],)
        )
        station_pk = cur.fetchone()['pk']
        cur.execute(
            insert_station_name, {'pk': station_pk, 'name': station['name']}
        )


# https://github.com/billy1125/TRA_time_space_diagram/blob/master/CSV/Category.csv
irregular_stations = {
    '7075': '觀音號誌',
    '7115': '永春',
    '5173': '中央號誌',
    '5177': '善安號誌',
    '5180': '古莊號誌',
    '5195': '富山號誌',
    '5105': '多良',
    '5205': '香蘭',
    '5215': '三和',
    '6115': '大禹',
    '6135': '瑞北',
    '6245': '干城',
    # ? 田蒲 between 6250~7000. Not found
    # ? 北回 between 4080~4090. Not found
    # ? 南臺南 between 4220~4250. Multiple
}


def fill_in_routes(cur: sqlite3.Cursor, route: Path):
    with route.open() as f:
        route_json = json.load(f)
    station_table = Table('station')
    insert_route_name = Query.into('route')\
        .columns('name').insert(Parameter('?'))\
        .returning('pk').get_sql()
    select_station_pk = Query.from_(station_table)\
        .select('pk').where(station_table.code == Parameter('?')).get_sql()
    update_active_station = Query.update(station_table)\
        .set('is_active', 1)\
        .where(station_table.pk == Parameter('?')).get_sql()
    insert_inactive_station = Query.into(station_table)\
        .columns('code', 'is_active').insert(Parameter('?'), 0)\
        .returning('pk').get_sql()
    insert_station_name = Query.into('station_name_cht')\
        .columns('station_fk', 'name').insert(Parameter(':pk'), Parameter(':name')).get_sql()
    insert_relative_distance_on_route_of_a_station = Query.into('route_station')\
        .columns('route_fk', 'station_fk', 'relative_distance')\
        .insert(Parameter(':route_pk'), Parameter(':station_pk'), Parameter(':distance'))\
        .get_sql()
    for route_name, routes in groupby(sorted(route_json, key=itemgetter('lineName')), key=itemgetter('lineName')):
        cur.execute(insert_route_name, (route_name,))
        route_pk = cur.fetchone()['pk']
        for route_info in routes:
            station_code = route_info['fkSta']
            cur.execute(select_station_pk, (station_code,))
            station_row = cur.fetchone()
            if station_row:
                station_pk = station_row['pk']
                cur.execute(update_active_station, (station_pk,))
            else:
                cur.execute(insert_inactive_station, (station_code,))
                station_pk = cur.fetchone()['pk']
                cur.execute(
                    insert_station_name,
                    {'pk': station_pk, 'name': irregular_stations.get(station_code, '')}
                )
            cur.execute(
                insert_relative_distance_on_route_of_a_station,
                {'route_pk': route_pk, 'station_pk': station_pk,
                 'distance': float(route_info['staMil'])}
            )


def get_order(item: dict[str, str]) -> int:
    return int(item['Order'])


def iso_time_to_timedelta(iso: str) -> timedelta:
    hour, minute, second = iso.split(':')
    return timedelta(hours=int(hour), minutes=int(minute), seconds=int(second))


Info = namedtuple('Info', ['order', 'key', 'time', 'station_pk'])


def mutate_info(item: dict[str, str], cur: sqlite3.Cursor) -> Info:
    station_table = Table('station')
    cur.execute(
        Query.from_(station_table)
        .where(station_table.code == Parameter('?'))
        .select('pk').get_sql(),
        (item['Station'],))
    station_pk = cur.fetchone()['pk']
    for key in ('ARRTime', 'DEPTime'):
        yield Info(
            order=get_order(item),
            key=key, station_pk=station_pk,
            time=iso_time_to_timedelta(item[key])
        )


def partition(pred: Callable[[Info], bool], iterable: [Info]) -> tuple(Generator[Info], Generator[Info]):
    "Use a predicate to partition entries into false entries and true entries"
    # partition(is_odd, range(10)) --> 0 2 4 6 8   and  1 3 5 7 9
    t1, t2 = tee(iterable)
    return filterfalse(pred, t1), filter(pred, t2)


def is_corner_case(order: int, last_order: int, key: str, time_: timedelta) -> bool:
    '''
    Arrive at its last stop before midnight,
    stay for some time, and
    out of service after midnight
    '''
    guess_last_stop_max_stay_time = timedelta(minutes=30)
    return (order == last_order and key == 'DEPTime' and time_ < guess_last_stop_max_stay_time)


def need_to_adjust_time(info: Info, over_night_order: int, last_order: int) -> bool:
    guessed_over_night_stop_max_stay_time = timedelta(hours=1)
    return (info.order == over_night_order and info.time < guessed_over_night_stop_max_stay_time)\
        or info.order > over_night_order\
        or is_corner_case(info.order, last_order, info.key, info.time)


def insert_(last_pk: Union[None, int], current: Info, cur: sqlite3.Cursor, train_pk: int) -> int:
    input_ = {
        'station_pk': current.station_pk, 'train_pk': train_pk,
        'time': current.time, 'previous': None, 'order': current.order,
    }
    if last_pk:
        input_['previous'] = last_pk
    cur.execute(
        Query.into('timetable')
        .columns('station_fk', 'train_fk', 'time', 'previous', 'order_')
        .insert(Parameter(':station_pk'), Parameter(':train_pk'),
                Parameter(':time'), Parameter(':previous'), Parameter(':order'))
        .returning('pk').get_sql(),
        input_)
    return cur.fetchone()['pk']


def insert_points_of_time(cur: sqlite3.Cursor, time_infos: list[dict[str, str]],
                          over_night_station_order: int, train: str, train_pk: int,
                          last_order: int):
    mutant = (xx for x in time_infos for xx in mutate_info(item=x, cur=cur))
    before_midnight, after_midnight = partition(
        partial(need_to_adjust_time, over_night_order=over_night_station_order, last_order=last_order),
        mutant)
    adjusted_infos = map(
        lambda x: Info(order=x.order, key=x.key, station_pk=x.station_pk, time=x.time + timedelta(days=1)),
        after_midnight)
    reduce(
        partial(insert_, cur=cur, train_pk=train_pk),
        sorted(chain(before_midnight, adjusted_infos), key=lambda x: (x.order, x.key)),
        None)


def get_over_night_station_order(station_code: Union[None, int], infos: list[dict[str, str]]) -> float:
    if station_code:
        order = next((float(info['Order']) for info in infos if info['Station'] == station_code))
    else:
        order = float('inf')
    return order


def fill_in_timetable(cur: sqlite3.Cursor, timetable: Path):
    with timetable.open() as f:
        timetable_json = json.load(f)
    insert_train_type = Query.into('train_type')\
        .columns('code').insert(Parameter('?')).returning('pk').get_sql()
    insert_train_type_name = Query.into('train_type_name_cht')\
        .columns('train_type_fk', 'name')\
        .insert(Parameter(':train_type_pk'), Parameter(':name')).get_sql()
    connect_train_n_train_type = Query.into('train')\
        .columns('train_type_fk', 'code')\
        .insert(Parameter(':train_type_pk'), Parameter(':code'))\
        .returning('pk').get_sql()
    for train_type, trains in groupby(
            sorted(timetable_json['TrainInfos'],
                   key=itemgetter('CarClass')),
            key=itemgetter('CarClass')):
        cur.execute(insert_train_type, (train_type,))
        train_type_pk = cur.fetchone()['pk']
        cur.execute(
            insert_train_type_name,
            {'train_type_pk': train_type_pk, 'name': CAR_CLASS[train_type]}
        )
        for train in trains:
            cur.execute(
                connect_train_n_train_type,
                {'train_type_pk': train_type_pk, 'code': train['Train']}
            )
            train_pk = cur.fetchone()['pk']
            over_night_station_order = get_over_night_station_order(train['OverNightStn'], train['TimeInfos'])
            last_order = get_order(max(train['TimeInfos'], key=get_order))
            insert_points_of_time(cur, train['TimeInfos'], over_night_station_order, train, train_pk, last_order)


def patch_stations(cur: sqlite3.Cursor):
    station_table = Table('station')
    route_station_table = Table('route_station')
    select_station_pk = Query.from_(station_table)\
        .where(station_table.code == Parameter('?')).select('pk').get_sql()
    update_station_distaince = Query.update(route_station_table)\
        .set('relative_distance', Parameter(':distance'))\
        .where(route_station_table.station_fk == Parameter(':pk')).get_sql()

    data = (
        ('3330', 200.5),  # WuRi
        ('1180', 100.6),  # ZhuBei
        ('6030', 300.5),  # RuiYuan
    )
    for code, distance in data:
        cur.execute(
            select_station_pk, (code,)
        )
        pk = cur.fetchone()['pk']
        cur.execute(update_station_distaince, {'distance': distance, 'pk': pk})


def load_data_from_json(con: sqlite3.Connection, route: Path,
                        station: Path, timetable: Path):
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


def setup_sqlite(db_location: str) -> sqlite3.Connection:
    sqlite3.register_adapter(timedelta, adapt_time)
    sqlite3.register_converter('t_time', convert_time)
    con = sqlite3.connect(db_location, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row
    return con


def get_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Construt database from downloaded JSON to specified location',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-d',
        type=str, dest='db', default='db.sqlite',
        help='Output database file name')

    parser.add_argument(
        '-I',
        default=Path('JSON'), type=Path, dest='input_folder',
        help='Input folder')
    parser.add_argument(
        '-t',
        default='timetable', type=str, dest='timetable_name',
        help='File name for timetable. No file extension needed, because it has to be JSON')
    parser.add_argument(
        '-s',
        default='station', type=str, dest='station_name',
        help='File name for station information. No file extension needed, because it has to be JSON')
    parser.add_argument(
        '-r',
        default='route', type=str, dest='route_name',
        help='File name for route information. No file extension needed, because it has to be JSON')
    return parser


if __name__ == '__main__':
    parser = get_arg_parser()
    args = parser.parse_args()

    con = setup_sqlite(args.db)
    with con:
        create_schema(con)
        load_data_from_json(
            con=con,
            route=args.input_folder / f'{args.route_name}.json',
            station=args.input_folder / f'{args.station_name}.json',
            timetable=args.input_folder / f'{args.timetable_name}.json',
        )
