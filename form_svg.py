from __future__ import annotations

import argparse
import sqlite3
from collections import namedtuple
from datetime import timedelta
from itertools import chain, filterfalse, groupby, tee
from operator import attrgetter, itemgetter
from pathlib import Path
from textwrap import dedent
from typing import Union

from pypika import Order, Parameter, Query, Tables
from pypika.functions import Count, Max, Min
from yattag import Doc

from construct_db_from_json import (create_schema, load_data_from_json,
                                    setup_sqlite)

SECOND_GAP = 0.4
TEN_MINUTE_GAP = round(60 * 10 * SECOND_GAP)
HOUR_GAP = round(3600 * SECOND_GAP)
PADDING = 50
ENLARGE_GAP_RATE = 10
FONT_HEIGHT = 12

TIMETABLE, STATION, STATION_NAME_CHT, ROUTE_STATION, TRAIN, ROUTE =\
    Tables('timetable', 'station', 'station_name_cht', 'route_station', 'train', 'route')


def print_(s: str):
    print('\033[K', end='\r')  # clear_previous_print
    print(s, end='\r')


def min_type(min_: int) -> str:
    if min_ % 6 == 0:  # hour
        type_ = 'hour'
    elif min_ % 3 == 0:  # thirty minutes
        type_ = 'min30'
    else:
        type_ = 'min10'
    return type_


HourLineGroup = namedtuple('HourLineGroup', ['line', 'text'])
LineInfo = namedtuple('LineInfo', ['x1', 'y1', 'x2', 'y2', 'klass'])
TextInfo = namedtuple('TextInfo', ['x', 'y', 'klass', 'text'])


def form_hour_lines(height: int, start_hour: int, hour_count: int) -> list[HourLineGroup]:
    bottom_y = PADDING + height

    result = []
    every_ten_min_x = [
        PADDING + m * TEN_MINUTE_GAP + i * HOUR_GAP
        for i in range(hour_count) for m in range(6)
    ] + [PADDING + hour_count * HOUR_GAP]  # the very last hour
    for i, x in enumerate(every_ten_min_x, start=start_hour * 6):
        type_ = min_type(i)
        result.append(
            HourLineGroup(
                line=LineInfo(x1=x, x2=x, y1=PADDING, y2=bottom_y, klass=type_),
                text=TextInfo(x=x, y=PADDING - 1, klass=type_, text=f'{i // 6:0>2d}{i % 6}0')
            )
        )
    return result


active_type = {1: 'station', 0: 'noserv_station'}

StationLineGroup = namedtuple('HourLineGroup', ['line', 'text'])


def form_station_lines(cur: sqlite3.Cursor, width: int) -> list[StationLineGroup]:
    cur.arraysize = 10  # random number
    result = []
    stations = cur.fetchmany()
    y_offset = 5  # avoid conflict with hour number
    while stations:
        for data in stations:
            type_ = active_type[data['is_active']]
            y = round(data['y'] * ENLARGE_GAP_RATE + PADDING)
            result.append(
                StationLineGroup(
                    line=LineInfo(x1=PADDING, x2=width - PADDING, y1=y, y2=y, klass=type_),
                    text=TextInfo(x=0, y=y - y_offset, klass=type_, text=data["name"])
                )
            )
        stations = cur.fetchmany()
    return result


def get_time_list(con: sqlite3.Connection,
                  code: str, route_name: str,
                  from_: int, to: int) -> tuple[(str, float)]:
    cur = con.execute(
        Query.from_(TIMETABLE)
        .join(STATION).on(TIMETABLE.station_fk == STATION.pk)
        .join(ROUTE_STATION).on(STATION.pk == ROUTE_STATION.station_fk)
        .join(TRAIN).on(TIMETABLE.train_fk == TRAIN.pk)
        .join(ROUTE).on(ROUTE_STATION.route_fk == ROUTE.pk)
        .where(
            (ROUTE.name == Parameter(':name'))
            & (TRAIN.code == Parameter(':code'))
            & (TIMETABLE.order_[Parameter(':from'):Parameter(':to')])
        )
        .orderby(TIMETABLE.time, order=Order.asc)
        .select(
            TIMETABLE.time.as_('x'),
            ROUTE_STATION.relative_distance.as_('y')
        ).get_sql(),
        {'code': code, 'name': route_name,
         'from': from_, 'to': to}
    )
    return tuple((r['x'], r['y']) for r in cur.fetchall())


type_to_css = {
    '1131': 'local',
    '1132': 'local',

    '1101': 'taroko',
    '1102': 'taroko',
    '1100': 'tze_chiang_diesel',
    '1103': 'tze_chiang_diesel',
    '110D': 'tze_chiang_diesel',
    '110E': 'tze_chiang_diesel',
    '110F': 'tze_chiang_diesel',
    '1105': 'tze_chiang',
    '1106': 'tze_chiang',
    '1108': 'tze_chiang',
    '1109': 'tze_chiang',
    '110A': 'tze_chiang',
    '1107': 'puyuma',
    '110B': 'emu1200',
    '110C': 'emu300',

    '1110': 'chu_kuang',
    '1111': 'chu_kuang',
    '1112': 'chu_kuang',
    '1114': 'chu_kuang',
    '1115': 'chu_kuang',

    '1120': 'fu_hsing',

    '1140': 'ordinary',

    '0000': 'special'
}


PathInfo = namedtuple('PathInfo', ['id', 'd', 'klass'])
TextPathInfo = namedtuple('TextPathInfo', ['offset', 'id', 'klass', 'text'])


def form_train_lines(con: sqlite3.Connection, start_hour: int,
                     segments: dict[tuple[str, str], tuple[tuple[int, int]]],
                     route_name: str) -> (PathInfo, TextPathInfo):
    pathes, text_pathes = [], []
    x_offset = timedelta(hours=start_hour)

    count = 1
    amount = sum(len(tuple(i for i in s)) for s in segments.values())
    print_(f'{amount} segments to process in "{route_name}"')

    for (code, train_type), _segments in segments.items():
        for from_, to in _segments:
            time_list = get_time_list(con, code, route_name, from_, to)
            d = ' '.join(
                (dedent(f'''
                 {round((x - x_offset).total_seconds() * SECOND_GAP + PADDING)},
                 {y * ENLARGE_GAP_RATE + PADDING}
                 ''').strip()
                 for x, y in time_list)
            )
            pathes.append(PathInfo(id=code, d=d, klass=type_to_css[train_type]))
            time_span = round(
                (max(map(itemgetter(0), time_list)).total_seconds()
                 - min(map(itemgetter(0), time_list)).total_seconds())
                * SECOND_GAP
            )
            text_pathes.extend(
                [TextPathInfo(offset=i, id=code, klass=type_to_css[train_type], text=code)
                 for i in range(PADDING, time_span - PADDING + 1, min(time_span - 2 * PADDING, 2 * TEN_MINUTE_GAP))]
            )

            print_(f'{count} / {amount} segments has been processed in "{route_name}"')
            count += 1
    return pathes, text_pathes


def partition(pred, iterable):
    "Use a predicate to partition entries into false entries and true entries"
    # partition(is_odd, range(10)) --> 0 2 4 6 8   and  1 3 5 7 9
    t1, t2 = tee(iterable)
    return filterfalse(pred, t1), filter(pred, t2)


def form_svg(con: sqlite3.Connection, route_name: str,
             height: int, width: int,
             start_hour: int, hour_count: int,
             segments: dict[tuple[str, str], tuple[tuple[int, int]]]
             ) -> str:
    cur = con.execute(
        Query.from_(STATION)
        .join(STATION_NAME_CHT).on(STATION.pk == STATION_NAME_CHT.station_fk)
        .join(ROUTE_STATION).on(STATION.pk == ROUTE_STATION.station_fk)
        .join(ROUTE).on(ROUTE_STATION.route_fk == ROUTE.pk)
        .where(ROUTE.name == Parameter('?'))
        .select(
            STATION.is_active,
            STATION_NAME_CHT.name,
            ROUTE_STATION.relative_distance.as_('y')
        ).get_sql(),
        (route_name,)
    )
    station_groups = form_station_lines(cur=cur, width=width)
    hour_groups = form_hour_lines(height=height, start_hour=start_hour, hour_count=hour_count)
    pathes, text_pathes = form_train_lines(
        con=con, start_hour=start_hour,
        segments=segments, route_name=route_name
    )

    doc, tag, text, line = Doc().ttl()
    doc.asis('<!DOCTYPE html>')
    with tag('html'):
        with tag('head'):
            doc.stag('meta', charset='utf-8')
            doc.stag('link', rel='icon', href='data:,')
            line('title', route_name)
            doc.stag('link', rel='stylesheet', href='./style.css')  # TODO dynamic location
        with tag('body'):
            with tag(
                'svg',
                ('xmlns', 'http://www.w3.org/2000/svg'),
                ('width', f'{width + 2 * PADDING}'),
                ('height', f'{height + 2 * PADDING}')
            ):
                doc.stag('script', href='./fixed_header.js')
                for group in hour_groups:
                    with tag('g', klass='hour'):
                        doc.stag(
                            'line', klass=group.line.klass,
                            x1=group.line.x1, x2=group.line.x2,
                            y1=group.line.y1, y2=group.line.y2,
                        )
                        line('text', group.text.text, x=group.text.x, y=group.text.y, klass=group.text.klass)
                for group in station_groups:
                    with tag('g', klass='station'):
                        doc.stag(
                            'line', klass=group.line.klass,
                            x1=group.line.x1, x2=group.line.x2,
                            y1=group.line.y1, y2=group.line.y2,
                        )
                        line('text', group.text.text, x=group.text.x, y=group.text.y, klass=group.text.klass)
                for id_, items in groupby(
                    sorted(chain(pathes, text_pathes), key=attrgetter('id')),
                        key=attrgetter('id')):
                    with tag('g', klass='train'):
                        line('title', id_)
                        text_pathes_, pathes_ = partition(lambda x: hasattr(x, 'd'), items)
                        for path in pathes_:
                            doc.stag('path', id=str(path.id), d=f'M {path.d}', klass=path.klass)
                        for text_path in text_pathes_:
                            with tag('text'):
                                line('textPath', text_path.text, startOffset=text_path.offset,
                                     href=f'#{text_path.id}', klass=text_path.klass)
    print_(f'Finish "{route_name}"')
    return doc.getvalue()


def seconds_to_hours(t: int) -> int:
    return round(t // 60 // 60)


Info = namedtuple('Info', ['early', 'late', 'code', 'train_type', 'from_', 'to'])


def get_code_n_train_type(t: Info) -> tuple[str, str]:
    return (t.code, t.train_type)


def gen_recursive_cte_statement(given_train_codes: Union[None, list[str]]):
    # No recursive CTE from pypika yet
    # Sqlite3 'RETURNING' is not yet supported by either SQLAlchemy or peewee
    # So we have this workaround
    statement = [
        '''
        WITH RECURSIVE
            segment (code, train_type, x, y, previous, current, order_, group_) AS (
                SELECT
                    train.code ,train_type.code ,_t.time ,route_station.relative_distance
                    ,_t.previous ,_t.pk ,_t.order_ ,_t.pk
                FROM timetable AS _t
                JOIN train ON _t.train_fk = train.pk
                JOIN train_type ON train.train_type_fk = train_type.pk
                JOIN station ON _t.station_fk = station.pk
                JOIN route_station ON station.pk = route_station.station_fk
                JOIN route ON route_station.route_fk = route.pk
                WHERE
                    (
                        _t.previous ISNULL  -- the very first stop
                        OR
                        NOT EXISTS (  -- the previous stop is not on the same route
                            SELECT NULL FROM timetable AS t
                            JOIN station AS st ON t.station_fk = st.pk
                            JOIN route_station AS rs ON st.pk = rs.station_fk
                            JOIN route AS ro ON rs.route_fk = ro.pk
                            WHERE ro.name = :route AND t.pk = _t.previous)
                    )
                    AND route.name = :route
        ''',
        '',
        '''
                UNION
                    SELECT
                        train.code, train_type.code, timetable.time, route_station.relative_distance,
                        timetable.previous, timetable.pk, timetable.order_, segment.group_
                    FROM segment
                    JOIN timetable ON timetable.previous = segment.current
                    JOIN train ON timetable.train_fk = train.pk
                    JOIN train_type ON train.train_type_fk = train_type.pk
                    JOIN station ON timetable.station_fk = station.pk
                    JOIN route_station ON station.pk = route_station.station_fk
                    JOIN route ON route_station.route_fk = route.pk
                    WHERE
                        route.name = :route
            )
        SELECT
            code, train_type,
            MIN(x) AS early, MAX(x) AS late,
            MIN(order_) AS from_, MAX(order_) AS to_
        FROM
            segment
        GROUP BY
            segment.code, segment.group_
        HAVING  -- Travel more than one stop on the route
            COUNT(segment.current) > 2
        '''
    ]

    if given_train_codes:
        _parameters = ', '.join(f':{i}' for i in range(len(given_train_codes)))
        statement[1] = f'AND train.code IN ({_parameters})'
    return ''.join(statement)


def decide_layout(con: sqlite3.Connection, route_name: str,
                  given_train_codes: Union[None, list[str]]) -> (int, int, int, int, tuple[str, str]):
    parameters = {'route': route_name}
    cur = con.execute(
        Query.from_(ROUTE_STATION)
        .join(ROUTE).on(ROUTE_STATION.route_fk == ROUTE.pk)
             .where(ROUTE.name == Parameter(':route'))
             .select(
            (Max(ROUTE_STATION.relative_distance) - Min(ROUTE_STATION.relative_distance))
                 .as_('height'),
        ).get_sql(),
        parameters)
    result = cur.fetchone()
    height = round(result['height'] * ENLARGE_GAP_RATE)

    if given_train_codes:
        for i, code in zip(range(len(given_train_codes)), given_train_codes):
            parameters[str(i)] = code
    cur = con.execute(gen_recursive_cte_statement(given_train_codes), parameters)
    infos = tuple(
        Info(early=r['early'], late=r['late'],
             code=r['code'], train_type=r['train_type'],
             from_=r['from_'], to=r['to_'])
        for r in cur.fetchall())
    segments = {
        key: tuple((row.from_, row.to) for row in r)
        for key, r in groupby(
            sorted(infos, key=get_code_n_train_type),
            key=get_code_n_train_type)
    }
    start_hour = seconds_to_hours(min(infos, key=attrgetter('early')).early)
    end_hour = seconds_to_hours(max(infos, key=attrgetter('late')).late) + 1
    hour_count = end_hour - start_hour + 1
    width = (hour_count - 1) * HOUR_GAP + 2 * PADDING
    return height, width, start_hour, hour_count, segments


def get_route_names(con: sqlite3.Connection, given_train_codes: Union[None, list[str]]) -> tuple[str]:
    query = (
        Query.from_(ROUTE)
        .join(ROUTE_STATION).on(ROUTE.pk == ROUTE_STATION.route_fk)
        .join(STATION).on(ROUTE_STATION.station_fk == STATION.pk)
        .join(TIMETABLE).on(STATION.pk == TIMETABLE.station_fk)
        .where(ROUTE_STATION.relative_distance != 0)  # exclude routes that have only one station
        .select(ROUTE.name).distinct()
    )
    if given_train_codes:
        _parameters = ', '.join('?' for _ in range(len(given_train_codes)))
        query = query.join(TRAIN).on(TIMETABLE.train_fk == TRAIN.pk)\
            .where(TRAIN.code.isin(Parameter(f'({_parameters})')))\
            .groupby(TRAIN.code, ROUTE.name)\
            .having(Count(STATION.pk) > 2)  # travel more than one stop on the route
        cur = con.execute(query.get_sql(), tuple(given_train_codes))
    else:
        cur = con.execute(query.get_sql())
    return tuple(r['name'] for r in cur.fetchall())


def get_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Form SVG from either downloaded JSON or prepared sqlite database',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-d',
        type=str, dest='db', default=':memory:',
        help='Input database file name')

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
    parser.add_argument(
        '-O',
        default='OUTPUT', type=str, dest='output_folder',
        help='Output folder')

    parser.add_argument(
        '-T',
        default=None, type=str, dest='train_list', nargs='*',
        help='Only draw these trains')
    return parser


if __name__ == '__main__':
    parser = get_arg_parser()
    args = parser.parse_args()

    print_('Start to load data')
    con = setup_sqlite(args.db)
    with con:
        if not Path(args.db).exists():
            create_schema(con)
            load_data_from_json(
                con=con,
                route=args.input_folder / f'{args.route_name}.json',
                station=args.input_folder / f'{args.station_name}.json',
                timetable=args.input_folder / f'{args.timetable_name}.json',
            )
        print_('Finish loading data')
        route_names = get_route_names(con, given_train_codes=args.train_list)
        print_(f'There are {len(route_names)} routes to process')
        for i, route in enumerate(route_names, start=1):
            height, width, start_hour, hour_count, segments =\
                decide_layout(con, route_name=route, given_train_codes=args.train_list)
            result = form_svg(
                con=con, route_name=route,
                height=height, width=width,
                start_hour=start_hour, hour_count=hour_count,
                segments=segments,
            )
            with open(f'{args.output_folder}/{route}.html', mode='w') as f:
                f.write(result)
            print_(f'{len(route_names) - i} / {len(route_names)} routes to go')
    print_('All done')
