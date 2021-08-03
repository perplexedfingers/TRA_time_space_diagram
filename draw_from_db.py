from __future__ import annotations

import pathlib
import sqlite3
from datetime import timedelta
from math import ceil
from operator import itemgetter

from convert_to_sqlite import create_schema, load_data_from_json, setup_sqlite

SECOND_GAP = 0.4
TEN_MINUTE_GAP = round(60 * 10 * SECOND_GAP)
HOUR_GAP = round(3600 * SECOND_GAP)
PADDING = 50
ENLARGE_GAP_RATE = 10
FONT_HEIGHT = 12


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


def draw_hour_lines(height: int, start_hour: int, hour_count: int) -> list[str]:
    bottom_y = PADDING + height
    text_gap = max(min(TEN_MINUTE_GAP * 3, height), FONT_HEIGHT + PADDING)

    result = []
    every_ten_min_x = [
        PADDING + m * TEN_MINUTE_GAP + i * HOUR_GAP
        for i in range(hour_count) for m in range(6)
    ] + [PADDING + hour_count * HOUR_GAP]  # the very last hour
    for i, x in enumerate(every_ten_min_x, start=start_hour * 6):
        type_ = min_type(i)
        result.append(f'<line x1="{x}" x2="{x}" y1="{PADDING}" y2="{bottom_y}" class="{type_}" />')
        for j in range(0, min(ceil(height + text_gap), height + PADDING), text_gap):
            if type_ == 'hour':
                text = '{:0>2d}00'.format(i // 6)
            else:
                text = f'{i % 6}0'
            result.append(f'<text x="{x}" y="{PADDING - 1 + j}" class="{type_}_text">{text}</text>')
    return result


active_type = {True: 'station', False: 'noserv_station'}


def draw_station_lines(cur: sqlite3.Cursor, width: int) -> list[str]:
    cur.arraysize = 10  # random number
    result = []
    stations = cur.fetchmany()
    while stations:
        station_y = tuple((s['is_active'], round(s['y'] * ENLARGE_GAP_RATE + PADDING), s['name']) for s in stations)
        for is_active, y, name in station_y:
            type_ = active_type[is_active]
            result.append(f'<line x1="{PADDING}" x2="{width - PADDING}" y1="{y}" y2="{y}" class="{type_}" />')
            for i in range(0, width, HOUR_GAP):
                result.append(f'<text x="{i}" y="{y - 5}" class="{type_}_text">{name}</text>')
        stations = cur.fetchmany()
    return result


def draw_backgrond(con: sqlite3.Connection, route_name: str,
                   height: int, width: int, start_hour: int, hour_count: int) -> list[str]:
    result = []
    result.extend(draw_hour_lines(height, start_hour, hour_count))

    cur = con.execute(
        '''
        SELECT
            station.is_active
            ,station_name_cht.name
            ,route_station.relative_distance AS y
        FROM
            station
        JOIN station_name_cht ON
            station.pk = station_name_cht.station_fk
        JOIN route_station ON
            station.pk = route_station.station_fk
        JOIN route ON
            route.pk = route_station.route_fk
        WHERE
            route.name = ?
        ''',
        (route_name,)
    )
    result.extend(draw_station_lines(cur, width))
    return result


def get_time_list(con: sqlite3.Connection, code: str, route_name: str) -> tuple[(str, float)]:
    cur = con.execute(
        '''
        SELECT
            timetable.time AS x
            ,route_station.relative_distance AS y
        FROM
            timetable
        JOIN station ON
            timetable.station_fk = station.pk
        JOIN route_station ON
            station.pk = route_station.station_fk
        JOIN train ON
            timetable.train_fk = train.pk
        JOIN route ON
            route.pk = route_station.route_fk
        WHERE
            route.name = :name
        AND
            train.code = :code
        ORDER BY
            timetable.time ASC
        ''',
        {'code': code, 'name': route_name}
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


def draw_train_lines(con: sqlite3.Connection, start_hour: int, train_list: tuple[str], route_name: str) -> list[str]:
    result = []
    x_offset = timedelta(hours=start_hour)
    print_(f'{len(train_list)} trains to process in "{route_name}"')
    for count, (code, train_type) in enumerate(train_list, start=1):
        time_list = get_time_list(con, code, route_name)
        d = ' '.join(
            (f'{round((x - x_offset).total_seconds() * SECOND_GAP) + PADDING},{y * ENLARGE_GAP_RATE + PADDING}'
             for x, y in time_list)
        )
        result.append(f'<path id="{code}" d="M {d}" class="{type_to_css[train_type]}"></path>')
        time_span = round((max(time_list)[0] - min(time_list)[0]).total_seconds())
        result.extend([
            f'''<text>
            <textPath startOffset="{PADDING + i}" xlink:href="#{code}" class="{type_to_css[train_type]}_text">
            <tspan dy="-3">
            {code}
            </tspan>
            </textPath>
            </text>'''
            for i in range(0, time_span, min(time_span, 2 * TEN_MINUTE_GAP))
        ])
        print_(f'{count} / {len(train_list)} trains to process in "{route_name}"')
    print_(f'Finish "{route_name}"')
    return result


def draw(con: sqlite3.Connection, route_name: str,
         height: int, width: int,
         start_hour: int, hour_count: int,
         train_list: tuple[str]
         ) -> str:
    result = [
        f'''<svg
        xmlns="http://www.w3.org/2000/svg"
        xmlns:xlink="http://www.w3.org/1999/xlink"
        width="{width + 2 * PADDING}"
        height="{height + 2 * PADDING}">
        ''',
        '''
        <style>
            line{stroke-width: 0.3}

            .hour{stroke: black}
            .hour_text{fill: black}

            .min30{stroke: darkblue}
            .min30_text{fill: grey}

            .min10{stroke: slateblue}
            .min10_text{fill: lightgrey}

            .station{stroke: black}
            .station_text{fill: black}

            .noserv_station{stroke: grey}
            .noserv_station_text{fill: grey}

            path{stroke-width: 2;fill: none}

            .taroko{stroke: hsl(300, 0%, 65%)}
            .taroko_text{fill: hsl(300, 0%, 65%)}

            .puyuma{stroke: red}
            .puyuma_text{fill: red}

            .tze_chiang{stroke: hsl(14, 44%, 79%)}
            .tze_chiang_text{fill: hsl(14, 44%, 79%)}

            .tze_chiang_diesel{stroke: hsl(30, 100%, 70%)}
            .tze_chiang_diesel_text{fill: hsl(30, 100%, 70%)}

            .emu1200{stroke: hsl(327, 100%, 80%); stroke-dasharray: 25,5}
            .emu1200_text{fill: hsl(327, 100%, 80%)}

            .emu300{stroke: hsl(0, 73%, 100%); stroke-dasharray: 25,5}
            .emu300_text{fill: hsl(0, 73%, 100%)}

            .chu_kuang{stroke: hsl(48, 100%, 80%)}
            .chu_kuang_text{fill: hsl(48, 100%, 80%)}

            .local{stroke: hsl(240, 100%, 80%)}
            .local_text{fill: hsl(240, 100%, 80%)}

            .fu_hsing{stroke: hsl(220, 49%, 86%)}
            .fu_hsing_text{fill: hsl(220, 49%, 86%)}

            .ordinary{stroke: black}
            .ordinary_text{fill: black}

            .special{stroke: hsl(120, 70%, 100%)}
            .special_text{fill: hsl(120, 70%, 100%)}
        </style>
        '''
    ]

    result.extend(draw_backgrond(con, route_name, height, width, start_hour, hour_count))

    result.extend(draw_train_lines(con, start_hour=start_hour, train_list=train_list, route_name=route_name))

    result.append('</svg>')
    return '\n'.join(result)


def seconds_to_hours(t: int) -> int:
    return round(t // 60 // 60)


def decide_layout(con: sqlite3.Connection, route_name: str) -> (int, int, int, int, tuple[str, str]):
    cur = con.execute(
        '''
        SELECT
            max(route_station.relative_distance) as height
        FROM
            route_station
        JOIN route ON
            route.pk = route_station.route_fk
        WHERE
            route.name = ?
        ''',
        (route_name,)
    )
    height = round(cur.fetchone()['height'] * ENLARGE_GAP_RATE)

    cur = con.execute(
        '''
        WITH RECURSIVE
            segment (train_code, train_type, time, previous, timetable_pk, next) AS (
                SELECT train.code, train_type.code, timetable.time, timetable.previous, timetable.pk, timetable.next
                FROM timetable
                JOIN train ON timetable.train_fk = train.pk
                JOIN train_type ON train.train_type_fk = train_type.pk
                JOIN station ON timetable.station_fk = station.pk
                JOIN route_station ON station.pk = route_station.station_fk
                JOIN route ON route_station.route_fk = route.pk
                WHERE route.name = :name
            UNION
                SELECT train.code, train_type.code, timetable.time, timetable.previous, timetable.pk, timetable.next
                FROM timetable
                JOIN segment ON timetable.pk = segment.timetable_pk
                JOIN train ON timetable.train_fk = train.pk
                JOIN train_type ON train.train_type_fk = train_type.pk
                JOIN station ON timetable.station_fk = station.pk
                JOIN route_station ON station.pk = route_station.station_fk
                JOIN route ON route_station.route_fk = route.pk
                WHERE
                    ((timetable.previous IN (segment.timetable_pk, NULL))
                        OR
                    (timetable.next IN (segment.timetable_pk, NULL)))
                AND
                    route.name = :name
            ORDER BY
                timetable.time ASC
            )  -- recursively build the traveling dots
        SELECT
            MIN(segment.time) AS early
            ,MAX(segment.time) AS late
            ,segment.train_code AS code
            ,segment.train_type AS train_type
        FROM
            segment
        GROUP BY
            EXISTS (SELECT NULL FROM segment AS t WHERE t.timetable_pk = segment.next) -- group segment together
            ,segment.train_code  -- seperate the segaments by train code
        HAVING
            COUNT(segment.timetable_pk) > 2 -- at least travel once in the route
        ORDER BY
            segment.train_code
        ''',
        {'name': route_name}
    )

    infos = tuple((r['early'], r['late'], r['code'], r['train_type']) for r in cur.fetchall())
    train_list = tuple((r[2], r[3]) for r in infos)
    start_hour = seconds_to_hours(min(infos, key=itemgetter(0))[0])
    end_hour = seconds_to_hours(max(infos, key=itemgetter(1))[1]) + 1
    hour_count = end_hour - start_hour + 1
    width = (hour_count - 1) * HOUR_GAP + 2 * PADDING
    return height, width, start_hour, hour_count, train_list


def get_route_names(con: sqlite3.Connection) -> tuple[str]:
    cur = con.execute(
        '''
        SELECT
            DISTINCT route.name
        FROM
            route
        JOIN route_station ON
            route_station.route_fk = route.pk
        JOIN station ON
            route_station.station_fk = station.pk
        JOIN timetable ON
            timetable.station_fk = station.pk
        WHERE
            route_station.relative_distance != 0  -- with DISTINCT, exclude routes that has no stations
        '''
    )
    return tuple(r['name'] for r in cur.fetchall())


if __name__ == '__main__':
    print_('Start to load data')
    con = setup_sqlite()
    with con:
        create_schema(con)
        load_data_from_json(
            con,
            pathlib.Path('./JSON/route.json'),
            pathlib.Path('./JSON/station.json'),
            pathlib.Path('./JSON/timetable.json'),
        )
        print_('Finish loading data')
        route_names = get_route_names(con)
        print_(f'There are {len(route_names)} routes to process')
        for i, route in enumerate(route_names, start=1):
            height, width, start_hour, hour_count, train_list = decide_layout(con, route_name=route)

            result = draw(
                con=con, route_name=route,
                height=height, width=width,
                start_hour=start_hour, hour_count=hour_count,
                train_list=train_list,
            )
            with open(f'OUTPUT/{route}.svg', mode='w') as f:
                f.write(result)
            print_(f'{i} / {len(route_names)} routes to go')
    print_('All done')
