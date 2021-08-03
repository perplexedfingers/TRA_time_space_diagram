from __future__ import annotations

import pathlib
import sqlite3
from datetime import timedelta
from itertools import groupby, tee
from math import ceil
from operator import itemgetter

from convert_to_sqlite import create_schema, load_data_from_json, setup_sqlite

# TODO use logging rather than print
# TODO align SQL statements
# TODO download data set utility

SECOND_GAP = 0.4
TEN_MINUTE_GAP = round(60 * 10 * SECOND_GAP)
HOUR_GAP = round(3600 * SECOND_GAP)
PADDING = 50
ENLARGE_GAP_RATE = 10


def draw_hour_lines(height: int, width: int, start_hour: int, hour_count: int) -> list[str]:
    bottom_y = PADDING + height
    font_height = 12
    text_gap = max(min(TEN_MINUTE_GAP * 3, height), font_height + PADDING)

    result = []
    every_ten_min_x = [
        PADDING + m * TEN_MINUTE_GAP + i * HOUR_GAP
        for i in range(hour_count) for m in range(6)
    ] + [PADDING + hour_count * HOUR_GAP]  # the very last hour
    for i, x in enumerate(every_ten_min_x, start=start_hour * 6):
        if i % 6 == 0:  # hour
            result.append(f'<line x1="{x}" x2="{x}" y1="{PADDING}" y2="{bottom_y}" class="hour" />')
        elif i % 3 == 0:  # thirty minutes
            result.append(f'<line x1="{x}" x2="{x}" y1="{PADDING}" y2="{bottom_y}" class="min30" />')
        else:
            result.append(f'<line x1="{x}" x2="{x}" y1="{PADDING}" y2="{bottom_y}" class="min10" />')
        for j in range(0, min(ceil(height + text_gap), height + PADDING), text_gap):
            if i % 6 == 0:  # hour
                result.append(f'<text x="{x}" y="{PADDING - 1 + j}" class="hour_text">{"{:0>2d}00".format(i // 6)}</text>')
            elif i % 3 == 0:  # thirty minutes
                result.append(f'<text x="{x}" y="{PADDING - 1 + j}" class="min30_text">{i % 6}0</text>')
            else:
                result.append(f'<text x="{x}" y="{PADDING - 1 + j}" class="min10_text">{i % 6}0</text>')
    return result


def draw_station_lines(cur: sqlite3.Cursor, width: int) -> list[str]:
    result = []
    cur.arraysize = 10  # random number
    stations = cur.fetchmany()
    while stations:
        station_y = [(s['is_active'], round(s['y'] * ENLARGE_GAP_RATE + PADDING), s['name']) for s in stations]
        for is_active, y, name in station_y:
            if is_active:
                result.append(f'<line x1="{PADDING}" x2="{width - PADDING}" y1="{y}" y2="{y}" class="station" />')
            else:
                result.append(f'<line x1="{PADDING}" x2="{width - PADDING}" y1="{y}" y2="{y}" class="noserv_station" />')
            for i in range(0, width, HOUR_GAP):
                if is_active:
                    result.append(f'<text x="{i}" y="{y - 5}" class="staion_text">{name}</text>')
                else:
                    result.append(f'<text x="{i}" y="{y - 5}" class="noserv_staion_text">{name}</text>')
        stations = cur.fetchmany()
    return result


def draw_backgrond(con: sqlite3.Connection, route_name: str, height: int, width: int, start_hour: int, hour_count: int):
    result = []
    result.extend(draw_hour_lines(height, width, start_hour, hour_count))

    cur = con.execute(
        '''
        SELECT station.is_active, station_name_cht.name, route_station.relative_distance AS y
        FROM station
        JOIN station_name_cht ON
            station.pk = station_name_cht.station_fk
        JOIN route_station ON
            station.pk = route_station.station_fk
        JOIN route ON
            route.pk = route_station.route_fk
        WHERE route.name = ?
        ''',
        (route_name,)
    )
    result.extend(draw_station_lines(cur, width))
    return result


def get_time_list(con: sqlite3.Connection, code: str, route_name: str) -> list[(int, float)]:
    cur = con.execute(
        '''
        SELECT
            timetable.time AS x
            ,route_station.relative_distance AS y
        FROM timetable
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
    return [(r['x'], r['y']) for r in cur.fetchall()]


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
    print(f'{len(train_list)} trains to process in "{route_name}"')
    for i, (code, train_type) in enumerate(train_list):
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
        print(f'{i} / {len(train_list)}', end='\r')
    print(f'Finish "{route_name}"')
    return result


def draw(con: sqlite3.Connection, route_name: str,
         height: int, width: int,
         start_hour: int, hour_count: int,
         train_list: tuple[str]
         ) -> list[str]:
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

            .tze_chiang_diesel{stroke: hsl(30, 100%, 100%)}
            .tze_chiang_diesel_text{fill: hsl(30, 100%, 100%)}

            .emu1200{stroke: hsl(327, 100%, 100%); stroke-dasharray: 25,5}
            .emu1200_text{fill: hsl(327, 100%, 100%)}

            .emu300{stroke: hsl(0, 73%, 100%); stroke-dasharray: 25,5}
            .emu300_text{fill: hsl(0, 73%, 100%)}

            .chu_kuang{stroke: hsl(48, 100%, 100%)}
            .chu_kuang_text{fill: hsl(48, 100%, 100%)}

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


def from_timedelta_to_hour(t: timedelta) -> int:
    return round(t.total_seconds() // 60 // 60)


def decide_layout(con: sqlite3.Connection, route_name: str) -> (int, int, int, int, list[str]):
    cur = con.execute(
        '''
        SELECT max(route_station.relative_distance) as height
        FROM route_station
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
        SELECT
            timetable.time
            ,train.code
            ,train_type.code AS train_type
            ,timetable.time - LAG(timetable.time)
                OVER (PARTITION BY train.code ORDER BY train.code, timetable.time ASC) AS diff
        FROM
            timetable
        JOIN station ON
            timetable.station_fk = station.pk
        JOIN route_station ON
            route_station.station_fk = station.pk
        JOIN route ON
            route.pk = route_station.route_fk
        JOIN train ON
            timetable.train_fk = train.pk
        JOIN train_type ON
            train.train_type_fk = train_type.pk
        WHERE
            route.name = ?
        ORDER BY
            train.code
            ,timetable.time ASC
        ''',
        (route_name,)
    )
    # TODO better solving these by SQL statement
    # Exclude tranfering line records can be solved by following statement
    #
    # SELECT
    # ...
    #   ,min(timetable.time) AS early
    #   ,max(timetable.time) AS late
    # ...
    # GROUP BY
    #     blah.code
    # HAVING
    #     COUNT(blah.timetable_pk) > 2
    # ...
    #
    # Travel time could be solved by WITH and LAG(). Like the following
    #
    # WITH blah AS (
    #     SELECT
    #         train.code
    #         ,min(timetable.time) as early
    #         ,max(timetable.time) as late
    #         ,timetable.pk AS timetable_pk
    #         ,timetable.time AS t
    #         ,route.name AS name
    #         ,station.pk AS x
    #         ,timetable.time - LAG(timetable.time) OVER (PARTITION BY train.code ORDER BY timetable.time ASC) AS diff
    #     FROM
    #         train
    #     JOIN timetable ON
    #         timetable.train_fk = train.pk
    #     JOIN station ON
    #         timetable.station_fk = station.pk
    #     JOIN route_station ON
    #         station.pk = route_station.station_fk
    #     JOIN route ON
    #         route.pk = route_station.route_fk
    # )
    # SELECT
    #     blah.code
    #     ,blah.t
    #     ,blah.x
    #     ,blah.diff
    #     ,blah.early
    #     ,blah.late
    #     ,max(blah.diff) AS max_diff
    # FROM blah
    # WHERE
    #     blah.name = ?
    # GROUP BY
    #     blah.code
    # HAVING
    #     COUNT(blah.timetable_pk) > 2
    # ORDER BY
    #     blah.code
    #     ,blah.t ASC
    #
    # However, I don't know how to combine LAG() with aggreation function

    infos = tuple((r['code'], r['time'], r['diff'], r['train_type']) for r in cur.fetchall())
    min_times = []
    max_times = []
    train_list = []
    max_travel_time = timedelta(hours=8).total_seconds()
    for code, info in groupby(infos, key=itemgetter(0)):
        for_diff, for_time, for_count = tee(info, 3)
        for_train_type = next(info)
        # HAVING COUNT(blah.timetable_pk) > 2
        if len([*for_count]) > 2\
                and max([r[2] for r in for_diff if r[2] is not None]) < max_travel_time:  # WITH, LAG() and WHERE
            train_list.append((code, for_train_type[3]))
            for i in for_time:
                min_times.append(i[1])  # MIN(timetable.time), GROUP BY and ORDER BY
                max_times.append(i[1])  # MAX(timetable.time), GROUP BY and ORDER BY
    start_hour = from_timedelta_to_hour(min(min_times))
    end_hour = from_timedelta_to_hour(max(max_times)) + 1

    hour_count = end_hour - start_hour + 1
    width = (hour_count - 1) * HOUR_GAP + 2 * PADDING
    return height, width, start_hour, hour_count, train_list


def get_route_names(con: sqlite3.Connection):
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
            route_station.relative_distance != 0
        AND
            EXISTS (
                SELECT 1
                FROM
                    timetable
                WHERE
                    timetable.station_fk IS NOT NULL
            )
        '''
    )
    return [r['name'] for r in cur.fetchall()]


if __name__ == '__main__':
    print('Start')
    con = setup_sqlite()
    with con:
        create_schema(con)
        load_data_from_json(
            con,
            pathlib.Path('./JSON/route.json'),
            pathlib.Path('./JSON/station.json'),
            pathlib.Path('./JSON/timetable.json'),
        )
        print('Finish loading data')
        route_names = get_route_names(con)
        print(f'There are {len(route_names)} routes to process')
        for i, route in enumerate(route_names, start=1):
            height, width, start_hour, hour_count, train_list = decide_layout(con, route_name=route)

            with open(f'{route}.svg', mode='w') as f:
                f.write(draw(
                    con=con, route_name=route,
                    height=height, width=width,
                    start_hour=start_hour, hour_count=hour_count,
                    train_list=train_list,
                ))
            print(f'{i} / {len(route_names)}')
    print('All done')
