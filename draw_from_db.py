from __future__ import annotations

import pathlib
import sqlite3
from datetime import timedelta
from itertools import groupby, tee, zip_longest
from math import ceil
from operator import itemgetter

from convert_to_sqlite import create_schema, load_data_from_json, setup_sqlite

# TODO use logging rather than print
# TODO look and feel
# TODO align SQL statements
# TODO download data set utility

SECOND_GAP = 0.4
HOUR_GAP = round(3600 * SECOND_GAP)
PADDING = 50
ENLARGE_GAP_RATE = 10


def draw_hour_lines(height: int, width: int, start_hour: int, hour_count: int) -> list[str]:
    bottom_y = PADDING + height
    text_gap = min(height // 7, 700)  # gap between vertical text TODO should relate with 'viewport'
    ten_minute_gap = round(60 * 10 * SECOND_GAP)

    result = []
    every_ten_min_x = [
        PADDING + m * ten_minute_gap + i * HOUR_GAP
        for i in range(hour_count) for m in range(6)
    ] + [PADDING + hour_count * HOUR_GAP]  # the very last hour
    for i, x in enumerate(every_ten_min_x, start=start_hour * 6):
        if i % 6 == 0:  # hour
            result.append(f'<line x1="{x}" x2="{x}" y1="{PADDING}" y2="{bottom_y}" stroke="yellow" />')
        elif i % 3 == 0:  # thirty minutes
            result.append(f'<line x1="{x}" x2="{x}" y1="{PADDING}" y2="{bottom_y}" stroke="green" />')
        else:
            result.append(f'<line x1="{x}" x2="{x}" y1="{PADDING}" y2="{bottom_y}" stroke="red" />')
        for j in range(0, ceil(height + text_gap), text_gap):
            if i % 6 == 0:  # hour
                result.append(f'<text fill="black" x="{x}" y="{PADDING - 1 + j}">{"{:0>2d}00".format(i // 6)}</text>')
            elif i % 3 == 0:  # thirty minutes
                result.append(f'<text fill="black" x="{x}" y="{PADDING - 1 + j}">{i % 6}0</text>')
            else:
                result.append(f'<text fill="grey" x="{x}" y="{PADDING - 1 + j}">{i % 6}0</text>')
    return result


def draw_station_lines(cur: sqlite3.Cursor, width: int) -> list[str]:
    result = []
    cur.arraysize = 10  # random number
    stations = cur.fetchmany()
    while stations:
        station_y = [(s['is_active'], round(s['y'] * ENLARGE_GAP_RATE + PADDING), s['name']) for s in stations]
        for is_active, y, name in station_y:
            if is_active:
                result.append(f'<line x1="{PADDING}" x2="{width - PADDING}" y1="{y}" y2="{y}" stroke="black" />')
            else:
                result.append(f'<line x1="{PADDING}" x2="{width - PADDING}" y1="{y}" y2="{y}" stroke="grey" />')
            for i in range(0, width, HOUR_GAP):
                if is_active:
                    result.append(f'<text fill="black" x="{i}" y="{y - 5}">{name}</text>')
                else:
                    result.append(f'<text fill="grey" x="{i}" y="{y - 5}">{name}</text>')
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


def draw_train_lines(con: sqlite3.Connection, start_hour: int, train_list: tuple[str], route_name: str) -> list[str]:
    result = []
    x_offset = timedelta(hours=start_hour)
    print(f'{len(train_list)} trains to process in "{route_name}"')
    for i, code in enumerate(train_list):
        time_list = get_time_list(con, code, route_name)
        d = ' '.join(
            (f'{round((x - x_offset).total_seconds() * SECOND_GAP) + PADDING},{y * ENLARGE_GAP_RATE + PADDING}'
             for x, y in time_list)
        )
        result.append(f'<path id="{code}" d="M {d}" stroke="black" stroke-width="2" fill="none"></path>')
        result.extend([
            f'''<text>
            <textPath stroke="black" startOffset="{PADDING + 600 * i}" xlink:href="#{code}">
            <tspan dy="-3">
            {code}
            </tspan>
            </textPath>
            </text>'''
            for i in range(0, 6)  # 600?
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
        style="font-family:Tahoma"
        width="{width}"
        height="{height + 2 * PADDING}">
        ''',
    ]

    result.extend(draw_backgrond(con, route_name, height, width, start_hour, hour_count))

    result.extend(draw_train_lines(con, start_hour=start_hour, train_list=train_list, route_name=route_name))

    result.append('</svg>')
    return '\n'.join(result)


def from_timedelta_to_hour(t: timedelta) -> int:
    return round(t.total_seconds() // 60 // 60)


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


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

    infos = tuple((r['code'], r['time'], r['diff']) for r in cur.fetchall())
    min_times = []
    max_times = []
    train_list = []
    max_travel_time = timedelta(hours=8).total_seconds()
    for code, info in groupby(infos, key=itemgetter(0)):
        for_diff, for_time, for_count = tee(info, 3)
        # HAVING COUNT(blah.timetable_pk) > 2
        if len([*for_count]) <= 2\
                or max([r[2] for r in for_diff if r[2] is not None]) > max_travel_time:  # LAG() with WHERE condition
            continue
        else:
            train_list.append(code)
            for a, b in grouper(for_time, n=2):
                min_times.append(a[1])  # min(timetable.time) as early with GROUP BY and ORDER BY
                max_times.append(b[1])  # max(timetable.time) as late with GROUP BY and ORDER BY
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
