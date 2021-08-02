from __future__ import annotations

import pathlib
import sqlite3
from datetime import timedelta
from math import ceil

from convert_to_sqlite import create_schema, load_data_from_json, setup_sqlite

SECOND_GAP = 0.1
HOUR_GAP = round(3600 * SECOND_GAP)
PADDING = 50


def draw_hour_lines(height: int, width: int, hours: list[int]) -> list[str]:
    bottom_y = PADDING + height
    text_gap = height // 7  # gap between vertical text TODO should relate with 'viewport'
    ten_minute_gap = round(60 * 10 * SECOND_GAP)

    result = []
    every_ten_min_x = [
        PADDING + m * ten_minute_gap + i * HOUR_GAP
        for i in range(len(hours)) for m in range(6)
    ] + [PADDING + len(hours) * HOUR_GAP]  # the very last hour
    for i, x in enumerate(every_ten_min_x, start=hours[0] * 6):
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
    stations = cur.fetchmany()
    while stations:
        station_y = [(s['is_active'], s['relative_distance'] + PADDING, s['name']) for s in stations]
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


def draw_train_lines(cur: sqlite3.Cursor, start_hour: int) -> list[str]:
    result = []
    stations = cur.fetchmany()
    # code, relative_distance, time
    x_offset = start_hour * 3600
    path = []
    while stations:
        for station in stations:
            path.append(
                f"{round((station['time'].total_seconds() - x_offset) * SECOND_GAP)},{station['relative_distance'] + PADDING}"
            )
            path.extend([
                f'<text><textPath stroke="black" startOffset = "{PADDING + 600 * i}"><tspan dy="-3">{station["code"]}</tspan></textPath></text>'
                for i in range(0, 6)  # 600?
            ])
        stations = cur.fetchmany()
    result.extend(path)
    return result


def draw_backgrond(con: sqlite3.Connection, route_name: str, height: int, width: int, hours: list[int]):
    result = []
    result.extend(draw_hour_lines(height, width, hours))

    cur = con.execute(
        '''
        SELECT station.is_active, station_name_cht.name, route_station.relative_distance
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


def draw(con: sqlite3.Connection, route_name: str, height: int, width: int, hours: list[int]) -> list[str]:
    result = [
        '<?xml version="1.0" encoding="utf-8" ?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" style="font-family:Tahoma" width="{width}" height="{height + 2 * PADDING}">',
    ]

    result.extend(draw_backgrond(con, route_name, height, width, hours))

    # cur = con.execute(
    #     '''
    #     SELECT
    #         train.code, route_station.relative_distance,
    #         timetable.time
    #     FROM timetable
    #     JOIN station ON
    #         timetable.station_fk = station.pk
    #     JOIN route_station ON
    #         station.pk = route_station.station_fk
    #     JOIN train ON
    #         timetable.train_fk = train.pk
    #     JOIN route ON
    #         route.pk = route_station.route_fk
    #     WHERE route.name = ?
    #     ORDER BY
    #         train.code, timetable.time ASC
    #     ''',
    #     (route_name,)
    # )
    # result.extend(draw_train_lines(cur, start_hour=hours[0]))

    result.append('</svg>')
    return '\n'.join(result)


def from_timedelta_to_hour(t: timedelta) -> int:
    return t // 60 // 60


def decide_layout(con: sqlite3.Connection, route_name: str) -> (int, int, list[int]):
    cur = con.execute(
        '''
        SELECT max(route_station.relative_distance) as height
        FROM route_station
        JOIN route ON
            route.pk = route_station.route_fk
        WHERE route.name = ?
        ''',
        (route_name,)
    )
    height = round(cur.fetchone()['height'])

    cur = con.execute(
        '''
        SELECT min(timetable.time) as early
        FROM timetable
        JOIN station ON
            timetable.station_fk = station.pk
        JOIN route_station ON
            route_station.station_fk = station.pk
        JOIN route ON
            route.pk = route_station.route_fk
        WHERE
            route.name = ?
        AND
            timetable.previous IS NULL
        ''',
        (route_name,)
    )
    earliest_time = cur.fetchone()['early']
    earliest_hour = from_timedelta_to_hour(earliest_time)

    cur = con.execute(
        '''
        SELECT max(timetable.time) as late
        FROM timetable
        JOIN station ON
            timetable.station_fk = station.pk
        JOIN route_station ON
            route_station.station_fk = station.pk
        JOIN route ON
            route.pk = route_station.route_fk
        WHERE
            route.name = ?
        AND
            timetable.next IS NULL
        ''',
        (route_name,)
    )
    latest_time = cur.fetchone()['late']
    latest_hour = from_timedelta_to_hour(latest_time) + 1

    hours = tuple(i for i in range(earliest_hour, latest_hour + 1))
    width = (len(hours) - 1) * HOUR_GAP + 2 * PADDING
    return height, width, hours


if __name__ == '__main__':
    route_name = '西部幹線'
    con = setup_sqlite()
    with con:
        create_schema(con)
        load_data_from_json(
            con,
            pathlib.Path('./JSON/route.json'),
            pathlib.Path('./JSON/station.json'),
            pathlib.Path('./JSON/timetable.json'),
        )
        height, width, hours = decide_layout(con, route_name=route_name)

        with open('test.svg', mode='w') as f:
            f.write(draw(con=con, route_name=route_name, height=height, width=width, hours=hours))
