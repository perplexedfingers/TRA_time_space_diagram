from __future__ import annotations

import pathlib
import sqlite3
from functools import wraps
from math import ceil

from convert_to_sqlite import create_schema, load_data_from_json, setup_sqlite

HOURS = [h for h in range(31)]
HOUR_GAP = 1200
PADDING = 50
WIDTH = 36100  # HOUR_GAP * (len(HOURS) - 1) + 2 * PADDING
TEXT_GAP = 500


def draw_hour_lines(height: float) -> list[str]:
    ten_minute_gap = HOUR_GAP / 6  # There are 6 ten minutes in an hour
    bottom_y = PADDING + height

    result = []
    for i, hour in enumerate(HOURS):
        # lines for every hour
        hour_x = PADDING + i * HOUR_GAP
        result.append(f'<line x1="{hour_x}" x2="{hour_x}" y1="{PADDING}" y2="{bottom_y}" stroke="yellow" />')

        # text for every hour, goes down vertically along the hour line
        for j in range(0, ceil(height + TEXT_GAP), TEXT_GAP):
            result.append(f'<text fill="black" x="{hour_x}" y="{PADDING - 1 + j}">{"{:0>2d}00".format(hour)}</text>')

        # lines for every 10 minutes
        if i + 1 != len(HOURS):
            for j in range(1, 6):
                minute_x = hour_x + j * ten_minute_gap
                # different color for every 30 minutes
                if j != 3:
                    result.append(f'<line x1="{minute_x}" x2="{minute_x}" y1="{PADDING}" y2="{bottom_y}" stroke="red" />')
                else:
                    result.append(
                        f'<line x1="{minute_x}" x2="{minute_x}" y1="{PADDING}" y2="{bottom_y}" stroke="green" />')

                # text for every 10 minutes. goes down vertically along the minute line
                for k in range(0, ceil(height + TEXT_GAP), TEXT_GAP):
                    if j != 3:
                        result.append(f'<text fill="grey" x="{minute_x}" y="{PADDING - 1 + k}">{j}0</text>')
                    else:
                        result.append(f'<text fill="black" x="{minute_x}" y="{PADDING - 1 + k}">{j}0</text>')
    return result


def draw_station_lines(cur: sqlite3.Cursor) -> list[str]:
    result = []
    stations = cur.fetchmany()
    while stations:
        for row in stations:
            y = row['relative_distance'] + PADDING
            # lines for every station
            if row['is_active']:
                result.append(f'<line x1="{PADDING}" x2="{WIDTH - PADDING}" y1="{y}" y2="{y}" stroke="black" />')
            else:
                result.append(f'<line x1="{PADDING}" x2="{WIDTH - PADDING}" y1="{y}" y2="{y}" stroke="grey" />')

            # text for every station. goes right horiziontally along the station line
            for i in range(0, WIDTH + HOUR_GAP, HOUR_GAP):
                if row['is_active']:
                    result.append(f'<text fill="black" x="{i}" y="{y - 5}">{row["name"]}</text>')
                else:
                    result.append(f'<text fill="grey" x="{i}" y="{y - 5}">{row["name"]}</text>')
        stations = cur.fetchmany()
    return result


def svg_warpper(f):
    @wraps(f)
    def wrapper(cur: sqlite3.Cursor, height: float, *args, **kw) -> str:
        result = [
            '<?xml version="1.0" encoding="utf-8" ?>',
            '<?xml-stylesheet href="style.css" type="text/css" title="sometext" alternate="no" media="screen"?>',
            f'<svg xmlns="http://www.w3.org/2000/svg" style="font-family:Tahoma" width="{WIDTH}" height="{height + 100}">',
        ]

        result.extend(f(cur=cur, height=height, *args, **kw))

        result.append('</svg>')
        return '\n'.join(result)
    return wrapper


@svg_warpper
def draw_background(cur: sqlite3.Cursor, height: float) -> list[str]:
    result = []
    result.extend(draw_hour_lines(height))
    result.extend(draw_station_lines(cur))
    return result


if __name__ == '__main__':
    con = setup_sqlite(db_location=':memory:')
    create_schema(con)
    load_data_from_json(
        con,
        pathlib.Path('./JSON/route.json'),
        pathlib.Path('./JSON/station.json'),
        pathlib.Path('./JSON/timetable.json'),
    )

    cur = con.cursor()
    cur.execute(
        '''
        SELECT max(route_station.relative_distance) as height
        FROM route_station
        JOIN route ON
            route.pk = route_station.route_fk
        WHERE route.name = "西部幹線"
        '''
    )
    height = cur.fetchone()['height']
    cur.execute(
        '''
        SELECT station.is_active, station_name_cht.name, route_station.relative_distance
        FROM station
        JOIN station_name_cht ON
            station.pk = station_name_cht.station_fk
        JOIN route_station ON
            station.pk = route_station.station_fk
        JOIN route ON
            route.pk = route_station.route_fk
        WHERE route.name = "西部幹線"
        '''
        # GROUP BY route.name
    )

    with open('test.svg', mode='w') as f:
        f.write(draw_background(cur, height))
