import csv
import json
from pathlib import Path
from urllib.parse import urlparse, urlunsplit
from urllib.request import urlopen

from bs4 import BeautifulSoup


def get_timetalbe_download_url(root_csv_url: str) -> str:
    with urlopen(root_csv_url) as f:
        lines = map(lambda s: s.decode(), f.readlines())
    reader = csv.reader(lines)

    json_file_list_url = next(r for r in reader if 'JSON' in r[0] or 'json' in r[0])[1]
    with urlopen(json_file_list_url) as f:
        html = BeautifulSoup(f, 'html.parser')
    json_file_html_element = html.a  # First "a" element. Might be wrong

    url_parse = urlparse(root_csv_url)
    json_file_url = urlunsplit(
        [url_parse.scheme, url_parse.netloc,
         json_file_html_element['href'], '', '']
    )
    return json_file_url


def download_and_save(url: str, file_name: str):
    with urlopen(url) as f:
        dumped_json = json.dumps(json.load(f), indent=4, ensure_ascii=False)
        Path(file_name).write_text(dumped_json, encoding='utf-8', errors='strict')


if __name__ == '__main__':
    # TODO maybe use args?
    timetable_root_url =\
        'https://ods.railway.gov.tw/tra-ods-web/ods/download/dataResource/8ae4cac374d0fbc50174d350a9aa04db'
    timetalbe_file_name = 'timetable.json'
    timetable_url = get_timetalbe_download_url(timetable_root_url)

    download_and_save(timetable_url, timetalbe_file_name)

    route_url = 'https://ods.railway.gov.tw/tra-ods-web/ods/download/dataResource/f0906cb8dcee4dfd9eb5f8a9a2bd0f5a'
    route_file_name = 'route.json'
    download_and_save(route_url, route_file_name)

    station_url = 'https://ods.railway.gov.tw/tra-ods-web/ods/download/dataResource/0518b833e8964d53bfea3f7691aea0ee'
    station_file_name = 'station.json'
    download_and_save(station_url, station_file_name)
