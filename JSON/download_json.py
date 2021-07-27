from pathlib import Path
from urllib.parse import urlparse, urlunsplit
from urllib.request import urlopen
import csv
import json

from bs4 import BeautifulSoup


def downlaod_latest_json(root_csv_url: str) -> dict:
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
    with urlopen(json_file_url) as f:
        result = json.load(f)
    return result


if __name__ == '__main__':
    # TODO maybe use args?
    root_csv_url = 'https://ods.railway.gov.tw/tra-ods-web/ods/download/dataResource/8ae4cac374d0fbc50174d350a9aa04db'
    file_name = 'timetable.json'

    timetable_json = downlaod_latest_json(root_csv_url)
    dumped_json = json.dumps(timetable_json, indent=4, ensure_ascii=False)
    Path(file_name).write_text(dumped_json, encoding='utf-8', errors='strict')
