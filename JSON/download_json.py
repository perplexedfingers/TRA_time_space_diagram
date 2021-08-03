import concurrent.futures
import json
from pathlib import Path
from urllib.parse import urlparse, urlunsplit
from urllib.request import urlopen

from bs4 import BeautifulSoup


def get_timetalbe_download_url(root_url: str) -> str:
    with urlopen(root_url) as f:
        html = BeautifulSoup(f, 'html.parser')
    json_file_html_element = html.a  # First "a" element. Might be wrong

    url_parse = urlparse(root_url)
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
    # TODO use args?
    timetable_root_url =\
        'https://ods.railway.gov.tw/tra-ods-web/ods/download/dataResource/railway_schedule/JSON/list'
    timetable_url = get_timetalbe_download_url(timetable_root_url)

    urls = (
        (timetable_url, 'timetable.json'),
        ('https://ods.railway.gov.tw/tra-ods-web/ods/download/dataResource/f0906cb8dcee4dfd9eb5f8a9a2bd0f5a'
         'route.json'),
        ('https://ods.railway.gov.tw/tra-ods-web/ods/download/dataResource/0518b833e8964d53bfea3f7691aea0ee',
         'station.json')
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        future_to_filename = {executor.submit(download_and_save, url, fn): fn for url, fn in urls}
        for future in concurrent.futures.as_completed(future_to_filename):
            fn = future_to_filename[future]
            try:
                future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (fn, exc))
