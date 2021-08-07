import argparse
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


def _download_and_save(url: str, path_: Path):
    with urlopen(url) as f:
        dumped_json = json.dumps(json.load(f), indent=4, ensure_ascii=False)
        path_.write_text(dumped_json, encoding='utf-8', errors='strict')


def download_and_save(urls: tuple[str, str]):
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        future_to_filename = {executor.submit(_download_and_save, url, path_): path_ for url, path_ in urls}
        for future in concurrent.futures.as_completed(future_to_filename):
            fn = future_to_filename[future]
            try:
                future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (fn, exc))


def get_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Download route, staion, and timetable.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-U',
        type=str, dest='root',
        help='The common part of the three URLs. It is okay to ends with or without trailing slash')
    parser.add_argument(
        '-T',
        type=str, dest='timetable_url',
        help='The part of URL different from the other two file URL for timetable download URL'
    )
    parser.add_argument(
        '-S',
        type=str, dest='station_rul', default='f0906cb8dcee4dfd9eb5f8a9a2bd0f5a',
        help='The part of URL different from the other two file URL for station information download URL')
    parser.add_argument(
        '-R',
        default='0518b833e8964d53bfea3f7691aea0ee', type=str, dest='route_url',
        help='The part of URL different from the other two file URL for route information download URL')

    parser.add_argument(
        '-O',
        default=Path('JSON'), type=Path, dest='output_folder',
        help='Output folder')
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
    if args.timetable_url or args.root:
        url_parse = urlparse(parser.root)
        timetable_url = urlunsplit(
            [url_parse.scheme, url_parse.netloc,
             url_parse.path + args.timetable_url, '', '']
        )
        route_url = urlunsplit(
            [url_parse.scheme, url_parse.netloc,
             url_parse.path + args.route_url, '', '']
        )
        station_url = urlunsplit(
            [url_parse.scheme, url_parse.netloc,
             url_parse.path + args.station_url, '', '']
        )
        urls = (
            (timetable_url, Path(f'{args.output_folder}/{args.timetable_name}.json')),
            (route_url, Path(f'{args.output_folder}/{args.route_name}.json')),
            (station_url, Path(f'{args.output_folder}/{args.station_name}.json'))
        )
    else:
        timetable_root_url =\
            'https://ods.railway.gov.tw/tra-ods-web/ods/download/dataResource/railway_schedule/JSON/list'
        timetable_url = get_timetalbe_download_url(timetable_root_url)
        urls = (
            (timetable_url, Path(f'{args.output_folder}/{args.timetable_name}.json')),
            ('https://ods.railway.gov.tw/tra-ods-web/ods/download/dataResource/f0906cb8dcee4dfd9eb5f8a9a2bd0f5a',
             Path(f'{args.output_folder}/{args.route_name}.json')),
            ('https://ods.railway.gov.tw/tra-ods-web/ods/download/dataResource/0518b833e8964d53bfea3f7691aea0ee',
             Path(f'{args.output_folder}/{args.station_name}.json'))
        )

    download_and_save(urls)
