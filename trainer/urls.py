import functools
import gzip
import io
import multiprocessing.dummy
import random
import typing

import requests


def archive_url(data: typing.Dict[str, bytes], url: str):
    data[url] = requests.get(url, allow_redirects=False).content
    print(len(data[url]), url)


def get_urls(urls: typing.Iterable[str],
             concurrent: int) -> typing.Dict[str, bytes]:
    data = {}
    with multiprocessing.dummy.Pool(concurrent) as p:
        p.starmap(archive_url, ((data, url) for url in urls))
    return data


def list_urls(urls: typing.Optional[typing.Set[str]],
              filepath: typing.Optional[str]) -> typing.FrozenSet[str]:
    urls = set()
    if args.url is not None:
        urls |= set(args.url)
    if args.file is not None:
        with open(args.file, 'r') as f:
            for line in f:
                line = line.strip()
                if len(line) == 0:
                    continue
                urls.add(line)
    return frozenset(urls)


def from_cdx(data: typing.IO, sample_size: int=4000) -> typing.Set[str]:
    all_data = set()
    original_url_index = None
    mimetype_index = None
    for line in data:
        if type(line) is bytes:
            line = line.decode('utf8')
        line = line.strip().split()
        if mimetype_index is None:
            print(line)
            original_url_index = line.index('a') - 1
            mimetype_index = line.index('m') - 1
            continue
        if line[mimetype_index] in ('text/html', 'application/json'):
            all_data.add(line[original_url_index])
    return set(random.sample(all_data, min(len(all_data), sample_size)))


def from_cdx_url(url: str, session=requests) -> typing.Set[str]:
    response = session.get(url)
    response.raise_for_status()
    data = response.content
    if data.startswith(b'\x1F\x8B'):
        data = gzip.decompress(data)
    return from_cdx(io.BytesIO(data))


def from_cdx_file(filepath: str) -> typing.Set[str]:
    if filepath.endswith('.gz'):
        with gzip.open(filepath, 'rb') as f:
            return from_cdx(f)
    with open(filepath, 'rb') as f:
        return from_cdx(f)

__all__ = ('process_urls', 'get_urls', 'from_cdx', 'from_cdx_url',
           'from_cdx_file')

