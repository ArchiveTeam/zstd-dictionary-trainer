import functools
import gzip
import io
import multiprocessing.dummy
import random
import struct
import threading
import typing

import internetarchive
import requests
import zstandard

dictionary_lock = threading.Lock()


@functools.lru_cache()
def get_dictionary(filename: str) -> zstandard.ZstdCompressionDict:
    s = internetarchive.get_session()
    r = s.get(
        'https://archive.org/download/' + filename,
        headers={'Range': 'bytes=0-7'}
    )
    if r.content[:4] != b'\x5D\x2A\x4D\x18':
        return None
    data_size = struct.unpack('<L', r.content[4:])[0]
    r = s.get(
        'https://archive.org/download/' + filename,
        headers={'Range': 'bytes=8-{}'.format(8+data_size-1)}
    )
    dictionary = r.content
    if r.content[:4] == b'\x28\xB5\x2F\xFD':
        dictionary = zstandard.ZstdDecompressor().decompress(dictionary)
    if dictionary[:4] != b'\x37\xA4\x30\xEC':
        raise ValueError('Not a dictionary.')
    return zstandard.ZstdCompressionDict(dictionary)


def archive_url(data: typing.Dict[str, bytes],
                url_data: typing.Tuple[str, int, int, str, bool]):
    url, offset, length, filename, redownload = url_data
    if redownload:
        data[url] = requests.get(url, allow_redirects=False).content
    else:
        if filename.endswith('.zst'):
            with dictionary_lock:
                dictionary = get_dictionary(filename)
        r = internetarchive.get_session().get(
            'https://archive.org/download/' + filename,
            headers={'Range': 'bytes={}-{}'.format(offset, offset+length-1)}
        )
        if filename.endswith('.zst'):
            data[url] = zstandard.ZstdDecompressor(dict_data=dictionary) \
                .decompressobj().decompress(r.content)
        elif filename.endswith('.gz'):
            data[url] = gzip.decompress(r.content)
        elif filename.endswith('.warc'):
            data[url] = r.content
        else:
            raise ValueError('WARC type not supported.')
    print(len(data[url]), url)


def get_urls(urls: typing.Iterable[str],
             concurrency: int) -> typing.Dict[str, bytes]:
    data = {}
    with multiprocessing.dummy.Pool(concurrency) as p:
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


def from_cdx(data: typing.IO, sample_size: int=4000,
             redownload: bool=False) -> typing.Set[str]:
    all_data = []
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
            offset_index = line.index('V') - 1
            length_index = line.index('S') - 1
            file_index = line.index('g') - 1
            continue
        if line[mimetype_index] in (
            'text/html',
            'application/json',
            'text/xml'
        ):
            all_data.append((
                line[original_url_index],
                int(line[offset_index]),
                int(line[length_index]),
                line[file_index],
                redownload
            ))
    return set(random.choices(all_data, k=min(len(all_data), sample_size)))


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

