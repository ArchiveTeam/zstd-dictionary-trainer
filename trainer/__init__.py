import functools
import hashlib
import io
import struct
import typing

import zstandard

from trainer.test import test
from trainer.upload import upload_to_public, upload_to_backup
from trainer.urls import get_urls

__VERSION__ = '20200329.01'


class Trainer:
    def __init__(self, urls: typing.Set[str]=None, concurrent: int=50,
                 dict_size: int=1024**2, k: int=100000, d: int=9,
                 level: int=9):
        self._urls = urls or set()
        self._concurrent = concurrent
        self._k = k
        self._d = d
        self._max_dict_size = dict_size
        self._level = level

    def add_url(self, url: str):
        self._urls.add(url)
        self.reset()

    def reset(self):
        del self.train_data
        del self.dictionary
        del self.io
        del self.hash

    def upload(self, filename: str, itemname: str) -> typing.Dict[str, str]:
        return {
            'public_url': self.upload_public(filename),
            'backup_url': self.upload_backup(filename, itemname)
        }

    def test(self):
        return test(self.train_data, self.dictionary)

    def upload_public(self, filename: str) -> str:
        return upload_to_public(self._io, filename)

    def upload_backup(self, filename: str, itemname: str) -> str:
        return upload_to_backup(self._io, filename, itemname)

    @functools.cached_property
    def sha256(self) -> str:
        return hashlib.sha256(self.dictionary.as_bytes()).hexdigest()

    @functools.cached_property
    def _io(self) -> typing.BinaryIO:
        bytes_io = io.BytesIO()
        bytes_io.write(self.dictionary.as_bytes())
        return bytes_io

    @functools.cached_property
    def train_data(self) -> typing.Dict[str, bytes]:
        return get_urls(self.urls, self._concurrent)

    @property
    def urls(self) -> typing.Set[str]:
        return self._urls

    @functools.cached_property
    def dictionary(self) -> zstandard.ZstdCompressionDict:
        return train(self.train_data, self._max_dict_size, self._k, self._d,
                     self._level)

    @property
    def dict_size(self) -> int:
        return len(self.dictionary.as_bytes())


def dump(dictionary: zstandard.ZstdCompressionDict,
         out: typing.Union[typing.BinaryIO, str], compress: bool=False,
         skippable_frame: bool=False) -> int:
    data = dictionary.as_bytes
    if compress:
        data = zstandard.ZstdCompressor().compress(data)
    if skippable_frame:
        data = b'\x5D\x2A\x4D\x18' + struct.pack('<L', len(data)) + data
    if type(out) is str:
        with open(out, 'wb') as f:
            return f.write(data)
    return out.write(data)


def train(data: typing.Dict[str, bytes], dict_size: int=1024**2, k: int=100000,
          d: int=8, level: int=9) -> zstandard.ZstdCompressionDict:
    return zstandard.train_dictionary(dict_size, list(data.values()), k=k, d=d,
                                      level=level)
