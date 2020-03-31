import functools
import io
import typing

import internetarchive
import mock
import requests

MAIN_URL_BASE = 'https://transfer.notkiska.pw/'


def upload_file(function):
    def wrapper(data: typing.BinaryIO, *args, **kwargs):
        position = data.tell()
        data.seek(io.SEEK_SET)
        response = function(data, *args, **kwargs)
        data.seek(position)
        return response
    return wrapper


@upload_file
def upload_to_backup(data: typing.BinaryIO, filename: str, itemname: str,
                     collection: str='test_collection'):
    with mock.patch('internetarchive.item.recursive_file_count',
                    lambda files, *args, **kwargs: len(files)):
        response = internetarchive.upload(
            itemname,
            {
                filename: io.BytesIO(data.read()) # IA closes file
            },
            metadata={
                'title': itemname,
                'mediatype': 'data',
                'collection': collection,
            },
            queue_derive=False,
            verify=True,
            verbose=True,
            delete=False,
            retries=10,
            retries_sleep=10
        )
        response[0].raise_for_status()
        return 'https://archive.org/download/{}/{}'.format(itemname, filename)


@upload_file
def upload_to_public(data: typing.BinaryIO, filename: str) -> str:
    response = requests.put(MAIN_URL_BASE + filename, data=data.read())
    response.raise_for_status()
    return response.text

__all__ = ('upload_to_backup', 'upload_to_public')

