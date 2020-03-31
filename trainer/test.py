import gzip
import typing

import zstandard


def test(data: typing.Dict[str, bytes],
         dictionary: zstandard.ZstdCompressionDict,
         test_without_dict: bool=True,
         test_gzip: bool=True) -> typing.Dict[str, dict]:
    context_dict = zstandard.ZstdCompressor(dict_data=dictionary)
    if test_without_dict:
        context_without_dict = zstandard.ZstdCompressor()
    results = {}
    for url, content in data.items():
        sizes = {
            'original': len(content),
            'zstd+dict': len(context_dict.compress(content))
        }
        if test_without_dict:
            sizes['zstd'] = len(context_without_dict.compress(content))
        if test_gzip:
            sizes['gzip'] = len(gzip.compress(content, compresslevel=9))
        results[url] = sizes
    return {
        'single': results,
        'sum': {
            k: sum(d[k] for d in results.values())
            for k in next(iter(results.values()))
        }
    }

