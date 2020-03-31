import argparse
import os
import typing

from trainer import dump, train
from trainer.test import test
from trainer.urls import list_urls, get_urls


def main(urls: typing.Optional[typing.List[str]],
         filepath: typing.Optional[str], output: typing.Optional[str],
         concurrent: int):
    data = get_urls(list_urls(urls, filepath), concurrent)
    print('got', sum(len(d) for s in data.values()), 'bytes')
    dictionary = train(data)
    #TODO print compression test
    print('dumping dictionary')
    with open(output, 'w') as f:
        dump(dictionary, f)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--url', type=str, action='append',
                        help='URLs to train with, repeatable.')
    parser.add_argument('-f', '--file', type=str,
                        help='File containing URLs to train with.')
    parser.add_argument('-o', '--output', type=str,
                        help='Location to save the dictionary.')
    parser.add_argument('-c', '--concurrent', type=int, default=50,
                        help='Concurrent number of URLs to download.')
    args = parser.parse_args()
    if args.url is None and args.file is None:
        raise Exception('At least one URL or file should be provided.')

