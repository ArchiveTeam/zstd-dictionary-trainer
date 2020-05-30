import argparse
import threading
import time

import internetarchive

from dashboard import run
from dashboard.database import add_entry
from trainer import Trainer, DummyTrainer
from trainer.urls import from_cdx_url


def cdx_url(collection: str) -> str:
    session = internetarchive.get_session()
    response = session.get(
        'https://archive.org/advancedsearch.php',
        params={
            'q': (
                'collection:archiveteam* '
                'AND format:(Item CDX Index) '
                'AND identifier:{}*'.format(collection)
            ),
            'fl[]': 'identifier',
            'sort[]': 'addeddate desc',
            'rows': '1',
            'output': 'json'
        }
    ).json()
    print(response)
    identifier = response['response']['docs'][0]['identifier']
    return 'https://archive.org/download/{0}/{0}.cdx.gz'.format(identifier)


def run_dashboard(port: int) -> threading.Thread:
    thread = threading.Thread(target=run, args=(port,))
    thread.daemon = True
    thread.start()
    return thread


def main(collection: str, name: str, concurrency: int, dummy: bool=False):
    if dummy:
        t = DummyTrainer()
    else:
        urls = from_cdx_url(cdx_url(collection), session=internetarchive.get_session())
        t = Trainer(urls, concurrency=concurrency)
    identifier = str(int(time.time()))
    filename_base = collection + '_dictionary_' + identifier
    upload_urls = t.upload(filename_base + '.zstdict.zst', filename_base)
    print(upload_urls)
    add_entry(identifier, name, t.sha256, upload_urls['public_url'],
              upload_urls['backup_url'])

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--collection', required=True, type=str,
                        help='Collection on Internet Archive')
    parser.add_argument('-n', '--name', required=True, type=str,
                        help='Name of the project.')
    parser.add_argument('-p', '--port', default=25654, type=int,
                        help='Port to use for the dashboard')
    parser.add_argument('--concurrency', default=50, type=int,
                        help='Concurrency to download WARC samples with.')
    parser.add_argument('--init-project', action='store_true',
                        help='Init dummy ZSTD dictionary for project.')
    args = parser.parse_args()
    thread = run_dashboard(args.port)
    while True:
        try:
            main(args.collection, args.name, args.concurrency,
                 args.init_project)
        except Exception as e:
            print('Could not train new dictionary.', str(e))
            time.sleep(300)
            continue
        time.sleep(3600)

