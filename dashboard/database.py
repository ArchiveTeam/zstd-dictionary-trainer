import contextlib
import os
import sqlite3
import typing

DATABASE_DB = os.path.join('dashboard', 'dictionaries.db')
DATABASE_SQL = os.path.join('dashboard', 'dictionaries.sql')


def dictionary_response(function) -> typing.Callable[[list, dict], dict]:
    def wrapper(*args, **kwargs) -> typing.Dict[str, str]:
        result = function(*args, **kwargs)
        if result is None:
            raise KeyError('Data not found.')
        return {
            'id': result[0],
            'url': result[1],
            'sha256': result[2]
        }
    return wrapper


def create_database():
    with cursor(create=True) as cur, open(DATABASE_SQL, 'r') as f:
        cur.execute(f.read())


@contextlib.contextmanager
def cursor(create=False):
    if not create and not os.path.isfile(DATABASE_DB):
        create_database()
    database = sqlite3.connect(DATABASE_DB)
    cursor = database.cursor()
    try:
        yield cursor
    finally:
        cursor.close()
        database.commit()
        database.close()


@dictionary_response
def get_latest_dictionary_url(project: str) -> typing.Tuple[str, str]:
    with cursor() as cur:
        cur.execute(
            'SELECT id,public_url,sha256 '
            'FROM dictionaries '
            'WHERE project=? '
            'ORDER BY id DESC '
            'LIMIT 1',
            (project,)
        )
        return cur.fetchone()


@dictionary_response
def get_dictionary_url(project: str, version: int) -> typing.Tuple[str, str]:
    with cursor() as cur:
        cur.execute(
            'SELECT id,public_url,sha256 '
            'FROM dictionaries '
            'WHERE id=? AND project=?'
            'LIMIT 1',
            (version, project)
        )
        return cur.fetchone()


def add_entry(identifier: int, project: str, sha256: str, public_url: str,
              backup_url: str):
    with cursor() as cur:
        cur.execute(
            'INSERT INTO dictionaries(id, project, sha256, public_url, '
            'backup_url) '
            'VALUES (?,?,?,?,?)',
            (identifier, project, sha256, public_url, backup_url)
        )

__all__ = ('get_latest_dictionary', 'get_dictionary')

