from os import path
from dbj import dbj
from functools import lru_cache


transactionDB = dbj(
    path.join('.', 'json_db', 'transactionDB.json'), autosave=False)


def insert_tx(transaction):
    uid = create_checksum(transaction)
    if not transactionDB.exists(uid):
        transactionDB.insert(transaction, uid)


def create_checksum(entry: dict):
    return _calculate_checksum(frozenset(entry.items()))


@lru_cache
def _calculate_checksum(fs: frozenset):
    return str(hex(hash(fs) & 0xffffffff))
