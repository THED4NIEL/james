from os import path
from dbj import dbj
from functools import lru_cache
from modules.sessions import sessionpath


txDB_BEP20 = dbj(
    path.join(sessionpath, 'transactionDB_BEP20.json'), autosave=False)
txDB_NATIVE = dbj(
    path.join(sessionpath, 'transactionDB_NATIVE.json'), autosave=False)
walletDB = dbj(path.join(sessionpath, 'walletDB.json'), autosave=False)
contractDB = dbj(path.join(sessionpath, 'contractDB.json'), autosave=False)
crawldb = dbj('crawldb.temp')


def save_crawler_db():
    txDB_BEP20.save(indent=0)
    txDB_NATIVE.save(indent=0)
    walletDB.save(indent=0)
    contractDB.save(indent=0)


def reset_crawler_db():
    txDB_BEP20.clear()
    txDB_NATIVE.clear()
    walletDB.clear()
    contractDB.clear()

# region LAZY FUNCTIONS


def wallet_insert(wallet):
    walletDB.insert(wallet, wallet['address'])


def contract_insert(contract):
    contractDB.insert(contract, contract['contractAddress'])


def native_insert(transaction):
    return _insert(txDB_NATIVE, transaction)


def bep20_insert(transaction):
    return _insert(txDB_BEP20, transaction)
# endregion


# region LOGIC FUNCTIONS
def _insert(database, transaction):
    uid = create_checksum(transaction)
    if not database.exists(uid):
        database.insert(transaction, uid)


def create_checksum(entry: dict):
    return _calculate_checksum(frozenset(entry.items()))


@lru_cache
def _calculate_checksum(fs: frozenset):
    return str(hex(hash(fs) & 0xffffffff))
# endregion
