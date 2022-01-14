from os import path
from dbj import dbj
from functools import lru_cache


txDB_BEP20 = dbj(
    path.join('.', 'json_db', 'transactionDB_BEP20.json'), autosave=False)
txDB_NATIVE = dbj(
    path.join('.', 'json_db', 'transactionDB_NATIVE.json'), autosave=False)
walletDB = dbj(path.join('.', 'json_db', 'walletDB.json'), autosave=False)
contractDB = dbj(path.join('.', 'json_db', 'contractDB.json'), autosave=False)
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


def wallet_data(address):
    return _data(walletDB, address)


def wallet_exists(address):
    return walletDB.exists(address)


def wallet_keys(query=''):
    return _keys(walletDB, query)


def contract_insert(contract):
    contractDB.insert(contract, contract['contractAddress'])


def contract_exists(address):
    return contractDB.exists(address)


def contract_data(address):
    return _data(contractDB, address)


def contract_keys(query=''):
    return _keys(contractDB, query)


def native_insert(transaction):
    return _insert(txDB_NATIVE, transaction)


def native_data(uid):
    return _data(txDB_NATIVE, uid)

def native_keys(query=''):
    return _keys(txDB_NATIVE, query)


def bep20_insert(transaction):
    return _insert(txDB_BEP20, transaction)


def bep20_data(uid):
    return _data(txDB_BEP20, uid)


def bep20_keys(query=''):
    return _keys(txDB_BEP20, query)
# endregion


# region LOGIC FUNCTIONS
def _insert(database, transaction):
    uid = create_checksum(transaction)
    if not database.exists(uid):
        database.insert(transaction, uid)
        return True
    else:
        return False


def _data(database, key):
    return database.getall() if key == '*' else database.get(key)


def _keys(database, query=''):
    return database.getallkeys() if query == '' else database.find(query)


def create_checksum(entry: dict):
    return _calculate_checksum(frozenset(entry.items()))


@lru_cache
def _calculate_checksum(fs: frozenset):
    return str(hex(hash(fs) & 0xffffffff))
# endregion
