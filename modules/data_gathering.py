import _thread as thread
import os
import queue
import time
from enum import Enum, auto

from functools import lru_cache, reduce
from bscscan import BscScan
from dbj import dbj
from ratelimit import limits, sleep_and_retry
from web3 import Web3
from web3.main import Web3

# region API CALL LIMITER
APICALLS_PER_SECOND = 4


@sleep_and_retry
@limits(calls=APICALLS_PER_SECOND, period=1)
def check_API_limit():
    'Solution by Kjetil Svenheim - https://stackoverflow.com/questions/40748687/python-api-rate-limiting-how-to-limit-api-calls-globally'
    return
# endregion

# region CLASS DEFINITIONS


class Direction(Enum):
    LEFT = auto()
    RIGHT = auto()
    ALL = auto()


class SearchType(Enum):
    TX = auto()
    ADDR = auto()


class ADDRESS(str):
    def __init__(self, input: str):
        if not isinstance(input, str):
            raise ValueError('input must be string')
        input = input.strip()

        if len(input) != 42:
            raise ValueError('address length is incorrect')
        if input[:2] != '0x':
            raise ValueError('address must start with 0x')

        self = input.lower()

    def __eq__(self, other):
        return (self.casefold() == other.casefold())

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self) -> int:
        return super().__hash__()


class TXHASH(str):
    def __init__(self, input: str):
        if not isinstance(input, str):
            raise ValueError('input must be string')
        input = input.strip()

        if len(input) != 66:
            raise ValueError('transaction hash length is incorrect')
        if input[:2] != '0x':
            raise ValueError('transaction hash must start with 0x')

        self = input.lower()

    def __eq__(self, other):
        return (self.casefold() == other.casefold())

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self) -> int:
        return super().__hash__()

# endregion

# region API retry wrapper for connection timeout


def APIretry(func):
    def wrap(*args, **kwargs):
        timeouts = 0
        while True:
            try:
                result = func(*args, **kwargs)
            except (ConnectionError, TimeoutError) as e:
                timeouts += 1
                if timeouts == 3:
                    raise e
                else:
                    continue
            except AssertionError as a:
                if a.args[0] == 'Max rate limit reached -- NOTOK':
                    continue
                else:
                    raise a
            else:
                break

        return result
    return wrap
# endregion

# region CRAWLER FUNCTIONS


def recursive_search_by_address_and_contract(addresses: list, contract: ADDRESS, direction: Direction, threads: int, trackBEP20: bool, trackNative: bool, followNative: bool):
    for address in addresses:
        if not isinstance(address, ADDRESS):
            address = ADDRESS(address)
        crawler_queue.put(address)

    for _ in range(threads):
        time.sleep(0.25)
        t = thread.start_new_thread(
            retrieve_transactions_by_address_and_contract, (direction, trackBEP20, trackNative, followNative, contract))
        crawler_threads.append(t)

    while len(crawler_threads) > 0:
        time.sleep(1)


def retrieve_transactions_by_address_and_contract(direction: Direction, trackBEP20: bool, trackNative: bool, followNative: bool, contract: ADDRESS, startblock=0, endblock=0):
    def identify_contract(address):
        is_contract = False

        if not contractDB.exists(address):
            circulating = get_circulating_supply(address)
            source = get_source(address)
            if circulating > 0:
                # * IS A TOKEN
                is_contract = True

                bytecode = get_bytecode(address)
                beptx = get_first_bep20_transaction(address)

                save_contract_information(
                    beptx, source, bytecode, True)
            elif source[0]['CompilerVersion'] != '':
                # * CONTRACT CONFIRMED, SAVE FOR FURTHER CLASSIFICATION
                is_contract = True

                bytecode = get_bytecode(address)
                nattx = get_first_native_transaction(address)

                save_contract_information(
                    nattx, source, bytecode, False)
            else:
                # * NO SIGNS OF A CONTRACT FOUND
                is_contract = False
        else:
            is_contract = True
        return is_contract

    @APIretry
    def get_circulating_supply(address):
        check_API_limit()
        return int(bsc.get_circulating_supply_by_contract_address(
            address))

    @APIretry
    def get_source(address):
        check_API_limit()
        return bsc.get_contract_source_code(address)

    @APIretry
    def get_bytecode(address):
        check_API_limit()
        return bsc.get_proxy_code_at(address)

    @APIretry
    def get_first_bep20_transaction(address):
        check_API_limit()
        return bsc.get_bep20_token_transfer_events_by_contract_address_paginated(
            contract_address=address, page=1, offset=1, sort='asc')

    @APIretry
    def get_first_native_transaction(address):
        check_API_limit()
        return bsc.get_normal_txs_by_address_paginated(
            address=address, page=1, offset=1, startblock=0, endblock=999999999, sort='asc')

    def save_contract_information(first_tx, source, bytecode, isToken):
        if isToken == True:
            name = first_tx[0]['tokenName']
            symbol = first_tx[0]['tokenSymbol']
            decimals = first_tx[0]['tokenDecimal']
            ctype = 'token'
        else:
            name = symbol = decimals = ''
            ctype = 'contract'

        entry = {'type': ctype,
                 'ContractAddress': address,
                 'Name': name,
                 'Symbol': symbol,
                 'Decimals': decimals,
                 'ABI': source[0]['ABI'],
                 'ContractName': source[0]['ContractName'],
                 'CompilerVersion': source[0]['CompilerVersion'],
                 'OptimizationUsed': source[0]['OptimizationUsed'],
                 'Runs': source[0]['Runs'],
                 'ConstructorArguments': source[0]['ConstructorArguments'],
                 'EVMVersion': source[0]['EVMVersion'],
                 'Library': source[0]['Library'],
                 'LicenseType': source[0]['LicenseType'],
                 'Proxy': source[0]['Proxy'],
                 'Implementation': source[0]['Implementation'],
                 'SwarmSource': source[0]['SwarmSource'],
                 'Bytecode': bytecode,
                 'first_transaction': first_tx[0]}
        contractDB.insert(entry, address)

    @APIretry
    def get_bep20_transactions(address, contract_provided=False, startblock=0, endblock=0):
        # TODO: find a way to get above a 20k transaction limit
        #! startblk = get_bep20_token_transfer_events_by_address_and_contract_paginated(contract, address, page, 1, 'asc)
        #! endblk = get_bep20_token_transfer_events_by_address_and_contract_paginated(contract, address, page, 1, 'desc)
        #! use creation block of token as starting point, get_proxy_block_by_number from there by blocksize +1
        #! use latest tx block as end point
        #! store whole blocks in db and filter/dbfind by contract, then dbgetmany
        page = 1
        sort = 'asc'
        number_of_records = 10000  # max
        nonlocal bep20_transactions
        bep20_queryresult = []

        while True:

            if contract_provided:
                check_API_limit()
                bep20_queryresult = bsc.get_bep20_token_transfer_events_by_address_and_contract_paginated(
                    contract_address=contract, address=address, page=page, offset=number_of_records, sort=sort)
            else:
                check_API_limit()
                bep20_queryresult = bsc.get_bep20_token_transfer_events_by_address(
                    address=address, startblock=startblock, endblock=endblock, sort=sort)

            bep20_transactions.extend(bep20_queryresult)
            if len(bep20_queryresult) < number_of_records or len(bep20_transactions) == 20000:
                break
            if len(bep20_transactions) == 10000:
                sort = 'desc'
                page = 1
            else:
                page += 1

    def process_bep20_transactions():
        # * process bep20 transactions
        # * check if tx is already indexed, if not, add to db
        # * crawl each transaction for receivers or senders and add them to the queue
        nonlocal bep20_transactions, highest_block, lowest_block, outgoing_wallets, incoming_wallets, bep20_tx_id_collection

        if bep20_transactions:
            highest_block = lowest_block = int(
                bep20_transactions[0]['blockNumber'])
        for transaction in bep20_transactions:
            id = create_checksum(transaction)
            if not transactionDB_BEP20.exists(id):
                transactionDB_BEP20.insert(
                    transaction, id)
            if (
                transaction['from'] == address
                and transaction['to'] != address
                and transaction['to'] not in donotfollow
            ):
                outgoing_wallets.add(
                    transaction['to'])
            if (
                transaction['from'] != address
                and transaction['to'] == address
                and transaction['from'] not in donotfollow
            ):
                incoming_wallets.add(
                    transaction['from'])
            block = int(transaction['blockNumber'])
            if block < lowest_block:
                lowest_block = block
            if block > highest_block:
                highest_block = block
            bep20_tx_id_collection.append(id)

    @APIretry
    def get_native_transactions(address, startblock=0, endblock=0):
        page = 1
        sort = 'asc'
        number_of_records = 10000  # max
        nonlocal native_transactions

        while True:
            nat_tx_queryresult = []

            if startblock == endblock == 0:
                check_API_limit()
                nat_tx_queryresult = bsc.get_normal_txs_by_address_paginated(
                    address=address, page=page, offset=number_of_records, startblock=0, endblock=999999999, sort=sort)
            else:
                check_API_limit()
                nat_tx_queryresult = bsc.get_normal_txs_by_address_paginated(
                    address=address, page=page, offset=number_of_records, startblock=startblock, endblock=endblock, sort=sort)

            native_transactions.extend(nat_tx_queryresult)
            if len(nat_tx_queryresult) < number_of_records or len(native_transactions) == 20000:
                break
            if len(native_transactions) == 10000:
                # * limit of 10k can be circumvented by changing sort order (max 20k)
                sort = 'desc'
                page = 1
            else:
                page += 1

    def process_native_transactions():
        # * process native transactions
        # * check if tx is already indexed, if not, add to db
        # * crawl each transaction for receivers or senders and add them to the queue
        nonlocal native_transactions, outgoing_wallets, incoming_wallets, nat_tx_id_collection

        for transaction in native_transactions:
            id = create_checksum(transaction)
            if not transactionDB_NATIVE.exists(id):
                transactionDB_NATIVE.insert(transaction, id)

            if (
                transaction['from'] == address
                and transaction['to'] != address
                and followNative
                and transaction['to'] not in donotfollow
            ):
                outgoing_wallets.add(
                    transaction['to'])
            if (
                transaction['from'] != address
                and transaction['to'] == address
                and followNative
                and transaction['from'] not in donotfollow
            ):
                incoming_wallets.add(
                    transaction['from'])

            nat_tx_id_collection.append(id)

    def feed_wallets_to_crawler_queue():
        nonlocal direction, outgoing_wallets, incoming_wallets
        if direction == Direction.LEFT:
            for result in incoming_wallets:
                crawler_queue.put(result)
        if direction == Direction.RIGHT:
            for result in outgoing_wallets:
                crawler_queue.put(result)
        if direction == Direction.ALL:
            all_wallets = set.union(
                incoming_wallets, outgoing_wallets)
            for result in all_wallets:
                crawler_queue.put(result)

    if trackNative == trackBEP20 == False:
        raise Exception

    if trackBEP20 == False and startblock == endblock == 0:
        raise Exception

    contract_provided = contract != ''
    outgoing_wallets = set()
    incoming_wallets = set()
    bep20_tx_id_collection = []
    nat_tx_id_collection = []
    native_transactions = []
    bep20_transactions = []

    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        while True:
            try:
                address = crawler_queue.get(block=True, timeout=10)
            except queue.Empty:
                crawler_threads.remove(thread.get_ident())
                break
            else:
                is_contract = identify_contract(address)
                is_indb = walletDB.exists(address)
                lowest_block = 0
                highest_block = 0

                if not is_indb and not is_contract:

                    if trackBEP20:
                        try:
                            get_bep20_transactions(
                                address, contract_provided, startblock, endblock)
                        except AssertionError as a:
                            if a.args[0] == '[] -- No transactions found':
                                bep20_transactions = []

                        process_bep20_transactions()

                    if trackNative:
                        try:
                            get_native_transactions(
                                address, startblock, endblock)
                        except AssertionError as a:
                            if a.args[0] == '[] -- No transactions found':
                                native_transactions = []

                        process_native_transactions()

                    wallet = {'address': address, 'bep20_tx_ids': bep20_tx_id_collection,
                              'native_tx_ids': nat_tx_id_collection, 'children': list(outgoing_wallets), 'parents': list(incoming_wallets)}
                    walletDB.insert(wallet, address)

                    feed_wallets_to_crawler_queue()

                    outgoing_wallets.clear()
                    incoming_wallets.clear()
                    bep20_transactions.clear()
                    bep20_tx_id_collection.clear()
                    native_transactions.clear()
                    nat_tx_id_collection.clear()


def create_checksum(entry: dict):
    return calculate_checksum(frozenset(entry.items()))


@lru_cache
def calculate_checksum(fs: frozenset):
    checksum = hash(fs)
    return str(hex(checksum & 0xffffffff))


def check_if_token(contract_address: ADDRESS):
    @APIretry
    def get_sample_transaction(contract_address):
        check_API_limit()
        return bsc.get_bep20_token_transfer_events_by_contract_address_paginated(
            contract_address=contract_address, page=1, offset=1, sort='asc')

    @APIretry
    def get_bytecode(contract_address):
        check_API_limit()
        return bsc.get_proxy_code_at(
            address=contract_address)

    @APIretry
    def get_source(contract_address):
        check_API_limit()
        return bsc.get_contract_source_code(
            contract_address=contract_address)

    @APIretry
    def get_circulating_supply(contract_address):
        check_API_limit()
        return int(bsc.get_circulating_supply_by_contract_address(
            contract_address=contract_address))

    def save_token_information(data, source, bytecode):
        name = data[0]['tokenName']
        symbol = data[0]['tokenSymbol']
        decimals = data[0]['tokenDecimal']

        token = {'type': 'token',
                 'ContractAddress': contract_address,
                 'Name': name,
                 'Symbol': symbol,
                 'Decimals': decimals,
                 'ABI': source[0]['ABI'],
                 'ContractName': source[0]['ContractName'],
                 'CompilerVersion': source[0]['CompilerVersion'],
                 'OptimizationUsed': source[0]['OptimizationUsed'],
                 'Runs': source[0]['Runs'],
                 'ConstructorArguments': source[0]['ConstructorArguments'],
                 'EVMVersion': source[0]['EVMVersion'],
                 'Library': source[0]['Library'],
                 'LicenseType': source[0]['LicenseType'],
                 'Proxy': source[0]['Proxy'],
                 'Implementation': source[0]['Implementation'],
                 'SwarmSource': source[0]['SwarmSource'],
                 'Bytecode': bytecode,
                 'first_transaction': data[0]}
        contractDB.insert(token, contract_address)

    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        is_token = False
        contract = contractDB.get(contract_address)
        if not contract:
            circulating = get_circulating_supply(contract_address)
            if circulating > 0:
                is_token = True

                source = get_source(contract_address)

                bytecode = get_bytecode(contract_address)

                txdata = get_sample_transaction(contract_address)

                save_token_information(txdata, source, bytecode)
        elif contract['type'] == 'token':
            is_token = True
        return is_token


def follow_tokenflow_by_address(addresses: list, contract_address: ADDRESS, direction: Direction, trackBEP20: bool, trackNative: bool, followNative: bool):
    is_token = check_if_token(contract_address)

    if not isinstance(addresses, list):
        addresses = [ADDRESS(addresses)]
    else:
        tmp = addresses
        for addr in tmp:
            if not isinstance(addr, ADDRESS):
                addresses.remove(addr)
                addresses.append(ADDRESS(addr))

    if is_token:
        recursive_search_by_address_and_contract(
            addresses=addresses, contract=contract_address, direction=direction, threads=threadlimit, trackBEP20=trackBEP20, trackNative=trackNative, followNative=followNative)
    ''


def follow_tokenflow_by_tx(transaction_hash: TXHASH, direction: Direction, trackBEP20: bool, trackNative: bool, followNative: bool):
    @APIretry
    def get_receipt_from_tx(transaction_hash):
        check_API_limit()
        return bsc.get_proxy_transaction_receipt(transaction_hash)

    with BscScan(api_key=api_key, asynchronous=False) as bsc:

        receipt = get_receipt_from_tx(transaction_hash)
        contract_address = ADDRESS(receipt['logs'][0]['address'])

    is_token = check_if_token(contract_address)

    if is_token:

        recipients = []
        for log in receipt['logs']:
            tmp = log['topics'][2]
            recipients.append(ADDRESS(tmp[:2] + tmp[-40:]))

        recursive_search_by_address_and_contract(
            addresses=recipients, contract=contract_address, threads=threadlimit, direction=direction, trackBEP20=trackBEP20, trackNative=trackNative, followNative=followNative)
    ''


def follow_tokenflow(by: SearchType, direction: Direction, trackBEP20: bool, trackNative: bool, followNative: bool, contract=None, tx=None, addresses=None):
    start = time.asctime()
    print('Start: {} '.format(start))

    if by == SearchType.TX and tx and direction:
        tx = TXHASH(tx)
        follow_tokenflow_by_tx(transaction_hash=tx, direction=direction,
                               trackBEP20=trackBEP20, trackNative=trackNative, followNative=followNative)

    if by == SearchType.ADDR and addresses and contract and direction:
        contract = ADDRESS(contract)
        follow_tokenflow_by_address(
            addresses=addresses, contract_address=contract, direction=direction, trackBEP20=trackBEP20, trackNative=trackNative, followNative=followNative)

    end = time.asctime()
    print('End: {}'.format(end))
    ''

# endregion


def load_all_db():
    transactionDB_BEP20.load()
    transactionDB_NATIVE.load()
    walletDB.load()
    contractDB.load()


def clear_all_db():
    transactionDB_BEP20.clear()
    transactionDB_NATIVE.clear()
    walletDB.clear()
    contractDB.clear()


def save_all_db():
    transactionDB_BEP20.save(indent=0)
    transactionDB_NATIVE.save(indent=0)
    walletDB.save(indent=0)
    contractDB.save(indent=0)


# region SETUP
w3 = Web3()

transactionDB_BEP20 = dbj(os.path.join(
    '.', 'json_db', 'transactionDB_BEP20.json'), autosave=False)
transactionDB_NATIVE = dbj(os.path.join(
    '.', 'json_db', 'transactionDB_NATIVE.json'), autosave=False)
walletDB = dbj(os.path.join(
    '.', 'json_db', 'walletDB.json'), autosave=False)
contractDB = dbj(os.path.join(
    '.', 'json_db', 'contractDB.json'), autosave=False)

api_key = str()

donotfollow = set()

threadlimit = int()
crawler_queue = queue.Queue()
crawler_threads = []

if __name__ == "__main__":
    ''

# endregion
