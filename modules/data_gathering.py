import _thread as thread
import os
import queue
import time
from enum import Enum, auto
from functools import lru_cache
from icecream import ic
from bscscan import BscScan
from dbj import dbj
from ratelimit import limits, sleep_and_retry


# region ICECREAM messages
def warn(warn: str):
    _log_queue.put('W> ' + warn)


def info(info: str):
    _log_queue.put('I> ' + info)


def err(err: str):
    _log_queue.put('E> ' + err)


# endregion

# region API CALL LIMITER
APICALLS = 1
RATE_LIMIT = 0.225


@sleep_and_retry
@limits(calls=APICALLS, period=RATE_LIMIT)
def _check_API_limit():
    # Solution by Kjetil Svenheim - https://stackoverflow.com/questions/40748687/python-api-rate-limiting-how-to-limit-api-calls-globally
    return True
# endregion

# region CLASS DEFINITIONS


class Direction(Enum):
    LEFT = auto()
    RIGHT = auto()
    ALL = auto()


class SearchType(Enum):
    TX = auto()
    ADDR = auto()


class Filter(Enum):
    Contract = auto()
    Contract_and_NativeTransfers = auto()
    TimeStamp = auto()
    Blocks = auto()
    NONE = auto()


class TrackConfig(Enum):
    BEP20 = auto()
    NATIVE = auto()
    BOTH = auto()


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


class SearchOptions():
    def __init__(self, direction: Direction, filterBy=[Filter.NONE], trackConfig=TrackConfig.BOTH, contractFilter='', startBlock=0, endBlock=9999999999, startTimestamp=0, endTimestamp=2147483647):
        self.direction = direction
        self.filterBy = (filterBy
                         if isinstance(filterBy, list)
                         else [filterBy])
        self.trackConfig = trackConfig
        self.contractFilter = (ADDRESS(contractFilter)
                               if contractFilter != ''
                               else '')
        self.startBlock = startBlock
        self.endBlock = endBlock
        self.startTimestamp = startTimestamp
        self.endTimestamp = endTimestamp


# endregion

# region API retry wrapper for connection timeout

def APIwrapper(func):
    def wrap(*args, **kwargs):
        timeouts = 0
        while True:
            try:
                result = func(*args, **kwargs)
            except (ConnectionError, TimeoutError, ConnectionAbortedError, ConnectionRefusedError, ConnectionResetError) as e:
                timeouts += 1
                if timeouts == 5:
                    raise e
                else:
                    continue
            except AssertionError as a:
                if a.args[0] == 'Max rate limit reached -- NOTOK':
                    continue
                elif a.args[0] == '[] -- No transactions found':
                    result = []
                    break
                else:
                    raise a
            else:
                break
        return result
    return wrap
# endregion

# region CRAWLER FUNCTIONS


def start_crawler_workers(addresses: list, options: SearchOptions):
    for address in addresses:
        if not isinstance(address, ADDRESS):
            address = ADDRESS(address)
        _api_queue.put(address)

    crawldb.clear()

    for _ in range(processing_threads):
        t = thread.start_new_thread(
            _process_transactions, (options, ''))
        _crawler_threads.append(t)

    for _ in range(api_threads):
        t = thread.start_new_thread(
            _retrieve_transactions, (options, ''))
        _crawler_threads.append(t)

    while len(_crawler_threads) > 0:
        ic(_log_queue.get())
        time.sleep(0.1)


def _identify_contract(bsc, address):
    is_contract = False
    if not contractDB.exists(address):
        circulating = _get_circulating_supply(bsc, address)
        source = _get_source(bsc, address)
        if circulating > 0:
            info(f'DETECTED     --- {address} TOKEN')
            is_contract = True
            bytecode = _get_bytecode(bsc, address)
            beptx = _get_first_bep20_transaction(bsc, address)
            _save_contract_information(address,
                                       beptx, source, bytecode, True)
        elif source[0]['CompilerVersion'] != '':
            info(f'DETECTED     --- {address} CONTRACT')
            is_contract = True
            bytecode = _get_bytecode(bsc, address)
            nattx = _get_first_native_transaction(bsc, address)
            _save_contract_information(address,
                                       nattx, source, bytecode, False)
        else:
            info(f'DETECTED     --- {address} WALLET')
            is_contract = False
    else:
        is_contract = True
    return is_contract


@APIwrapper
def _get_circulating_supply(bsc, address):
    _check_API_limit()
    return int(bsc.get_circulating_supply_by_contract_address(
        address))


@APIwrapper
def _get_source(bsc, address):
    _check_API_limit()
    return bsc.get_contract_source_code(address)


@APIwrapper
def _get_bytecode(bsc, address):
    _check_API_limit()
    return bsc.get_proxy_code_at(address)


@APIwrapper
def _get_first_bep20_transaction(bsc, address):
    _check_API_limit()
    return bsc.get_bep20_token_transfer_events_by_contract_address_paginated(
        contract_address=address, page=1, offset=1, sort='asc')


@APIwrapper
def _get_first_native_transaction(bsc, address):
    _check_API_limit()
    return bsc.get_normal_txs_by_address_paginated(
        address=address, page=1, offset=1, startblock=0, endblock=999999999, sort='asc')


def _save_contract_information(address, first_tx, source, bytecode, isToken):
    if isToken == True:
        name = first_tx[0]['tokenName']
        symbol = first_tx[0]['tokenSymbol']
        decimals = first_tx[0]['tokenDecimal']
        ctype = 'token'
    else:
        name = symbol = decimals = ''
        ctype = 'contract'
    entry = {'type': ctype,
             'contractAddress': address,
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


@APIwrapper
def _get_bep20_transactions(bsc, address, options: SearchOptions):
    # TODO: find a way to get above a 20k transaction limit
    #! startblk = get_bep20_token_transfer_events_by_address_and_contract_paginated(contract, address, page, 1, 'asc)
    #! endblk = get_bep20_token_transfer_events_by_address_and_contract_paginated(contract, address, page, 1, 'desc)
    #! use creation block of token as starting point, get_proxy_block_by_number from there by blocksize +1
    #! use latest tx block as end point
    #! store whole blocks in db and filter/dbfind by contract, then dbgetmany
    page = 1
    sort = 'asc'
    number_of_records = 10000  # max
    bep20_transactions = []
    while True:
        bep20_queryresult = []
        if (Filter.Contract in options.filterBy
                or Filter.Contract_and_NativeTransfers in options.filterBy):
            _check_API_limit()
            bep20_queryresult = bsc.get_bep20_token_transfer_events_by_address_and_contract_paginated(
                contract_address=options.contractFilter, address=address, page=page, offset=number_of_records, sort=sort)
        elif(Filter.Blocks in options.filterBy):
            _check_API_limit()
            bep20_queryresult = bsc.get_bep20_token_transfer_events_by_address(
                address=address, startblock=options.startBlock, endblock=options.endBlock, sort=sort)
        else:
            _check_API_limit()
            bep20_queryresult = bsc.get_bep20_token_transfer_events_by_address(
                address=address, startblock=0, endblock=9999999999, sort=sort)
        bep20_transactions.extend(bep20_queryresult)
        if len(bep20_queryresult) < number_of_records or len(bep20_transactions) == 20000:
            break
        if len(bep20_transactions) == 10000:
            sort = 'desc'
            page = 1
        else:
            page += 1
    return bep20_transactions


@APIwrapper
def _get_native_transactions(bsc, address, options: SearchOptions):
    page = 1
    sort = 'asc'
    number_of_records = 10000  # max
    native_transactions = []
    while True:
        nat_tx_queryresult = []
        if(Filter.Blocks in options.filterBy):
            _check_API_limit()
            nat_tx_queryresult = bsc.get_normal_txs_by_address(
                address=address, startblock=options.startBlock, endblock=options.endBlock, sort=sort)
        else:
            _check_API_limit()
            nat_tx_queryresult = bsc.get_normal_txs_by_address_paginated(
                address=address, page=page, offset=number_of_records, startblock=0, endblock=9999999999, sort=sort)
        native_transactions.extend(nat_tx_queryresult)
        if len(nat_tx_queryresult) < number_of_records or len(native_transactions) == 20000:
            break
        if len(native_transactions) == 10000:
            # * limit of 10k can be circumvented by changing sort order (max 20k)
            sort = 'desc'
            page = 1
        else:
            page += 1
    return native_transactions


def _retrieve_transactions(options: SearchOptions, ThreadName=''):
    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        while True:
            try:
                address = _api_queue.get(block=True, timeout=60)
            except queue.Empty:
                _crawler_threads.remove(thread.get_ident())
                break
            else:
                if not crawldb.exists(address):
                    crawldb.insert({'checked': True}, address)
                    is_contract = _identify_contract(bsc, address)
                    is_indb = walletDB.exists(address)

                    if not is_indb and not is_contract:
                        info(f'GATHERING    --- {address}')
                        if (TrackConfig.BEP20 == options.trackConfig
                                or TrackConfig.BOTH == options.trackConfig):
                            bep = _get_bep20_transactions(bsc,
                                                          address, options)

                        if (TrackConfig.NATIVE == options.trackConfig
                                or TrackConfig.BOTH == options.trackConfig):
                            nat = _get_native_transactions(bsc,
                                                           address, options)

                        workload = {'address': address,
                                    'bep20': bep.copy(), 'native': nat.copy()}
                        _processing_queue.put(workload)
                else:
                    warn(f'SKIPPING     --- {address} DUPLICATE')


def _filter_transactions(options: SearchOptions, address, database: dbj, transactions):
    outgoing = set()
    incoming = set()
    tx_coll = set()

    for tx in transactions:
        if (Filter.TimeStamp in options.filterBy
                and not options.startTimestamp < tx['timeStamp'] > options.endTimestamp):
            continue
        if (Filter.Blocks in options.filterBy
                and not options.startBlock < tx['blockNumber'] > options.endBlock):
            continue
        if(Filter.Contract in options.filterBy
           and options.contractFilter not in {tx['contractAddress'], tx['to']}):
            continue
        if(Filter.Contract_and_NativeTransfers in options.filterBy
           and options.contractFilter not in {tx['contractAddress'], tx['to']}
                and tx['input'] != '0x'):
            continue

        id = create_checksum(tx)
        if not database.exists(id):
            database.insert(
                tx, id)
        if (options.direction in {Direction.RIGHT, Direction.ALL}
            and tx['from'] == address
            and tx['to'] != address
            and tx['to'] not in donotfollow
            ):
            outgoing.add(
                tx['to'])
        if (options.direction in {Direction.LEFT, Direction.ALL}
            and tx['to'] == address
            and tx['from'] != address
            and tx['from'] not in donotfollow
            ):
            incoming.add(
                tx['from'])
        tx_coll.add(id)
    return {'in': incoming, 'out': outgoing, 'txid': tx_coll}


def _process_transactions(options: SearchOptions, ThreadName=''):
    while True:
        try:
            workload = _processing_queue.get(block=True, timeout=60)
        except queue.Empty:
            _crawler_threads.remove(thread.get_ident())
            break
        else:
            address = workload.get('address')
            txNAT = workload.get('native')
            txBEP = workload.get('bep20')

            if not walletDB.exists(address):
                info(f'PROCESSING   --- {address}')
                outgoing = set()
                incoming = set()
                bep = {'in': set(),
                       'out': set(), 'txid': set()}
                nat = {'in': set(),
                       'out': set(), 'txid': set()}

                if options.trackConfig in {TrackConfig.BEP20, TrackConfig.BOTH}:
                    bep = _filter_transactions(
                        options, address, txDB_BEP20, txBEP)
                    outgoing.update(bep['out'])
                    incoming.update(bep['in'])

                if options.trackConfig in {TrackConfig.NATIVE, TrackConfig.BOTH}:
                    nat = _filter_transactions(
                        options, address, txDB_NATIVE, txNAT)
                    outgoing.update(nat['out'])
                    incoming.update(nat['in'])

                wallet = {'address': address, 'txBEP': list(bep['txid']),
                          'txNAT': list(nat['txid']), 'child': list(outgoing), 'parent': list(incoming)}
                walletDB.insert(wallet, address)

                if options.direction == Direction.LEFT:
                    for result in incoming:
                        _api_queue.put(result)
                if options.direction == Direction.RIGHT:
                    for result in outgoing:
                        _api_queue.put(result)
                if options.direction == Direction.ALL:
                    all_wallets = set.union(
                        incoming, outgoing)
                    for result in all_wallets:
                        _api_queue.put(result)


def create_checksum(entry: dict):
    return _calculate_checksum(frozenset(entry.items()))


@lru_cache
def _calculate_checksum(fs: frozenset):
    return str(hex(hash(fs) & 0xffffffff))


def _check_if_token(contract_address: ADDRESS):
    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        is_token = False
        contract = contractDB.get(contract_address)
        if not contract:
            circulating = _get_circulating_supply(bsc, contract_address)
            if circulating > 0:
                is_token = True

                source = _get_source(bsc, contract_address)

                bytecode = _get_bytecode(bsc, contract_address)

                txdata = _get_first_bep20_transaction(bsc, contract_address)

                _save_contract_information(
                    contract_address, txdata, source, bytecode, True)
        elif contract['type'] == 'token':
            is_token = True
        return is_token


def _follow_tokenflow_by_address(addresses: list, options: SearchOptions):
    if options.contractFilter != '':
        _check_if_token(options.contractFilter)

    if not isinstance(addresses, list):
        addresses = [ADDRESS(addresses)]
    else:
        tmp = addresses
        for addr in tmp:
            if not isinstance(addr, ADDRESS):
                addresses.remove(addr)
                addresses.append(ADDRESS(addr))

    start_crawler_workers(
        addresses=addresses, options=options)


def _follow_tokenflow_by_tx(transaction_hash: TXHASH, options: SearchOptions):
    @APIwrapper
    def get_receipt_from_tx(transaction_hash):
        _check_API_limit()
        return bsc.get_proxy_transaction_receipt(transaction_hash)

    with BscScan(api_key=api_key, asynchronous=False) as bsc:

        receipt = get_receipt_from_tx(transaction_hash)
        contract_address = ADDRESS(receipt['logs'][0]['address'])
        options.contractFilter = contract_address
        options.filterBy = [Filter.Contract_and_NativeTransfers]

    is_token = _check_if_token(contract_address)

    if is_token:

        recipients = []
        for log in receipt['logs']:
            tmp = log['topics'][2]
            recipients.append(ADDRESS(tmp[:2] + tmp[-40:]))

        start_crawler_workers(
            addresses=recipients, options=options)


def follow_tokenflow(by: SearchType, options: SearchOptions, tx=None, addresses=None):
    info(f'Start: {time.asctime()} ')
    if by == SearchType.TX and tx:
        tx = TXHASH(tx)
        _follow_tokenflow_by_tx(transaction_hash=tx, options=options)

    if by == SearchType.ADDR and addresses:
        _follow_tokenflow_by_address(
            addresses=addresses, options=options)

    info(f'End: {time.asctime()}')
# endregion


def load_all_db():
    txDB_BEP20.load()
    txDB_NATIVE.load()
    walletDB.load()
    contractDB.load()


def clear_all_db():
    txDB_BEP20.clear()
    txDB_NATIVE.clear()
    walletDB.clear()
    contractDB.clear()


def save_all_db():
    txDB_BEP20.save(indent=0)
    txDB_NATIVE.save(indent=0)
    walletDB.save(indent=0)
    contractDB.save(indent=0)


# region SETUP
crawldb = dbj('crawldb.temp')
txDB_BEP20 = dbj(os.path.join(
    '.', 'json_db', 'transactionDB_BEP20.json'), autosave=False)
txDB_NATIVE = dbj(os.path.join(
    '.', 'json_db', 'transactionDB_NATIVE.json'), autosave=False)
walletDB = dbj(os.path.join(
    '.', 'json_db', 'walletDB.json'), autosave=False)
contractDB = dbj(os.path.join(
    '.', 'json_db', 'contractDB.json'), autosave=False)

api_key = str()

donotfollow = set()

api_threads = int()
processing_threads = int()
_api_queue = queue.Queue()
_processing_queue = queue.Queue()
_crawler_threads = []

_log_queue = queue.Queue()

if __name__ == "__main__":
    ''

# endregion
