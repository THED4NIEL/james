import _thread as thread
import queue
import time

import modules.gatherer.api_functions as api
import modules.logging as logger
from bscscan import BscScan
from modules.classes import (ADDRESS, Direction, Filter, SearchOptions,
                             TrackConfig)
import modules.gatherer.database as db

# region SETUP
_address_queue = queue.Queue()
_api_queue = queue.Queue()
_processing_queue = queue.Queue()
_crawler_threads = []

api_key = str()
api_threads = int()
processing_threads = int()
donotfollow = set()
# endregion


def start_crawler_workers(addresses: list, options: SearchOptions):
    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        for address in addresses:
            if not isinstance(address, ADDRESS):
                address = ADDRESS(address)
            _address_queue.put(address)

        thread.start_new_thread(_filter_wallets, ())

        for n in range(api_threads):
            t = thread.start_new_thread(
                _retrieve_transactions, (bsc, options, f'G{n}'))
            _crawler_threads.append(t)

        for n in range(processing_threads):
            t = thread.start_new_thread(
                _process_transactions, (options, f'P{n}'))
            _crawler_threads.append(t)

        while len(_crawler_threads) > 0:
            time.sleep(1)

        logger.info('----- FETCHING DONE, CHECKING FOR MISSING NAT TX -----')

        _get_missing_normal_transactions(bsc)


def _retrieve_transactions(bsc: BscScan, options: SearchOptions, ThreadName=''):
    while True:
        try:
            address = _api_queue.get(block=True, timeout=10)
        except queue.Empty:
            _crawler_threads.remove(thread.get_ident())
            raise SystemExit
        else:
            is_contract = _identify_contract(bsc, address)
            is_indb = db.wallet_exists(address)
            if is_indb == is_contract == False:
                logger.info(f'GATHERING    -{ThreadName}- {address}')
                if (TrackConfig.BEP20 == options.trackConfig
                        or TrackConfig.BOTH == options.trackConfig):
                    bep = api.get_bep20_transactions(
                        bsc, address, options)
                if (TrackConfig.NATIVE == options.trackConfig
                        or TrackConfig.BOTH == options.trackConfig):
                    nat = api.get_native_transactions(
                        bsc, address, options)
                workload = {'address': address,
                            'bep20': bep.copy(),
                            'native': nat.copy()}
                _processing_queue.put(workload)


def _process_transactions(options: SearchOptions, ThreadName=''):
    while True:
        try:
            workload = _processing_queue.get(block=True, timeout=10)
        except queue.Empty:
            _crawler_threads.remove(thread.get_ident())
            raise SystemExit
        else:
            address = workload.get('address')
            txNAT = workload.get('native')
            txBEP = workload.get('bep20')

            if not db.wallet_exists(address):
                logger.info(f'PROCESSING   -{ThreadName}- {address}')
                outgoing = set()
                incoming = set()
                bep = {'in': set(), 'out': set(), 'txid': set()}
                nat = {'in': set(), 'out': set(), 'txid': set()}

                if options.trackConfig in {TrackConfig.BEP20, TrackConfig.BOTH}:
                    bep = _filter_transactions(
                        options, address, TrackConfig.BEP20, txBEP)
                    outgoing.update(bep['out'])
                    incoming.update(bep['in'])

                if options.trackConfig in {TrackConfig.NATIVE, TrackConfig.BOTH}:
                    nat = _filter_transactions(
                        options, address, TrackConfig.NATIVE, txNAT)
                    outgoing.update(nat['out'])
                    incoming.update(nat['in'])

                db.wallet_insert({'address': address,
                                  'txBEP': list(bep['txid']),
                                  'txNAT': list(nat['txid']),
                                  'child': list(outgoing),
                                  'parent': list(incoming)})


def _get_missing_normal_transactions(bsc):
    natTX = set()
    bepTX = set()

    wallets = db.wallet_data('*')

    if not wallets:
        return

    for wallet in wallets:
        natTX.update(wallet['txNAT'])
        bepTX.update(wallet['txBEP'])

    missing_natTX = bepTX - natTX

    logger.info(
        f'-----  FETCHING {len(missing_natTX)}  SINGLE NATIVE TRANSACTIONS  -----')
    for hash in missing_natTX:
        tx = api.get_normal_transaction_by_hash(bsc, hash)

        db.native_insert({
            "blockNumber": int(tx['blockNumber'], base=16),
            "timeStamp": "",
            "hash": tx['hash'],
            "nonce": int(tx['nonce'], base=16),
            "blockHash": tx['blockHash'],
            "transactionIndex": int(tx['transactionIndex'], base=16),
            "from": tx['from'],
            "to": tx['to'],
            "value": int(tx['value'], base=16),
            "gas": int(tx['gas'], base=16),
            "gasPrice": int(tx['gasPrice'], base=16),
            "isError": "",
            "txreceipt_status": "",
            "input": tx['input'],
            "contractAddress": "",
            "cumulativeGasUsed": "",
            "gasUsed": "",
            "confirmations": ""
        })


def _filter_transactions(options: SearchOptions, address, type, transactions):
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
           and options.contractFilter not in {tx['contractAddress'], tx['to']}
           and options.contractFilter[2:] not in tx['input']):
            continue

        if(Filter.Contract_and_NativeTransfers in options.filterBy
           and options.contractFilter not in {tx['contractAddress'], tx['to']}
           and options.contractFilter[2:] not in tx['input']
                and tx['input'] != '0x'):
            continue

        if TrackConfig.NATIVE:
            db.native_insert(tx)
        elif TrackConfig.BEP20:
            db.bep20_insert(tx)

        if (
            options.direction in {Direction.RIGHT, Direction.ALL}
            and tx['from'] == address
            and tx['to'] != address
            and tx['to'] != ''
            and tx['to'] not in donotfollow
        ) and outgoing.isdisjoint({tx['to']}):
            _address_queue.put(tx['to'])
            outgoing.add(tx['to'])

        if (
            options.direction in {Direction.LEFT, Direction.ALL}
            and tx['to'] == address
            and tx['from'] != address
            and tx['from'] != ''
            and tx['from'] not in donotfollow
        ) and incoming.isdisjoint({tx['from']}):
            _address_queue.put(tx['from'])
            incoming.add(tx['from'])

        tx_coll.add(tx['hash'])

    return {'in': incoming, 'out': outgoing, 'txid': tx_coll}


def _filter_wallets():
    db.crawldb.clear()

    while True:
        try:
            address = _address_queue.get(block=True, timeout=30)
        except queue.Empty:
            raise SystemExit
        else:
            if not db.crawldb.exists(address):
                db.crawldb.insert({'checked': True}, address)
                _api_queue.put(address)


def _identify_contract(bsc, address):
    is_contract = False

    if not db.contract_exists(address):
        bytecode = api.get_bytecode(bsc, address)
        if bytecode != '0x':
            circulating = api.get_circulating_supply(bsc, address)
            if circulating > 0:
                logger.info(f'DETECTED     ---- {address} TOKEN')
                source = api.get_source(bsc, address)
                beptx = api.get_first_bep20_transaction(bsc, address)
                _save_contract_information(
                    address, beptx, source, bytecode, True)
            else:
                logger.info(f'DETECTED     ---- {address} CONTRACT')
                source = api.get_source(bsc, address)
                nattx = api.get_first_native_transaction(bsc, address)
                _save_contract_information(
                    address, nattx, source, bytecode, False)
            is_contract = True
        else:
            logger.info(f'DETECTED     ---- {address} WALLET')
    else:
        is_contract = True

    return is_contract


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

    db.contract_insert(entry)

