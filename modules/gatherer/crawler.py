import _thread as thread
from multiprocessing import Lock
import queue
import time
import os
import logging
import inspect

import modules.gatherer.api_functions as api
from bscscan import BscScan
from modules.classes import (Address, Direction, Filter, SearchConfig,
                             TrackConfig, ContractType)
import modules.gatherer.database as gdb

# region SETUP
_address_queue = queue.Queue()
_api_queue = queue.Queue()
_processing_queue = queue.Queue()
_crawler_threads = []

api_key = os.getenv('API_KEY') or ''
logging.debug(f'api_key = {api_key}')
api_threads = int(_apit) if (_apit := os.getenv('API_THREADS')) else 1
logging.debug(f'api_threads = {api_threads}')
processing_threads = int(_prott) if (
    _prott := os.getenv('CRAWLER_THREADS')) else 1
logging.debug(f'processing_threads = {processing_threads}')
thread_timeout = int(_tto) if (_tto := os.getenv(
    'THREAD_TIMEOUT')) and _tto != '0' else None
logging.debug(f'thread_timeout = {thread_timeout}')
donotfollow = set()
# endregion


def start_crawler_workers(addresses: list, options: SearchConfig):
    with BscScan(api_key=api_key, asynchronous=False) as bsc:  # type: ignore
        for address in addresses:
            if not isinstance(address, Address):
                address = Address(address)
            _address_queue.put(address)

        logging.info('Starting address filter thread')
        thread.start_new_thread(_filter_wallets, ("F0", thread_timeout))

        logging.info(f'Starting {api_threads} transaction gatherer thread(s)')
        for n in range(api_threads):
            t = thread.start_new_thread(
                _retrieve_transactions, (bsc, options, f'G{n}', thread_timeout))
            _crawler_threads.append(t)

        logging.info(
            f'Starting {processing_threads} transaction processing thread(s)')
        for n in range(processing_threads):
            t = thread.start_new_thread(
                _process_transactions, (options, f'P{n}', thread_timeout))
            _crawler_threads.append(t)

        # TODO: create a lock every X minutes to prevent new entries to get pulled from queue
        # TODO: while lock is active save (pickle) state of all queues and db's as snapshot
        # TODO: finally resume work
        time.sleep(10)
        while (_address_queue.qsize() + _api_queue.qsize() + _processing_queue.qsize()) > 0:
            time.sleep(1)

        logging.info(
            'Gathering missing native transactions for classification')
        _get_missing_normal_transactions(bsc)


def _filter_wallets(ThreadName='', thread_timeout=None):
    gdb.crawldb.clear()

    while True:
        try:
            address = _address_queue.get(block=True, timeout=thread_timeout)
            logging.info(
                f'wfilter {ThreadName}: got task {address} from queue')
        except queue.Empty as e:
            raise SystemExit from e
        else:
            if not gdb.crawldb.exists(address):
                gdb.crawldb.insert({'checked': True}, address)
                _api_queue.put(address)
                logging.info(
                    f'wfilter {ThreadName}: task {address} not in database, added to queue')

            _address_queue.task_done()
            logging.info(
                f'wfilter {ThreadName}: finished task {address}')


# TODO: change flow to request native transactions per wallet even with TrackConfig set to BEP20 to prevent excessive post-fetching
def _retrieve_transactions(bsc: BscScan, options: SearchConfig, ThreadName='', thread_timeout=None):
    # TODO: add bep721
    while True:
        try:
            address = _api_queue.get(block=True, timeout=thread_timeout)
            logging.info(
                f'gatherer {ThreadName}: got task {address} from queue')
        except queue.Empty as e:
            _crawler_threads.remove(thread.get_ident())
            raise SystemExit from e
        else:
            is_contract = _identify_contract(bsc, address)
            is_indb = gdb.walletDB.exists(address)
            if is_indb == is_contract == False:
                if options.trackConfig in {TrackConfig.ALL, TrackConfig.BEP20}:
                    bep = api.get_bep_tx(
                        bsc, address, options)
                else:
                    bep = []
                if options.trackConfig in {TrackConfig.ALL, TrackConfig.NATIVE}:
                    nat = api.get_nat_tx(
                        bsc, address, options)
                else:
                    nat = []
                workload = {'address': address,
                            'bep20': bep.copy(),
                            'native': nat.copy()}
                _processing_queue.put(workload)

            _api_queue.task_done()
            logging.info(
                f'gatherer {ThreadName}: finished task {address}')


def _process_transactions(options: SearchConfig, ThreadName='', thread_timeout=None):
    # TODO: add bep721
    while True:
        try:
            workload = _processing_queue.get(
                block=True, timeout=thread_timeout)
            logging.info(
                f'processor {ThreadName}: got task {workload["address"]} from queue')
        except queue.Empty as e:
            _crawler_threads.remove(thread.get_ident())
            raise SystemExit from e
        else:
            address = workload.get('address')
            txNAT = workload.get('native')
            txBEP = workload.get('bep20')

            if not gdb.walletDB.exists(address):
                logging.info(
                    f'processor {ThreadName}: processing transactions from {address}')
                logging.info(
                    f'processor {ThreadName}: task {workload.get("address")} consists of {len(txNAT)} native and {len(txBEP)} BEP20 transactions')
                outgoing = set()
                incoming = set()
                bep = {'in': set(), 'out': set(), 'txid': set()}
                nat = {'in': set(), 'out': set(), 'txid': set()}

                if options.trackConfig in {TrackConfig.BEP20, TrackConfig.ALL}:
                    bep = _filter_transactions(
                        options, address, TrackConfig.BEP20, txBEP)
                    outgoing.update(bep['out'])
                    incoming.update(bep['in'])

                if options.trackConfig in {TrackConfig.NATIVE, TrackConfig.ALL}:
                    nat = _filter_transactions(
                        options, address, TrackConfig.NATIVE, txNAT)
                    outgoing.update(nat['out'])
                    incoming.update(nat['in'])

                gdb.wallet_insert({'address': address,
                                  'txBEP': list(bep['txid']),
                                   'txNAT': list(nat['txid']),
                                   'child': list(outgoing),
                                   'parent': list(incoming)})

            _processing_queue.task_done()
            logging.info(
                f'processor {ThreadName}: finished task {address}')


# TODO: change flow to save native transactions per wallet even with TrackConfig set to BEP20 to prevent excessive post-fetching
# ! Native TX need to be fetched and put into DB, not!!! into address queue when set to BEP20
# ! Decide wether to discard BEP20 when set to NATIVE
def _filter_transactions(options: SearchConfig, address, type, transactions):
    outgoing = set()
    incoming = set()
    tx_coll = set()

    logging.info(
        f'txfilter: filtering {len(transactions)} transactions from {address}')

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

        # TODO: change to only "NATIVE TRANSFERS" and treat filterby as a list
        if(Filter.Contract_and_NativeTransfers in options.filterBy
           and options.contractFilter not in {tx['contractAddress'], tx['to']}
           and options.contractFilter[2:] not in tx['input']
                and tx['input'] != '0x'):
            continue

        # TODO: add bep721
        if type == TrackConfig.NATIVE:
            gdb.native_insert(tx)
        elif type == TrackConfig.BEP20:
            gdb.bep20_insert(tx)
        elif type == TrackConfig.NFT:
            raise NotImplementedError

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

    logging.info(
        f'txfilter: finished. {len(tx_coll)} transactions from {address} remaining after filtering')
    return {'in': incoming, 'out': outgoing, 'txid': tx_coll}


def _get_missing_normal_transactions(bsc):
    natTX = set()
    bepTX = set()

    wallets = gdb.walletDB.getall()

    if not wallets:
        return

    for wallet in wallets:
        natTX.update(wallet['txNAT'])
        bepTX.update(wallet['txBEP'])

    missing_natTX = bepTX - natTX

    number_missing = len(missing_natTX)
    number_got = 0

    logging.info(f'fetching {number_missing} missing native transactions')
    for hash in missing_natTX:
        tx = api.get_normal_transaction_by_hash(bsc, hash)
        status = api.get_tx_status(bsc, hash)

        gdb.native_insert({
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
            "txreceipt_status": status['status'],
            "input": tx['input'],
            "contractAddress": "",
            "cumulativeGasUsed": "",
            "gasUsed": "",
            "confirmations": ""
        })

        number_got += 1
        if not number_got % 10:
            logging.info(
                f'fetched {number_got}/{number_missing} missing native transactions')


def _identify_contract(bsc, address):
    is_contract = False

    if not gdb.contractDB.exists(address):
        bytecode = api.get_bytecode(bsc, address)
        if bytecode != '0x':
            circulating = api.get_circulating_supply(bsc, address)
            source = api.get_source(bsc, address)
            if circulating > 0:
                if beptx := api.get_first_bep20_transaction(bsc, address):
                    logging.info(
                        f'contract identification: categorized {address} AS TOKEN')
                    _save_contract_information(
                        address, beptx, source, bytecode, ContractType.TOKEN)
                elif nfttx := api.get_first_bep721_transaction(bsc, address):
                    logging.info(
                        f'contract identification: categorized {address} AS NFT')
                    _save_contract_information(
                        address, nfttx, source, bytecode, ContractType.NFT)
                else:
                    logging.warning(
                        f'contract identification: categorization FAILED {address} due to CIRCULATING SUPPLY WITHOUT TRANSFER')
                    _save_contract_information(
                        address, [None], source, bytecode, None)
            else:
                logging.info(
                    f'contract identification: categorized {address} AS CONTRACT')
                nattx = api.get_first_native_transaction(bsc, address)
                _save_contract_information(
                    address, nattx, source, bytecode, ContractType.CONTRACT)
            is_contract = True
        else:
            ''
            logging.info(f'categorized {address} AS WALLET')
    else:
        is_contract = True

    return is_contract


def _save_contract_information(address, first_tx, source, bytecode, typedef):
    if typedef == ContractType.TOKEN:
        id = ''
        name = first_tx[0]['tokenName']
        symbol = first_tx[0]['tokenSymbol']
        decimals = first_tx[0]['tokenDecimal']
        ctype = 'token'
    elif typedef == ContractType.NFT:
        id = first_tx[0]['tokenID']
        name = first_tx[0]['tokenName']
        symbol = first_tx[0]['tokenSymbol']
        decimals = first_tx[0]['tokenDecimal']
        ctype = 'nft'
    elif typedef == ContractType.CONTRACT:
        name = source[0]['ContractName']
        symbol = decimals = id = 'None'
        ctype = 'contract'
    else:
        name = symbol = decimals = id = 'UNIDENTIFIED'
        ctype = 'UNIDENTIFIED_CONTRACT'

    entry = {'type': ctype,
             'contractAddress': address,
             'tokenID': id,
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

    gdb.contract_insert(entry)
