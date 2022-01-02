from eth_typing.evm import Address
from ratelimit import limits, sleep_and_retry
from web3 import Web3
import _thread as thread
import queue
import time
import os
from enum import Enum
from bscscan import BscScan
from dbj import dbj
from timeit import default_timer as timer
from requests.structures import CaseInsensitiveDict
from web3.main import Web3
# from chatterbot import ChatBot
# from chatterbot.trainers import ChatterBotCorpusTrainer

# region temp workaround
APICALLS = int()
TIMESCALE = int()


@sleep_and_retry
@limits(calls=APICALLS, period=TIMESCALE)
def check_API_limit():
    'Solution by Kjetil Svenheim - https://stackoverflow.com/questions/40748687/python-api-rate-limiting-how-to-limit-api-calls-globally'
    return
# endregion

# def james():
#   chatbot = ChatBot('JAMES')
#   trainer = ChatterBotCorpusTrainer(chatbot)
#   trainer.train('chatterbot.corpus.english')
#   chatbot.get_response('Hello, I\'m JAMES, how are you today?')


class Direction(Enum):
    LEFT = 'in'
    RIGHT = 'out'
    ALL = 'all'


class SearchType(Enum):
    TX = 'tx'
    ADDR = 'addr'


class ADDRESS():
    address = str()

    def __init__(self, address: str):
        if not isinstance(address, str):
            raise ValueError('input must be string')
        if len(address) != 42:
            raise ValueError('address length is incorrect')
        if address[0:2] != '0x':
            raise ValueError('address must start with 0x')

        self.address = address.lower()
        ''

    def __str__(self):
        return self.address

    def __repr__(self):
        return self.address

    def __eq__(self, other):
        return (self.address.casefold() == str(other).casefold())

    def __ne__(self, other):
        return not (self == other)
    ''


class TXHASH():
    transaction_hash = str()

    def __init__(self, transaction_hash: str):
        if not isinstance(transaction_hash, str):
            raise ValueError('input must be string')
        if len(transaction_hash) != 66:
            raise ValueError('transaction hash length is incorrect')
        if transaction_hash[0:2] != '0x':
            raise ValueError('transaction hash must start with 0x')

        self.transaction_hash = transaction_hash.lower()
        ''

    def __str__(self):
        return self.transaction_hash

    def __repr__(self):
        return self.transaction_hash

    def __eq__(self, other):
        return (self.transaction_hash.casefold() == str(other).casefold())

    def __ne__(self, other):
        return not (self == other)
    ''


# region addresses
swap_addresses = CaseInsensitiveDict({
    '0x9f011e700fe3bd7004a8701a64517f2dc23f87dd': 'PCS ROUTER',
    '0xc590175e458b83680867afd273527ff58f74c02b': 'METAMASK ROUTER',
    '0x3790c9b5a9b9d9aa1c69140a5f01a57c9b868e1e': 'METAMASK ROUTER',
    '0x6bBf1fa4a7eE6525F36aE8ffb6ee22DD009351E3': 'BOGGEDFINANCE ROUTER'
})

mint_addresses = CaseInsensitiveDict({
    '0x0000000000000000000000000000000000000000': 'MINT ADDRESS',
    '0x0000000000000000000000000000000000000001': 'MINT ADDRESS'
})

dead_addresses = CaseInsensitiveDict(
    {'0x000000000000000000000000000000000000dead': 'DEAD ADDRESS'})


# endregion


def recursive_search_by_address_and_contract(addresses: list, contract: ADDRESS, direction: Direction, threads: int, trackBEP20: bool, trackNative: bool, followNative: bool):
    # TODO: check, if addresses in a queue are better to handle
    #! move queue into main program, allows for-address-loop to be in iter func, reducing complexity in retrieve func
    #! possibly no recursive structure needed with queue: <repeat iter next wallet -> add queue>
    #safeprint = thread.allocate_lock()

    for address in addresses:
        if not isinstance(address, ADDRESS):
            address = ADDRESS(address)
        crawler_queue.put(address)

    for i in range(threads):
        time.sleep(0.25)
        t = thread.start_new_thread(
            retrieve_transactions_by_address_and_contract, (direction, trackBEP20, trackNative, followNative, contract))
        crawler_threads.append(t)

    while len(crawler_threads) > 0:
        time.sleep(1000)
        pass

    ''


def retrieve_transactions_by_address_and_contract(direction: Direction, trackBEP20: bool, trackNative: bool, followNative: bool, contract: ADDRESS, startblock=0, endblock=0):
    def identify_contract():
        exceptions = []
        is_contract = False

        is_token_in_db = tokenDB.exists(address)
        is_contract_in_db = contractDB.exists(address)

        if is_token_in_db == is_contract_in_db == False:
            while len(exceptions) < 3:
                try:
                    check_API_limit()
                    circulating = int(bsc.get_circulating_supply_by_contract_address(
                        address))
                    check_API_limit()
                    source = bsc.get_contract_source_code(address)
                    check_API_limit()
                    bytecode = bsc.get_proxy_code_at(address)
                    if circulating > 0:
                        # * IS A TOKEN
                        is_contract = True

                        check_API_limit()
                        data = bsc.get_bep20_token_transfer_events_by_contract_address_paginated(
                            address=address, page=1, offset=1, sort='asc')
                        name = data[0]['tokenName']
                        symbol = data[0]['tokenSymbol']
                        decimals = data[0]['tokenDecimal']

                        token = {'contract': address, 'name': name,
                                 'symbol': symbol, 'decimals': decimals, 'source': source, 'bytecode': bytecode}
                        tokenDB.insert(token, address)
                    else:
                        # * IS NO TOKEN, BUT MAY BE A CONTRACT
                        if source[0]['CompilerVersion'] != '':
                            # * CONTRACT CONFIRMED, SAVE FOR FURTHER CLASSIFICATION
                            first_tx = bsc.get_normal_txs_by_address_paginated(
                                address=address, page=0, offset=1, startblock=0, endblock=999999999, sort='asc')
                            entry = {'source': source, 'bytecode': bytecode,
                                     'first_transaction': first_tx}
                            contractDB.insert(entry, address)
                            is_contract = True
                        else:
                            # * NO SIGNS OF A CONTRACT FOUND
                            is_contract = False
                except (ConnectionError, TimeoutError) as e:
                    exceptions.append(e)
                    continue
                except AssertionError as a:
                    if a.args[0] == '[] -- No transactions found':
                        pass
                    else:
                        exceptions.append(a)
                        continue
                break
            if len(exceptions) == 3:
                raise exceptions.pop()
            else:
                exceptions.clear()
        else:
            is_contract = True
        return is_contract

    def get_and_process_bep20_transactions():
        # TODO: find a way to get above a 20k limit
        #! startblk = get_bep20_token_transfer_events_by_address_and_contract_paginated(contract, address, page, 1, 'asc)
        #! endblk = get_bep20_token_transfer_events_by_address_and_contract_paginated(contract, address, page, 1, 'desc)
        #! use creation block of token as starting point, get_proxy_block_by_number from there by blocksize +1
        #! use latest tx block as end point
        #! store whole blocks in db and filter/dbfind by contract, then dbgetmany
        page = 1
        sort = 'asc'
        number_of_records = 10000  # max
        exceptions = list()
        bep20_transactions = list()
        nonlocal highest_block, lowest_block, outgoing_wallets, incoming_wallets, bep20_tx_id_collection

        while True:
            # * Get transactions for BEP20 tokens
            bep20_queryresult = {}
            while len(exceptions) < 3:
                try:
                    if contract_provided:
                        check_API_limit()
                        bep20_queryresult = bsc.get_bep20_token_transfer_events_by_address_and_contract_paginated(
                            contract_address=contract, address=address, page=page, offset=number_of_records, sort=sort)
                    else:
                        check_API_limit()
                        bep20_queryresult = bsc.get_bep20_token_transfer_events_by_address(
                            address=address, startblock=startblock, endblock=endblock, sort=sort)
                except (ConnectionError, TimeoutError) as e:
                    exceptions.append(e)
                    continue
                except AssertionError as a:
                    if a.args[0] == '[] -- No transactions found':
                        bep20_queryresult = {}
                        break
                    else:
                        exceptions.append(a)
                        continue
                break
            if exceptions == 3:
                raise exceptions.pop()
            else:
                exceptions.clear()
            bep20_transactions.extend(bep20_queryresult)
            if len(bep20_queryresult) < number_of_records or len(bep20_transactions) == 20000:
                break
            if len(bep20_transactions) == 10000:
                # * limit of 10k can be circumvented by changing sort order (max 20k)
                sort = 'desc'
                page = 1
            else:
                page += 1
        # * process BEP20 transactions
        # * check if tx is already indexed, if not, add to db
        # * crawl each transaction for receivers or senders and add them to the queue
        if len(bep20_transactions) > 0:
            highest_block = lowest_block = int(
                bep20_transactions[0]['blockNumber'])
        for transaction in bep20_transactions:
            id = create_checksum(transaction['hash'])
            if not transactionDB_BEP20.exists(id):
                transactionDB_BEP20.insert(
                    transaction, id)
            if transaction['from'] == address and transaction['to'] != address:
                if transaction['to'] not in donotfollow:
                    outgoing_wallets.add(
                        transaction['to'])
            if transaction['from'] != address and transaction['to'] == address:
                if transaction['from'] not in donotfollow:
                    incoming_wallets.add(
                        transaction['from'])
            block = int(transaction['blockNumber'])
            if block < lowest_block:
                lowest_block = block
            if block > highest_block:
                highest_block = block
            bep20_tx_id_collection.append(id)
        ''

    def get_and_process_normal_transactions():
        page = 1
        sort = 'asc'
        number_of_records = 10000  # max
        exceptions = list()
        native_transactions = list()
        nonlocal highest_block, lowest_block, outgoing_wallets, incoming_wallets, nat_tx_id_collection

        # * Get native transactions
        while True:
            nat_tx_queryresult = {}
            while len(exceptions) < 3:
                try:
                    if contract_provided:
                        check_API_limit()
                        nat_tx_queryresult = bsc.get_normal_txs_by_address_paginated(
                            address=address, page=page, offset=number_of_records, startblock=lowest_block, endblock=highest_block, sort=sort)
                    else:
                        if startblock == endblock == 0:
                            check_API_limit()
                            nat_tx_queryresult = bsc.get_normal_txs_by_address_paginated(
                                address=address, page=page, offset=number_of_records, startblock=0, endblock=999999999, sort=sort)
                        else:
                            check_API_limit()
                            nat_tx_queryresult = bsc.get_normal_txs_by_address_paginated(
                                address=address, page=page, offset=number_of_records, startblock=startblock, endblock=endblock, sort=sort)
                except (ConnectionError, TimeoutError) as e:
                    exceptions.append(e)
                    continue
                except AssertionError as a:
                    if a.args[0] == '[] -- No transactions found':
                        nat_tx_queryresult = {}
                        break
                    else:
                        exceptions.append(a)
                        continue
                break
            if exceptions == 3:
                raise exceptions.pop()
            else:
                exceptions.clear()

            native_transactions.extend(nat_tx_queryresult)
            if len(nat_tx_queryresult) < number_of_records or len(native_transactions) == 20000:
                break
            if len(native_transactions) == 10000:
                # * limit of 10k can be circumvented by changing sort order (max 20k)
                sort = 'desc'
                page = 1
            else:
                page += 1

        # * process native transactions
        # * check if tx is already indexed, if not, add to db
        # * crawl each transaction for receivers or senders and add them to the queue

        for transaction in native_transactions:
            id = create_checksum(transaction['hash'])
            if not transactionDB_NATIVE.exists(id):
                transactionDB_NATIVE.insert(transaction, id)

            if transaction['from'] == address and transaction['to'] != address and followNative:
                if transaction['to'] not in donotfollow:
                    outgoing_wallets.add(
                        transaction['to'])
            if transaction['from'] != address and transaction['to'] == address and followNative:
                if transaction['from'] not in donotfollow:
                    incoming_wallets.add(
                        transaction['from'])

            nat_tx_id_collection.append(id)

    if trackNative == trackBEP20 == False:
        raise Exception

    if trackBEP20 == False and startblock == endblock == 0:
        raise Exception

    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        contract_provided = contract != ''
        outgoing_wallets = set()
        incoming_wallets = set()
        while True:
            try:
                address = crawler_queue.get(block=True, timeout=10)

                is_contract = identify_contract()
                is_indb = walletDB.exists(address)

                if not is_indb and not is_contract:

                    bep20_tx_id_collection = list()
                    nat_tx_id_collection = list()
                    lowest_block = int()
                    highest_block = int()

                    if trackBEP20:
                        get_and_process_bep20_transactions()
                    if trackNative:
                        get_and_process_normal_transactions()

                    wallet = {'address': address, 'bep20_tx_ids': bep20_tx_id_collection,
                              'native_tx_ids': nat_tx_id_collection, 'children': list(outgoing_wallets), 'parents': list(incoming_wallets)}
                    walletDB.insert(wallet, address)

            except queue.Empty:
                crawler_threads.remove(thread.get_ident())
                raise SystemExit
            else:
                if direction == Direction.LEFT:
                    for result in incoming_wallets:
                        crawler_queue.put(result)
                if direction == Direction.RIGHT:
                    for result in outgoing_wallets:
                        crawler_queue.put(result)
                if direction == Direction.ALL:
                    all_wallets = set.union(incoming_wallets, outgoing_wallets)
                    for result in all_wallets:
                        crawler_queue.put(result)

                outgoing_wallets.clear()
                incoming_wallets.clear()


def create_checksum(item: str):
    hex_checksum = str(hex(hash(item) & 0xffffffff))
    return hex_checksum


def check_if_token(contract_address: ADDRESS):
    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        exceptions = []
        is_token = False

        if not tokenDB.exists(contract_address):
            while len(exceptions) < 3:
                try:
                    check_API_limit()
                    circulating = int(bsc.get_circulating_supply_by_contract_address(
                        contract_address=contract_address))
                    if circulating > 0:
                        check_API_limit()
                        source = bsc.get_contract_source_code(
                            contract_addres=contract_address)
                        is_token = True
                        check_API_limit()
                        bytecode = bsc.get_proxy_code_at(
                            contract_addres=contract_address)
                        check_API_limit()
                        data = bsc.get_bep20_token_transfer_events_by_contract_address_paginated(
                            contract_addres=contract_address, page=1, offset=1, sort='asc')
                        name = data[0]['tokenName']
                        symbol = data[0]['tokenSymbol']
                        decimals = data[0]['tokenDecimal']

                        token = {'contract': contract_address, 'name': name,
                                 'symbol': symbol, 'decimals': decimals, 'source': source, 'bytecode': bytecode}
                        tokenDB.insert(token, contract_address)
                except (ConnectionError, TimeoutError) as e:
                    exceptions.append(e)
                    continue
                break
            if len(exceptions) == 3:
                raise exceptions.pop()
            else:
                exceptions.clear()
        else:
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
    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        check_API_limit()
        receipt = bsc.get_proxy_transaction_receipt(transaction_hash)
        contract_address = ADDRESS(receipt['logs'][0]['address'])

    is_token = check_if_token(contract_address)

    if is_token:

        recipients = []
        for log in receipt['logs']:
            tmp = log['topics'][2]
            recipients.append(ADDRESS(tmp[0:2] + tmp[-40:]))

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


def load_all_db():
    transactionDB_BEP20.load()
    transactionDB_NATIVE.load()
    walletDB.load()
    tokenDB.load()
    contractDB.load()


def clear_all_db():
    transactionDB_BEP20.clear()
    transactionDB_NATIVE.clear()
    walletDB.clear()
    tokenDB.clear()
    contractDB.clear()


def save_all_db():
    transactionDB_BEP20.save(indent=4)
    transactionDB_NATIVE.save(indent=4)
    walletDB.save(indent=4)
    tokenDB.save(indent=4)
    contractDB.save(indent=4)


if __name__ == "__main__":
    with open('api_key.txt', 'r') as file:
        api_key = file.read()

    w3 = Web3()

    # region DB setup
    transactionDB_BEP20 = dbj(os.path.join(
        '.', 'json_db', 'transactionDB_BEP20.json'), autosave=False)
    transactionDB_NATIVE = dbj(os.path.join(
        '.', 'json_db', ' transactionDB_NATIVE.json'), autosave=False)
    transactionDB_IDENTIFICATION = dbj(
        os.path.join('.', 'json_db', 'transactionDB_IDENTIFICATION.json'), autosave=False)
    walletDB = dbj(os.path.join(
        '.', 'json_db', 'walletDB.json'), autosave=False)
    tokenDB = dbj(os.path.join('.', 'json_db',
                  'tokenDB.json'), autosave=False)
    contractDB = dbj(os.path.join(
        '.', 'json_db', 'contractDB.json'), autosave=False)

    load_all_db()
    clear_all_db()
    # endregion

    # API limit configuration
    APICALLS = 4
    TIMESCALE = 1
    # endregion

    # region crawler configuration
    threadlimit = 1
    crawler_queue = queue.Queue()
    crawler_threads = []
    # endregion

    # region address exceptions configuration
    donotfollow = CaseInsensitiveDict({})
    donotfollow.update(dead_addresses)
    donotfollow.update(mint_addresses)
    # endregion

    # follow_tokenflow(
    #     by=SearchType.ADDR, address='0xD063cCd39Fd9AaA759b77E5EA1e212B4288C7EFE', contract='0x09e2b83fe5485a7c8beaa5dffd1d324a2b2d5c13', direction=Direction.RIGHT, trackBEP20=True, trackNative=True, followNative=True)
    # follow_tokenflow(
    #    by=SearchType.TX, tx='0x470274cac1206acd765d61850aa65987818c7092de72529b827f27167ac33993', direction=Direction.RIGHT, trackBEP20=True, trackNative=True)

    if_main_end = True


prog_end = True
