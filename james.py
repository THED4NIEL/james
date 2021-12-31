from ratelimit import limits, sleep_and_retry
import functools
import _thread as thread
import queue
import time
import re
from enum import Enum
from bscscan import BscScan
from dbj import dbj
from timeit import default_timer as timer
from requests.structures import CaseInsensitiveDict
# from chatterbot import ChatBot
# from chatterbot.trainers import ChatterBotCorpusTrainer

# region temp workaround
APICALLS = 4
TIMESCALE = 1


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


# region addresses
tx_actions = CaseInsensitiveDict({
    '0x7c025200': 'SWAP',
    '0xdc056eff': 'SWAP',
    '0x60806040': 'CONTRACT CREATION'
})

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

donotfollow = CaseInsensitiveDict({})
donotfollow.update(dead_addresses)
donotfollow.update(swap_addresses)
donotfollow.update(mint_addresses)

# endregion


def recursive_search_by_address_and_contract(addresses: list, contract: str, direction: Direction, threads: int, trackBEP20: bool, trackNative: bool, followNative: bool):
    # TODO: check, if addresses in a queue are better to handle
    #! move queue into main program, allows for-address-loop to be in iter func, reducing complexity in retrieve func
    #! possibly no recursive structure needed with queue: <repeat iter next wallet -> add queue>
    #safeprint = thread.allocate_lock()

    for address in addresses:
        crawler_queue.put(address)

    for i in range(threads):
        time.sleep(0.75)
        t = thread.start_new_thread(
            retrieve_transactions_by_address_and_contract, (direction, trackBEP20, trackNative, followNative, contract))
        crawler_threads.append(t)

    while len(crawler_threads) > 0:
        pass

    ''


def retrieve_transactions_by_address_and_contract(direction: Direction, trackBEP20: bool, trackNative: bool, followNative: bool, contract: str, startblock=0, endblock=0):
    def identify_contract():
        exceptions = []
        is_contract = False

        is_token_in_db = token_db.exists(address)
        is_contract_in_db = contract_db.exists(address)

        if is_token_in_db == is_contract_in_db == False:
            while len(exceptions) < 3:
                try:
                    check_API_limit()
                    circulating = int(bsc.get_circulating_supply_by_contract_address(
                        address))
                    check_API_limit()
                    source = bsc.get_contract_source_code(address)
                    if circulating > 0:
                        # * IS A TOKEN
                        is_contract = True
                        if source[0]['SourceCode'] != '':
                            sourcecode = source[0]['SourceCode']
                            constructor = re.search(
                                r'constructor\(\) public \{[\s\S]+?\}', sourcecode).group(0)
                            name = re.search(
                                r'name[\s*?]=[\s*?][\'\"](.*?)[\'\"];', constructor).group(1)
                            symbol = re.search(
                                r'symbol[\s*?]=[\s*?][\'\"](.*?)[\'\"];', constructor).group(1)
                            decimals = re.search(
                                r'decimal[s]?[\s*?]=[\s*?](.*?);', constructor).group(1)
                        else:
                            check_API_limit()
                            data = bsc.get_bep20_token_transfer_events_by_contract_address_paginated(
                                address, 1, 1, 'asc')
                            name = data[0]['tokenName']
                            symbol = data[0]['tokenSymbol']
                            decimals = data[0]['tokenDecimal']

                        token = {'contract': address, 'name': name,
                                 'symbol': symbol, 'decimals': decimals, 'source': source}
                        token_db.insert(token, address)
                    else:
                        # * IS NO TOKEN, BUT MAYBE A CONTRACT
                        if source[0]['CompilerVersion'] != '':
                            # * CONTRACT CONFIRMED, SAVE FOR FURTHER CLASSIFICATION
                            first_tx = bsc.get_normal_txs_by_address_paginated(
                                address, 0, 1, 0, 999999999, 'asc')
                            entry = {'source': source,
                                     'first_transaction': first_tx}
                            contract_db.insert(entry, address)
                            is_contract = True
                        else:
                            # * NO SIGNS OF A CONTRACT FOUND
                            is_contract = False
                except (ConnectionError, TimeoutError) as e:
                    exceptions.append(e)
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

        while True:
            # * Get transactions for BEP20 tokens
            bep20_queryresult = {}
            while len(exceptions) < 3:
                try:
                    if contract_provided:
                        check_API_limit()
                        bep20_queryresult = bsc.get_bep20_token_transfer_events_by_address_and_contract_paginated(
                            contract, address, page, number_of_records, sort)
                    else:
                        check_API_limit()
                        bep20_queryresult = bsc.get_bep20_token_transfer_events_by_address(
                            address, startblock, endblock, sort)
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
        nonlocal highest_block, lowest_block

        highest_block = lowest_block = int(
            bep20_transactions[0]['blockNumber'])
        for transaction in bep20_transactions:
            if not bep20_tx_db.exists(transaction['hash']):
                bep20_tx_db.insert(
                    transaction, transaction['hash'])
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
            bep20_tx_id_collection.append(
                transaction['hash'])
        ''

    def get_and_process_normal_transactions():
        page = 1
        sort = 'asc'
        number_of_records = 10000  # max
        nonlocal highest_block, lowest_block

        # * Get native transactions
        while True:
            nat_tx_queryresult = {}
            while len(exceptions) < 3:
                try:
                    if contract_provided:
                        check_API_limit()
                        nat_tx_queryresult = bsc.get_normal_txs_by_address_paginated(
                            address, page, number_of_records, lowest_block, highest_block, sort)
                    else:
                        if startblock == endblock == 0:
                            check_API_limit()
                            nat_tx_queryresult = bsc.get_normal_txs_by_address_paginated(
                                address, page, number_of_records, 0, 999999999, sort)
                        else:
                            check_API_limit()
                            nat_tx_queryresult = bsc.get_normal_txs_by_address_paginated(
                                address, page, number_of_records, startblock, endblock, sort)
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
            if len(nat_tx_queryresult) < number_of_records or len(bep20_transactions) == 20000:
                break
            if len(bep20_transactions) == 10000:
                # * limit of 10k can be circumvented by changing sort order (max 20k)
                sort = 'desc'
                page = 1
            else:
                page += 1

        # * process native transactions
        # * check if tx is already indexed, if not, add to db
        # * crawl each transaction for receivers or senders and add them to the queue
        for transaction in native_transactions:
            if not nat_tx_db.exists(transaction['hash']):
                nat_tx_db.insert(
                    transaction, transaction['hash'])

            if transaction['from'] == address and transaction['to'] != address and followNative:
                if transaction['to'] not in donotfollow:
                    outgoing_wallets.add(
                        transaction['to'])
            if transaction['from'] != address and transaction['to'] == address and followNative:
                if transaction['from'] not in donotfollow:
                    incoming_wallets.add(
                        transaction['from'])

            nat_tx_id_collection.append(transaction['hash'])

    if trackNative == trackBEP20 == False:
        raise Exception

    if trackBEP20 == False and startblock == endblock == 0:
        raise Exception

    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        contract_provided = contract != ''
        outgoing_wallets = set()
        incoming_wallets = set()
        exceptions = list()
        while True:
            try:
                address = crawler_queue.get(block=True, timeout=30)

                is_contract = identify_contract()
                is_indb = wallet_db.exists(address)

                if not is_indb and not is_contract:

                    bep20_tx_id_collection = list()
                    bep20_transactions = list()
                    nat_tx_id_collection = list()
                    native_transactions = list()
                    lowest_block = int()
                    highest_block = int()

                    if trackBEP20:
                        get_and_process_bep20_transactions()
                    if trackNative:
                        get_and_process_normal_transactions()

                    wallet = {'address': address, 'bep20_tx_ids': bep20_tx_id_collection,
                              'native_tx_ids': nat_tx_id_collection}
                    wallet_db.insert(wallet, address)

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
                    # TODO: find a better way to handle this
                    all_wallets = set.union(incoming_wallets, outgoing_wallets)
                    for result in all_wallets:
                        crawler_queue.put(result)

                outgoing_wallets.clear()
                incoming_wallets.clear()


def create_checksum(item: dict):
    checksum = functools.reduce(
        lambda x, y: x ^ y, [hash(z) for z in item.items()])
    hex_checksum = str(hex(checksum & 0xffffffff))
    return hex_checksum


def check_if_token(contract_address: str):
    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        exceptions = []
        is_token = False

        if not token_db.exists(contract_address):
            while len(exceptions) < 3:
                try:
                    check_API_limit()
                    circulating = int(bsc.get_circulating_supply_by_contract_address(
                        contract_address))
                    if circulating > 0:
                        check_API_limit()
                        source = bsc.get_contract_source_code(contract_address)
                        is_token = True
                        if source[0]['SourceCode'] != '':
                            sourcecode = source[0]['SourceCode']
                            constructor = re.search(
                                r'constructor\(\) public \{[\s\S]+?\}', sourcecode).group(0)
                            name = re.search(
                                r'name[\s*?]=[\s*?][\'\"](.*?)[\'\"];', constructor).group(1)
                            symbol = re.search(
                                r'symbol[\s*?]=[\s*?][\'\"](.*?)[\'\"];', constructor).group(1)
                            decimals = re.search(
                                r'decimal[s]?[\s*?]=[\s*?](.*?);', constructor).group(1)
                        else:
                            check_API_limit()
                            data = bsc.get_bep20_token_transfer_events_by_contract_address_paginated(
                                contract_address, 1, 1, 'asc')
                            name = data[0]['tokenName']
                            symbol = data[0]['tokenSymbol']
                            decimals = data[0]['tokenDecimal']

                        token = {'contract': contract_address, 'name': name,
                                 'symbol': symbol, 'decimals': decimals, 'source': source}
                        token_db.insert(token, contract_address)
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


def follow_tokenflow_by_address(address: list, contract_address: str, direction: Direction, trackBEP20: bool, trackNative: bool, followNative: bool):
    is_token = check_if_token(contract_address)
    if is_token:
        recursive_search_by_address_and_contract(
            addresses=address, contract=contract_address, direction=direction, threads=threadlimit, trackBEP20=trackBEP20, trackNative=trackNative, followNative=followNative)
    ''


def follow_tokenflow_by_tx(transaction_hash: str, direction: Direction, trackBEP20: bool, trackNative: bool, followNative: bool):
    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        check_API_limit()
        receipt = bsc.get_proxy_transaction_receipt(transaction_hash)
        contract_address = receipt['logs'][0]['address']

    is_token = check_if_token(contract_address)

    if is_token:

        recipients = []
        for log in receipt['logs']:
            tmp = log['topics'][2]
            recipients.append(tmp[0:2] + tmp[-40:])

        recursive_search_by_address_and_contract(
            addresses=recipients, contract=contract_address, threads=threadlimit, direction=direction, trackBEP20=trackBEP20, trackNative=trackNative, followNative=followNative)
    ''


def follow_tokenflow(by: SearchType, direction: Direction, trackBEP20: bool, trackNative: bool, followNative: bool, contract=None, tx=None, address=None):
    start = time.asctime()
    print('Start: {} '.format(start))

    if by == SearchType.TX and tx and direction:
        follow_tokenflow_by_tx(transaction_hash=tx, direction=direction,
                               trackBEP20=trackBEP20, trackNative=trackNative, followNative=followNative)
        ''
    if by == SearchType.ADDR and address and contract and direction:
        if not isinstance(address, list):
            address = [address]
        follow_tokenflow_by_address(
            address=address, contract_address=contract, direction=direction, trackBEP20=trackBEP20, trackNative=trackNative, followNative=followNative)
        ''
    end = time.asctime()
    print('End: {}'.format(end))
    ''


def load_all_db():
    bep20_tx_db.load()
    nat_tx_db.load()
    wallet_db.load()
    token_db.load()
    contract_db.load()


def clear_all_db():
    bep20_tx_db.clear()
    nat_tx_db.clear()
    wallet_db.clear()
    token_db.clear()
    contract_db.clear()


def save_all_db():
    bep20_tx_db.save(indent=4)
    nat_tx_db.save(indent=4)
    wallet_db.save(indent=4)
    token_db.save(indent=4)
    contract_db.save(indent=4)


if __name__ == "__main__":
    with open('api_key.txt', 'r') as file:
        api_key = file.read()

    bep20_tx_db = dbj('.\\json_db\\bep20_tx_db.json', autosave=False)
    nat_tx_db = dbj('.\\json_db\\nat_tx_db.json', autosave=False)
    # tx_classification = dbj(
    #    '.\\json_db\\tx_classification.json', autosave=False)
    wallet_db = dbj('.\\json_db\\wallet_db.json', autosave=False)
    token_db = dbj('.\\json_db\\token_db.json', autosave=True)
    contract_db = dbj('.\\json_db\\contract_db.json', autosave=True)

    load_all_db()
    clear_all_db()

    threadlimit = 1
    crawler_queue = queue.Queue()
    crawler_threads = []
    follow_tokenflow(
        by=SearchType.ADDR, address='0xD063cCd39Fd9AaA759b77E5EA1e212B4288C7EFE', contract='0x09e2b83fe5485a7c8beaa5dffd1d324a2b2d5c13', direction=Direction.RIGHT, trackBEP20=True, trackNative=True, followNative=False)
    # follow_tokenflow(
    #    by=SearchType.TX, tx='0x470274cac1206acd765d61850aa65987818c7092de72529b827f27167ac33993', direction=Direction.RIGHT, trackBEP20=True, trackNative=True)

    ''


'YEET'
