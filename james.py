import json
from json import decoder
from threading import Lock, Thread
from ratelimit import limits, sleep_and_retry
import click
import functools
import _thread as thread
import queue
import time
import numpy as npy
from bscscan import BscScan
from dbj import dbj
from timeit import default_timer as timer
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


# region addresses
#! 0x7c025200 = SWAP
swap_addresses = [
    '0x9f011e700fe3bd7004a8701a64517f2dc23f87dd',   # PCS ROUTER
    '0xc590175e458b83680867afd273527ff58f74c02b',   # METAMASK ROUTER
    '0x3790c9b5a9b9d9aa1c69140a5f01a57c9b868e1e'   # METAMASK ROUTER
]

mint_addresses = [
    '0x0000000000000000000000000000000000000000',
    '0x0000000000000000000000000000000000000001'
]

dead_addresses = [
    '0x000000000000000000000000000000000000dead'
]
# endregion


def parse_token_information_from_tx(data):
    if contract_db.exists(data[0]['contractAddress']):
        'already in database'
    else:
        ca = data[0]['contractAddress']
        name = data[0]['tokenName']
        sym = data[0]['tokenSymbol']
        dec = data[0]['tokenDecimal']
        token = {'contract': ca, 'name': name, 'symbol': sym, 'decimals': dec}
        contract_db.insert(token, ca)
        ''


def recursive_search_by_address_and_contract(addresses=[], contract='', direction='', threads=1):

    # TODO: check, if addresses in a queue are better to handle
    #! move queue into main program, allows for-address-loop to be in iter func, reducing complexity in retrieve func
    #! possibly no recursive structure needed with queue: <repeat iter next wallet -> add queue>
    #safeprint = thread.allocate_lock()

    for address in addresses:
        crawler_queue.put(address)

    for i in range(threads):
        t = thread.start_new_thread(
            retrieve_transactions_by_address_and_contract, (contract, direction))
        crawler_threads.append(t)

    while len(crawler_threads) > 0:
        pass

    ''


def retrieve_transactions_by_address_and_contract(contract='', direction=""):
    # TODO: determine if processing and API access should be separated further for better performance
    #! currently the thread provides work for itself. maybe another model will be more useful
    # ? saparate workers for: getting txdata; processing txdata; assembling walletdata
    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        outgoing_wallets = []
        incoming_wallets = []
        while True:
            try:
                address = crawler_queue.get(block=True, timeout=10)
                outgoing_wallets.clear()
                incoming_wallets.clear()

                exists = wallet_db.exists(address)
                if not exists:
                    tx_id_collection = []
                    transactions = []
                    page = 1
                    number_of_records = 10000  # max
                    sort = 'asc'

                    # TODO: find a way to get above a 20k limit
                    #! startblk = get_bep20_token_transfer_events_by_address_and_contract_paginated(contract, address, page, 1, 'asc)
                    #! endblk = get_bep20_token_transfer_events_by_address_and_contract_paginated(contract, address, page, 1, 'desc)
                    #! use creation block of token as starting point, get_proxy_block_by_number from there by blocksize +1
                    #! use latest tx block as end point
                    #! store whole blocks in db and filter/dbfind by contract, then dbgetmany
                    while True:

                        queryresult = {}
                        exceptions = []
                        while len(exceptions) < 3:
                            try:
                                check_API_limit()
                                queryresult = bsc.get_bep20_token_transfer_events_by_address_and_contract_paginated(
                                    contract, address, page, number_of_records, sort)
                            except Exception as e:
                                exceptions.append(e)
                                continue
                            break
                        if not queryresult:
                            raise exceptions.pop()
                        else:
                            exceptions.clear()

                        transactions.extend(queryresult)
                        if len(queryresult) < number_of_records or len(transactions) == 20000:
                            break
                        if len(transactions) == 10000:
                            # * limit of 10k can be circumvented by changing sort order (max 20k)
                            sort = 'desc'
                            page = 1
                        else:
                            page += 1

                    while len(exceptions) < 3:
                        try:
                            check_API_limit()
                            balance = bsc.get_acc_balance_by_token_contract_address(
                                contract, address)
                        except Exception as e:
                            exceptions.append(e)
                            continue
                        break
                    if not balance:
                        raise exceptions.pop()
                    else:
                        exceptions.clear()

                    for transaction in transactions:
                        entry = extract_transaction_data_from_transfer_event(
                            contract, transaction)
                        name = create_checksum(entry)

                        if not tx_db.exists(name):
                            tx_db.insert(entry, name)

                        if entry['sender'] == address and entry['recipient'] != address:
                            if entry['recipient'] not in swap_addresses:
                                outgoing_wallets.append(entry['recipient'])
                        if entry['sender'] != address and entry['recipient'] == address:
                            if entry['sender'] not in swap_addresses:
                                incoming_wallets.append(entry['sender'])

                        tx_id_collection.append(name)

                    wallet = {'address': address, 'balance': balance,
                              'tx_ids': tx_id_collection}
                    wallet_db.insert(wallet, address)

            except queue.Empty:
                crawler_threads.remove(thread.get_ident())
                raise SystemExit
            else:
                if direction == '<':
                    for result in incoming_wallets:
                        crawler_queue.put(result)
                if direction == '>':
                    for result in outgoing_wallets:
                        crawler_queue.put(result)
                if direction == '<>' or direction == '':
                    # TODO: find a better way to handle this
                    all_wallets = incoming_wallets
                    all_wallets.extend(outgoing_wallets)
                    for result in all_wallets:
                        crawler_queue.put(result)


def extract_transaction_data_from_transfer_event(contract='', transaction=''):
    tx = transaction['hash']
    decimals = int(transaction['tokenDecimal'])
    amt = int(transaction['value'], base=10) / pow(10, decimals)
    sender = transaction['from']
    recipient = transaction['to']
    block = int(transaction['blockNumber'])
    entry = {'txhash': tx, 'sender': sender, 'recipient': recipient,
             'amount': amt, 'block': block, 'contract': contract}
    return entry


def follow_tokenflow_by_address(address='', contract_address='', direction=''):
    if not isinstance(address, list):
        address = [address]

    recursive_search_by_address_and_contract(
        address, contract_address, direction)
    ''


def follow_tokenflow_by_tx(transaction_hash='', direction=''):
    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        check_API_limit()
        receipt = bsc.get_proxy_transaction_receipt(transaction_hash)
        contract_address = receipt['logs'][0]['address']

        if not contract_db.find('contract == "' + str(contract_address) + '"'):
            check_API_limit()
            data = bsc.get_bep20_token_transfer_events_by_contract_address_paginated(
                contract_address, 1, 1, 'asc')
            parse_token_information_from_tx(data)

    tx = extract_transaction_data_from_receipt(receipt, contract_address)
    addresses = (item['recipient'] for item in tx)

    recursive_search_by_address_and_contract(
        addresses, contract_address, direction)
    ''


def follow_tokenflow(by='', contract_address='', tx='', address='', direction=''):
    start = time.asctime()
    print('Start: {} '.format(start))

    if by == 'tx' and tx and direction:
        follow_tokenflow_by_tx(transaction_hash=tx, direction=direction)
        ''
    if by == 'address' and address and contract_address and direction:
        follow_tokenflow_by_address(
            address=address, contract_address=contract_address, direction=direction)
        ''
    end = time.asctime()
    print('End: {}'.format(end))
    ''


def extract_transaction_data_from_receipt(receipt={}, contract=''):
    transactions = []
    for log in receipt['logs']:
        tx = log['transactionHash']

        decimals = int(contract_db.get(contract).get('decimals'))
        amt = int(log['data'], base=16) / pow(10, decimals)

        tmp = log['topics'][1]
        sender = tmp[0:2] + tmp[-40:]

        tmp = log['topics'][2]
        recipient = tmp[0:2] + tmp[-40:]

        block = int(log['blockNumber'], base=16)
        entry = {'txhash': tx, 'sender': sender, 'recipient': recipient,
                 'amount': amt, 'block': block, 'contract': contract}

        name = create_checksum(entry)
        if not tx_db.exists(name):
            tx_db.insert(entry, name)
            transactions.append(entry)
    return transactions


def create_checksum(entry):
    checksum = functools.reduce(
        lambda x, y: x ^ y, [hash(z) for z in entry.items()])
    hex_checksum = str(hex(checksum & 0xffffffff))
    return hex_checksum


def clear_database():
    tx_db.clear()
    wallet_db.clear()


if __name__ == "__main__":
    with open('api_key.txt', 'r') as file:
        api_key = file.read()

    tx_db = dbj('.\\json_db\\tx_db.json', autosave=False)
    wallet_db = dbj('.\\json_db\\wallet_db.json', autosave=False)
    contract_db = dbj('.\\json_db\\contract_db.json', autosave=True)

    tx_db.load()
    wallet_db.load()
    contract_db.load()

    clear_database()

    crawler_queue = queue.Queue()
    api_calls_queue = queue.Queue()
    crawler_threads = []
    #db_lock = Lock()

    # james()
    follow_tokenflow(
        by='address', address='0x6c6c5a6bcc01200a09e0666d92a63e1df4218a5b', contract_address='0x09e2b83fe5485a7c8beaa5dffd1d324a2b2d5c13', direction='>')

    tx_db.save()
    wallet_db.save()
    ''


'YEET'
