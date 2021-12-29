import json
from json import decoder
import click
import functools
import threading
import numpy as npy
from bscscan import BscScan
from dbj import dbj
from timeit import default_timer as timer
# from chatterbot import ChatBot
# from chatterbot.trainers import ChatterBotCorpusTrainer

# region temp workaround


def get_bep20_token_transfer_events_by_address(address: str, startblock: int, endblock: int, sort: str):
    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        result = {}
        except_lim = 0
        #! stop only at three exceptions in a row to compensate the breaking of the workflow by timeout
        while except_lim < 3:
            try:
                result = bsc.get_bep20_token_transfer_events_by_address(
                    address, startblock, endblock, sort)
            except Exception:
                except_lim += 1
                continue
            break
        if not result:
            raise NotImplementedError
    return result


def get_proxy_transaction_by_hash(txhash: str):
    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        result = {}
        except_lim = 0
        #! stop only at three exceptions in a row to compensate the breaking of the workflow by timeout
        while except_lim < 3:
            try:
                result = bsc.get_proxy_transaction_by_hash(txhash)
            except Exception:
                except_lim += 1
                continue
            break
        if not result:
            raise NotImplementedError
    return result


def get_proxy_transaction_receipt(txhash: str):
    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        result = {}
        except_lim = 0
        #! stop only at three exceptions in a row to compensate the breaking of the workflow by timeout
        while except_lim < 3:
            try:
                result = bsc.get_proxy_transaction_receipt(txhash)
            except Exception:
                except_lim += 1
                continue
            break
        if not result:
            raise NotImplementedError
    return result


def get_bep20_token_transfer_events_by_address_and_contract_paginated(contract_address: str, address: str, page: int, offset: int, sort: str):
    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        result = {}
        except_lim = 0
        #! stop only at three exceptions in a row to compensate the breaking of the workflow by timeout
        while except_lim < 3:
            try:
                result = bsc.get_bep20_token_transfer_events_by_address_and_contract_paginated(
                    contract_address, address, page, offset, sort)
            except Exception:
                except_lim += 1
                continue
            break
        if not result:
            raise NotImplementedError
    return result


def get_bep20_token_transfer_events_by_contract_address_paginated(contract_address: str, page: int, offset: int, sort: str):
    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        result = {}
        except_lim = 0
        #! stop only at three exceptions in a row to compensate the breaking of the workflow by timeout
        while except_lim < 3:
            try:
                result = bsc.get_bep20_token_transfer_events_by_contract_address_paginated(
                    contract_address, page, offset, sort)
            except Exception:
                except_lim += 1
                continue
            break
        if not result:
            raise NotImplementedError
    return result


def get_proxy_block_number():
    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        result = {}
        except_lim = 0
        #! stop only at three exceptions in a row to compensate the breaking of the workflow by timeout
        while except_lim < 3:
            try:
                result = bsc.get_proxy_block_number()
            except Exception:
                except_lim += 1
                continue
            break
        if not result:
            raise NotImplementedError
    return result


def get_acc_balance_by_token_contract_address(contract_address: str, address: str):
    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        result = {}
        except_lim = 0
        #! stop only at three exceptions in a row to compensate the breaking of the workflow by timeout
        while except_lim < 3:
            try:
                result = bsc.get_acc_balance_by_token_contract_address(
                    contract_address, address)
            except Exception:
                except_lim += 1
                continue
            break
        if not result:
            raise NotImplementedError
    return result

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


def get_token_information(contract=''):
    if contract_db.exists(contract):
        'already in database'
    else:
        data = get_bep20_token_transfer_events_by_contract_address_paginated(
            contract, 1, 1, 'asc')
        ca = data[0]['contractAddress']
        name = data[0]['tokenName']
        sym = data[0]['tokenSymbol']
        dec = data[0]['tokenDecimal']
        token = {'contract': ca, 'name': name, 'symbol': sym, 'decimals': dec}
        contract_db.insert(token, ca)
        ''


def get_tokenbalance_by_address(addresses=[], contract=''):
    if isinstance(addresses, list):
        addresses_and_tokenbalance = []
        for address in addresses:
            balance = get_acc_balance_by_token_contract_address(
                contract, address)
            addresses_and_tokenbalance.append(
                {'address': address, 'balance': balance})
        return addresses_and_tokenbalance
    else:
        balance = get_acc_balance_by_token_contract_address(
            contract, addresses)
        return balance


def recursive_search_by_address_and_contract(addresses, contract, direction):
    # TODO: check, if addresses in a queue are better to handle
    #! move queue into main program, allows for-address-loop to be in iter func, reducing complexity in retrieve func
    #! possibly no recursive structure needed with queue: <repeat iter next wallet -> add queue>
    next = retrieve_transactions_by_address_and_contract(
        addresses, contract, direction)
    if next:
        recursive_search_by_address_and_contract(next, contract, direction)
    ''


def retrieve_transactions_by_address_and_contract(addresses, contract, direction="<>"):
    outgoing_wallets = []
    incoming_wallets = []
    for address in addresses:
        if not wallet_db.find('address == "' + str(address) + '"'):
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
                ini_len = len(transactions)
                transactions.extend(get_bep20_token_transfer_events_by_address_and_contract_paginated(
                    contract, address, page, number_of_records, sort))
                length_diff = len(transactions) - ini_len
                if length_diff < number_of_records or len(transactions) == 20000:
                    break
                if len(transactions) == 10000:
                    # * limit of 10k can be circumvented by changing sort order (max 20k)
                    sort = 'desc'
                    page = 1
                else:
                    page += 1

            balance = get_tokenbalance_by_address(address, contract)

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
        ''
    if direction == '<':
        return incoming_wallets
    if direction == '>':
        return outgoing_wallets
    if direction == '<>' or direction == '':
        # TODO: find a better way to handle this
        all_wallets = incoming_wallets
        all_wallets.extend(outgoing_wallets)
        return all_wallets
    ''


def extract_transaction_data_from_transfer_event(contract, transaction):
    tx = transaction['hash']
    decimals = int(transaction['tokenDecimal'])
    amt = int(transaction['value'], base=10) / pow(10, decimals)
    sender = transaction['from']
    recipient = transaction['to']
    block = int(transaction['blockNumber'])
    entry = {'txhash': tx, 'sender': sender, 'recipient': recipient,
             'amount': amt, 'block': block, 'contract': contract}
    return entry


def follow_tokenflow_by_tx(transaction_hash='', direction='<>'):
    receipt = get_proxy_transaction_receipt(transaction_hash)
    contract_address = receipt['logs'][0]['address']

    get_token_information(contract_address)
    tx = extract_transaction_data_from_receipt(receipt, contract_address)
    addresses = (item['recipient'] for item in tx)

    # TODO: remove if not needed
    #endblock = get_proxy_block_number()
    #startblock = int(receipt['blockNumber'], base=16)

    recursive_search_by_address_and_contract(
        addresses, contract_address, direction)
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

    # james()
    start = timer()
    follow_tokenflow_by_tx(
        '0x923c70f540c703d1e942447ca28ef56e67ca2e575b79b3f780433c9f74b965d3', '<>')
    end = timer()
    elapsed = end - start
    ''


'YEET'
