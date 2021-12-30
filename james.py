from ratelimit import limits, sleep_and_retry
import functools
import _thread as thread
import queue
import time
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
tx_actions = {
    '0x7c025200': "SWAP"
}

swap_addresses = {
    '0x9f011e700fe3bd7004a8701a64517f2dc23f87dd': 'PCS ROUTER',
    '0xc590175e458b83680867afd273527ff58f74c02b': 'METAMASK ROUTER',
    '0x3790c9b5a9b9d9aa1c69140a5f01a57c9b868e1e': 'METAMASK ROUTER'
}

mint_addresses = [
    '0x0000000000000000000000000000000000000000',
    '0x0000000000000000000000000000000000000001'
]

dead_addresses = [
    '0x000000000000000000000000000000000000dead'
]
# endregion


def recursive_search_by_address_and_contract(addresses=[], contract='', direction='', threads=4, trackBEP20=False, trackNative=False):
    # TODO: check, if addresses in a queue are better to handle
    #! move queue into main program, allows for-address-loop to be in iter func, reducing complexity in retrieve func
    #! possibly no recursive structure needed with queue: <repeat iter next wallet -> add queue>
    #safeprint = thread.allocate_lock()

    for address in addresses:
        crawler_queue.put(address)

    #! limit threads for testing, remove after
    threads = 1

    for i in range(threads):
        time.sleep(0.75)
        t = thread.start_new_thread(
            retrieve_transactions_by_address_and_contract, (contract, direction, trackBEP20, trackNative))
        crawler_threads.append(t)

    while len(crawler_threads) > 0:
        pass

    ''


def retrieve_transactions_by_address_and_contract(contract='', direction='', trackBEP20=True, trackNative=True, startblock=0, endblock=0):
    # TODO: determine if processing and API access should be separated further for better performance
    #! currently the thread provides work for itself. maybe another model will be more useful
    # ? saparate workers for: getting txdata; processing txdata; assembling walletdata
    if trackNative == trackBEP20 == False:
        raise Exception

    if trackBEP20 == False and startblock == endblock == 0:
        raise Exception

    contract_provided = contract != ''
    outgoing_wallets = []
    incoming_wallets = []

    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        while True:
            try:
                address = crawler_queue.get(block=True, timeout=30)
                outgoing_wallets.clear()
                incoming_wallets.clear()

                exists = wallet_db.exists(address)
                if not exists:
                    bep20_tx_id_collection = []
                    bep20_transactions = []
                    nat_tx_id_collection = []
                    native_transactions = []

                    page = 1
                    sort = 'asc'
                    number_of_records = 10000  # max

                    # TODO: find a way to get above a 20k limit
                    #! startblk = get_bep20_token_transfer_events_by_address_and_contract_paginated(contract, address, page, 1, 'asc)
                    #! endblk = get_bep20_token_transfer_events_by_address_and_contract_paginated(contract, address, page, 1, 'desc)
                    #! use creation block of token as starting point, get_proxy_block_by_number from there by blocksize +1
                    #! use latest tx block as end point
                    #! store whole blocks in db and filter/dbfind by contract, then dbgetmany
                    while trackBEP20:
                        # * Get transactions for BEP20 tokens
                        bep20_queryresult = {}
                        exceptions = []
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
                            except Exception as e:
                                exceptions.append(e)
                                continue
                            break
                        if not bep20_queryresult:
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
                    if trackBEP20:
                        highest = lowest = int(bep20_transactions[0]['block'])

                        for transaction in bep20_transactions:
                            if not bep20_tx_db.exists(transaction['txhash']):
                                bep20_tx_db.insert(
                                    transaction, transaction['txhash'])

                            if transaction['from'] == address and transaction['to'] != address:
                                if transaction['to'] not in swap_addresses:
                                    outgoing_wallets.append(
                                        transaction['to'])
                            if transaction['from'] != address and transaction['to'] == address:
                                if transaction['from'] not in swap_addresses:
                                    incoming_wallets.append(
                                        transaction['from'])

                            block = int(transaction['block'])
                            if block < lowest:
                                lowest = block
                            if block > highest:
                                highest = block

                            bep20_tx_id_collection.append(
                                transaction['txhash'])

                    page = 1
                    sort = 'asc'
                    # * Get native transactions
                    while trackNative:
                        nat_tx_queryresult = {}
                        exceptions = []
                        while len(exceptions) < 3:
                            try:
                                if contract_provided:
                                    check_API_limit()
                                    nat_tx_queryresult = bsc.get_normal_txs_by_address_paginated(
                                        '0xD063cCd39Fd9AaA759b77E5EA1e212B4288C7EFE', page, number_of_records, lowest, highest, sort)
                                else:
                                    check_API_limit()
                                    nat_tx_queryresult = bsc.get_normal_txs_by_address_paginated(
                                        '0xD063cCd39Fd9AaA759b77E5EA1e212B4288C7EFE', page, number_of_records, startblock, endblock, sort)
                            except Exception as e:
                                exceptions.append(e)
                                continue
                            break
                        if not nat_tx_queryresult:
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
                    if trackNative:
                        for transaction in native_transactions:
                            if not nat_tx_db.exists(transaction['txhash']):
                                nat_tx_db.insert(
                                    transaction, transaction['txhash'])

                            if transaction['from'] == address and transaction['to'] != address:
                                if transaction['to'] not in swap_addresses:
                                    outgoing_wallets.append(
                                        transaction['to'])
                            if transaction['from'] != address and transaction['to'] == address:
                                if transaction['from'] not in swap_addresses:
                                    incoming_wallets.append(
                                        transaction['from'])

                            nat_tx_id_collection.append(transaction['txhash'])

                    wallet = {'address': address, 'bep20_tx_ids': bep20_tx_id_collection,
                              'native_tx_ids': nat_tx_id_collection}
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


def create_checksum(entry):
    checksum = functools.reduce(
        lambda x, y: x ^ y, [hash(z) for z in entry.items()])
    hex_checksum = str(hex(checksum & 0xffffffff))
    return hex_checksum


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

        if not contract_db.exists(contract_address):
            check_API_limit()
            data = bsc.get_bep20_token_transfer_events_by_contract_address_paginated(
                contract_address, 1, 1, 'desc')
            ca = data[0]['contractAddress']
            name = data[0]['tokenName']
            sym = data[0]['tokenSymbol']
            dec = data[0]['tokenDecimal']
            token = {'contract': ca, 'name': name,
                     'symbol': sym, 'decimals': dec}
            contract_db.insert(token, ca)

    recipients = []
    for log in receipt['logs']:
        tmp = log['topics'][2]
        recipients.append(tmp[0:2] + tmp[-40:])

    recursive_search_by_address_and_contract(
        recipients, contract_address, direction)
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


if __name__ == "__main__":
    with open('api_key.txt', 'r') as file:
        api_key = file.read()

    bep20_tx_db = dbj('.\\json_db\\bep20_tx_db.json', autosave=False)
    nat_tx_db = dbj('.\\json_db\\nat_tx_db.json', autosave=False)
    tx_classification = dbj(
        '.\\json_db\\tx_classification.json', autosave=False)
    wallet_db = dbj('.\\json_db\\wallet_db.json', autosave=False)
    contract_db = dbj('.\\json_db\\contract_db.json', autosave=True)

    bep20_tx_db.load()
    nat_tx_db.load()
    wallet_db.load()
    contract_db.load()

    crawler_queue = queue.Queue()
    crawler_threads = []
    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        x = bsc.get_normal_txs_by_address_paginated(
            '0xD063cCd39Fd9AaA759b77E5EA1e212B4288C7EFE', 1, 100, 13831528, 13886631, '')
        y = bsc.get_bep20_token_transfer_events_by_address_and_contract_paginated(
            '0xD063cCd39Fd9AaA759b77E5EA1e212B4288C7EFE', 1, 100, '')
    # follow_tokenflow(
    #    by='address', address='0x6c6c5a6bcc01200a09e0666d92a63e1df4218a5b', contract_address='0x09e2b83fe5485a7c8beaa5dffd1d324a2b2d5c13', direction='>')
    # follow_tokenflow(
    #    by='tx', tx='0x470274cac1206acd765d61850aa65987818c7092de72529b827f27167ac33993', direction='>')

    ''


'YEET'