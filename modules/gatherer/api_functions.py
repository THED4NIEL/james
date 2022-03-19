import os
from modules.classes import Filter, SearchConfig
from ratelimit import limits, sleep_and_retry
import logging
import inspect

# region SETUP
APICALLS = 1
#! only applicable to multiprocessing
# api_thread_mul = int(os.getenv('API_THREADS')) if os.getenv('API_THREADS') != '' else 1
api_call_div = int(_acps) if (_acps := os.getenv('APICALLS_PER_SECOND')) else 5
RATE_LIMIT = (APICALLS/api_call_div)*0.9  # ... * api_thread_mul
# endregion


@sleep_and_retry
@limits(calls=APICALLS, period=RATE_LIMIT)
def check_API_limit():
    # Solution by Kjetil Svenheim - https://stackoverflow.com/questions/40748687/python-api-rate-limiting-how-to-limit-api-calls-globally
    return True


def retry_wrapper(func):
    def wrap(*args, **kwargs):  # sourcery skip: remove-redundant-except-handler
        timeouts = 0
        while True:
            try:
                result = func(*args, **kwargs)
            except (ConnectionError, TimeoutError, ConnectionAbortedError, ConnectionRefusedError, ConnectionResetError) as e:
                timeouts += 1
                logging.warning(f'retry_wrapper {func}: {e.args[0]}')
                if timeouts == 5:
                    raise e from e
                else:
                    continue
            except AssertionError as a:
                if a.args[0] in [
                    'Max rate limit reached -- NOTOK',
                    'None -- Query Timeout occured. Please select a smaller result dataset',
                ]:
                    logging.warning(f'retry_wrapper {func}: {a.args[0]}')
                    continue
                elif a.args[0] == '[] -- No transactions found':
                    result = []
                    break
                else:
                    raise a from a
            else:
                break
        return result
    return wrap


@retry_wrapper
def get_normal_transaction_by_hash(bsc, hash):
    check_API_limit()
    return bsc.get_proxy_transaction_by_hash(hash)


@retry_wrapper
def get_tx_status(bsc, hash):
    check_API_limit()
    return bsc.get_tx_receipt_status(hash)


@retry_wrapper
def get_circulating_supply(bsc, address):
    check_API_limit()
    return int(bsc.get_circulating_supply_by_contract_address(address))


@retry_wrapper
def get_source(bsc, address):
    check_API_limit()
    return bsc.get_contract_source_code(address)


@retry_wrapper
def get_bytecode(bsc, address):
    check_API_limit()
    return bsc.get_proxy_code_at(address)


@retry_wrapper
def get_first_bep20_transaction(bsc, address):
    check_API_limit()
    return bsc.get_bep20_token_transfer_events_by_contract_address_paginated(contract_address=address, page=1, offset=1, sort='asc')


@retry_wrapper
def get_first_bep721_transaction(bsc, address):
    check_API_limit()
    return bsc.get_bep721_token_transfer_events_by_contract_address_paginated(contract_address=address, page=1, offset=1, sort='asc')


@retry_wrapper
def get_first_native_transaction(bsc, address):
    check_API_limit()
    return bsc.get_normal_txs_by_address_paginated(address=address, page=1, offset=1, startblock=0, endblock=999999999, sort='asc')


@retry_wrapper
def get_receipt_from_tx(bsc, transaction_hash):
    check_API_limit()
    return bsc.get_proxy_transaction_receipt(txhash=transaction_hash)

# region old transaction receivers
# @retry_wrapper
# def get_bep20_transactions(bsc, address, options: SearchOptions):
#    page = 1
#    sort = 'asc'
#    number_of_records = 10000  # max
#    transactions = []
#
#    while True:
#        result = []
#
#        if (Filter.Contract in options.filterBy
#                or Filter.Contract_and_NativeTransfers in options.filterBy):
#            check_API_limit()
#            result = bsc.get_bep20_token_transfer_events_by_address_and_contract_paginated(
#                contract_address=options.contractFilter, address=address, page=page, offset=number_of_records, sort=sort)
#        elif(Filter.Blocks in options.filterBy):
#            check_API_limit()
#            result = bsc.get_bep20_token_transfer_events_by_address(
#                address=address, startblock=options.startBlock, endblock=options.endBlock, sort=sort)
#        else:
#            check_API_limit()
#            result = bsc.get_bep20_token_transfer_events_by_address(
#                address=address, startblock=0, endblock=9999999999, sort=sort)
#        transactions.extend(result)
#
#        if len(result) < number_of_records or len(transactions) == 20000:
#            break
#        if len(transactions) == 10000:
#            sort = 'desc'
#            page = 1
#        else:
#            page += 1
#
#    return transactions
#
#
# @retry_wrapper
# def get_native_transactions(bsc, address, options: SearchOptions):
#    page = 1
#    sort = 'asc'
#    number_of_records = 10000  # max
#    transactions = []
#
#    while True:
#        result = []
#
#        if(Filter.Blocks in options.filterBy):
#            check_API_limit()
#            result = bsc.get_normal_txs_by_address(
#                address=address, startblock=options.startBlock, endblock=options.endBlock, sort=sort)
#        else:
#            check_API_limit()
#            result = bsc.get_normal_txs_by_address_paginated(
#                address=address, page=page, offset=number_of_records, startblock=0, endblock=9999999999, sort=sort)
#        transactions.extend(result)
#
#        if len(result) < number_of_records or len(transactions) == 20000:
#            break
#        if len(transactions) == 10000:
#            # * limit of 10k can be circumvented by changing sort order (max 20k)
#            sort = 'desc'
#            page = 1
#        else:
#            page += 1
#
#    return transactions
#
#
# @retry_wrapper
# def get_bep721_transactions(bsc, address, options: SearchOptions):
#    page = 1
#    sort = 'asc'
#    number_of_records = 10000  # max
#    transactions = []
#
#    while True:
#        result = []
#
#        if (Filter.Contract in options.filterBy
#                or Filter.Contract_and_NativeTransfers in options.filterBy):
#            check_API_limit()
#            result = bsc.staticget_bep721_token_transfer_events_by_address_and_contract_paginated(
#                contract_address=options.contractFilter, address=address, page=page, offset=number_of_records, sort=sort)
#        elif(Filter.Blocks in options.filterBy):
#            check_API_limit()
#            result = bsc.get_bep721_token_transfer_events_by_address(
#                address=address, startblock=options.startBlock, endblock=options.endBlock, sort=sort)
#        else:
#            check_API_limit()
#            result = bsc.get_bep721_token_transfer_events_by_address(
#                address=address, startblock=0, endblock=9999999999, sort=sort)
#        transactions.extend(result)
#
#        if len(result) < number_of_records or len(transactions) == 20000:
#            break
#        if len(transactions) == 10000:
#            # * limit of 10k can be circumvented by changing sort order (max 20k)
#            sort = 'desc'
#            page = 1
#        else:
#            page += 1
#
#    return transactions
# endregion


def transaction_crawler(func):
    def wrap(bsc, address, options, number_of_records=10000, sort='asc', page=0, startblock=0, endblock=9999999999):
        transactions = []

        while True:
            result = func(bsc, address, options, number_of_records,
                          sort, page, startblock, endblock)

            if ((Filter.Contract in options.filterBy or Filter.Contract_and_NativeTransfers in options.filterBy)
                    and Filter.Blocks in options.filterBy):
                filtered = [tx for tx in result if options.contractFilter[2:] in [
                    tx['contractAddress'], tx['to'], tx['input']] or tx['input'] == "0x"]
                transactions.extend(filtered)
            else:
                transactions.extend(result)

            if len(result) == number_of_records:
                logging.info(
                    f'transaction_crawler: reached max number of transactions for {address}, changing to block-mode')
                if Filter.Blocks not in options.filterBy:
                    options.filterBy.append(Filter.Blocks)
                    options.endBlock = endblock

                options.startBlock = max(
                    (int(tx['blockNumber']) for tx in result))
                logging.info(
                    f'transaction_crawler: set start block of {address} to {options.startBlock}')
            else:
                break

        return transactions
    return wrap


@retry_wrapper
@transaction_crawler
def get_bep_tx(bsc, address, options, number_of_records=10000, sort='asc', page=0, startblock=0, endblock=9999999999):
    check_API_limit()
    if (Filter.Blocks not in options.filterBy
            and (Filter.Contract in options.filterBy or Filter.Contract_and_NativeTransfers in options.filterBy)):
        return bsc.get_bep20_token_transfer_events_by_address_and_contract_paginated(
            contract_address=options.contractFilter, address=address, page=page, offset=number_of_records, sort=sort)
    elif(Filter.Blocks in options.filterBy):
        return bsc.get_bep20_token_transfer_events_by_address(
            address=address, startblock=options.startBlock, endblock=options.endBlock, sort=sort)
    else:
        return bsc.get_bep20_token_transfer_events_by_address(
            address=address, startblock=startblock, endblock=endblock, sort=sort)


@retry_wrapper
@transaction_crawler
def get_nat_tx(bsc, address, options, number_of_records=10000, sort='asc', page=0, startblock=0, endblock=9999999999):
    check_API_limit()
    if(Filter.Blocks in options.filterBy):
        return bsc.get_normal_txs_by_address(
            address=address, startblock=options.startBlock, endblock=options.endBlock, sort=sort)
    else:
        return bsc.get_normal_txs_by_address_paginated(
            address=address, page=page, offset=number_of_records, startblock=startblock, endblock=endblock, sort=sort)


@retry_wrapper
@transaction_crawler
def get_nft_tx(bsc, address, options, number_of_records=10000, sort='asc', page=0, startblock=0, endblock=9999999999):
    check_API_limit()
    if (Filter.Blocks not in options.filterBy
            and (Filter.Contract in options.filterBy or Filter.Contract_and_NativeTransfers in options.filterBy)):
        return bsc.staticget_bep721_token_transfer_events_by_address_and_contract_paginated(
            contract_address=options.contractFilter, address=address, page=page, offset=number_of_records, sort=sort)
    elif(Filter.Blocks in options.filterBy):
        return bsc.get_bep721_token_transfer_events_by_address(
            address=address, startblock=options.startBlock, endblock=options.endBlock, sort=sort)
    else:
        return bsc.get_bep721_token_transfer_events_by_address(
            address=address, startblock=startblock, endblock=endblock, sort=sort)
