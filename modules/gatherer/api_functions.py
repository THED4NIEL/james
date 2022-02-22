import os
import modules.logging as logger
from modules.classes import Filter, SearchOptions
from ratelimit import limits, sleep_and_retry

# region SETUP
APICALLS = 1
#! only applicable to multiprocessing
# api_thread_mul = int(os.getenv('API_THREADS')) if os.getenv('API_THREADS') != '' else 1
api_call_div = int(_acps) if (_acps := os.getenv('APICALLS_PER_SECOND')) else 5
RATE_LIMIT = (APICALLS/api_call_div)  # ... * api_thread_mul
# endregion


@sleep_and_retry
@limits(calls=APICALLS, period=RATE_LIMIT)
def check_API_limit():
    # Solution by Kjetil Svenheim - https://stackoverflow.com/questions/40748687/python-api-rate-limiting-how-to-limit-api-calls-globally
    return True


def APIwrapper(func):
    def wrap(*args, **kwargs):  # sourcery skip: remove-redundant-except-handler
        timeouts = 0
        while True:
            try:
                result = func(*args, **kwargs)
            except (ConnectionError, TimeoutError, ConnectionAbortedError, ConnectionRefusedError, ConnectionResetError) as e:
                timeouts += 1
                logger.warn('APITIMEOUT   ---- Connection Error')
                if timeouts == 5:
                    raise e from e
                else:
                    continue
            except AssertionError as a:
                if a.args[0] == 'Max rate limit reached -- NOTOK':
                    logger.warn('APILIMIT     ---- Max rate limit reached')
                    continue
                elif a.args[0] == 'None -- Query Timeout occured. Please select a smaller result dataset':
                    logger.warn('APITIMEOUT   ---- Query Timeout')
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


@APIwrapper
def get_normal_transaction_by_hash(bsc, hash):
    check_API_limit()
    return bsc.get_proxy_transaction_by_hash(hash)


@APIwrapper
def get_tx_status(bsc, hash):
    check_API_limit()
    return bsc.get_tx_receipt_status(hash)


@APIwrapper
def get_circulating_supply(bsc, address):
    check_API_limit()
    return int(bsc.get_circulating_supply_by_contract_address(address))


@APIwrapper
def get_source(bsc, address):
    check_API_limit()
    return bsc.get_contract_source_code(address)


@APIwrapper
def get_bytecode(bsc, address):
    check_API_limit()
    return bsc.get_proxy_code_at(address)


@APIwrapper
def get_first_bep20_transaction(bsc, address):
    check_API_limit()
    return bsc.get_bep20_token_transfer_events_by_contract_address_paginated(contract_address=address, page=1, offset=1, sort='asc')


@APIwrapper
def get_first_bep721_transaction(bsc, address):
    check_API_limit()
    return bsc.get_bep721_token_transfer_events_by_contract_address_paginated(contract_address=address, page=1, offset=1, sort='asc')


@APIwrapper
def get_first_native_transaction(bsc, address):
    check_API_limit()
    return bsc.get_normal_txs_by_address_paginated(address=address, page=1, offset=1, startblock=0, endblock=999999999, sort='asc')


@APIwrapper
def get_receipt_from_tx(bsc, transaction_hash):
    check_API_limit()
    return bsc.get_proxy_transaction_receipt(txhash=transaction_hash)


@APIwrapper
def get_bep20_transactions(bsc, address, options: SearchOptions):
    # TODO: find a way to get above a 20k transaction limit
    #! startblk = highest of first batch
    #! endblk = lowest of second batch
    #! get tx from range above, if result = 10000, use endblk = highest of this batch
    page = 1
    sort = 'asc'
    number_of_records = 10000  # max
    bep20_transactions = []

    while True:
        bep20_queryresult = []

        if (Filter.Contract in options.filterBy
                or Filter.Contract_and_NativeTransfers in options.filterBy):
            check_API_limit()
            bep20_queryresult = bsc.get_bep20_token_transfer_events_by_address_and_contract_paginated(
                contract_address=options.contractFilter, address=address, page=page, offset=number_of_records, sort=sort)
        elif(Filter.Blocks in options.filterBy):
            check_API_limit()
            bep20_queryresult = bsc.get_bep20_token_transfer_events_by_address(
                address=address, startblock=options.startBlock, endblock=options.endBlock, sort=sort)
        else:
            check_API_limit()
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
def get_native_transactions(bsc, address, options: SearchOptions):
    # TODO: find a way to get above a 20k transaction limit
    #! startblk = highest of first batch
    #! endblk = lowest of second batch
    #! get tx from range above, if result = 10000, use endblk = highest of this batch
    page = 1
    sort = 'asc'
    number_of_records = 10000  # max
    native_transactions = []

    while True:
        nat_tx_queryresult = []

        if(Filter.Blocks in options.filterBy):
            check_API_limit()
            nat_tx_queryresult = bsc.get_normal_txs_by_address(
                address=address, startblock=options.startBlock, endblock=options.endBlock, sort=sort)
        else:
            check_API_limit()
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


@APIwrapper
def get_bep721_transactions(bsc, address, options: SearchOptions):
    # TODO: find a way to get above a 20k transaction limit
    #! startblk = highest of first batch
    #! endblk = lowest of second batch
    #! get tx from range above, if result = 10000, use endblk = highest of this batch
    page = 1
    sort = 'asc'
    number_of_records = 10000  # max
    bep721_transactions = []

    while True:
        bep721_tx_queryresult = []

        if (Filter.Contract in options.filterBy
                or Filter.Contract_and_NativeTransfers in options.filterBy):
            check_API_limit()
            bep721_tx_queryresult = bsc.staticget_bep721_token_transfer_events_by_address_and_contract_paginated(
                contract_address=options.contractFilter, address=address, page=page, offset=number_of_records, sort=sort)
        elif(Filter.Blocks in options.filterBy):
            check_API_limit()
            bep721_tx_queryresult = bsc.get_bep721_token_transfer_events_by_address(
                address=address, startblock=options.startBlock, endblock=options.endBlock, sort=sort)
        else:
            check_API_limit()
            bep721_tx_queryresult = bsc.get_bep721_token_transfer_events_by_address(
                address=address, startblock=0, endblock=9999999999, sort=sort)
        bep721_transactions.extend(bep721_tx_queryresult)

        if len(bep721_tx_queryresult) < number_of_records or len(bep721_transactions) == 20000:
            break
        if len(bep721_transactions) == 10000:
            # * limit of 10k can be circumvented by changing sort order (max 20k)
            sort = 'desc'
            page = 1
        else:
            page += 1

    return bep721_transactions
