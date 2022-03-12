import os
import logging
import inspect

import modules.gatherer.api_functions as api
import modules.gatherer.crawler as crawler
import modules.gatherer.database as gdb
from bscscan import BscScan
from modules.classes import *

# In the garden we are growin'
# Many changes will be flowin'
# If you want to be amazin'
# See the flowers we are raisin'


# region SETUP
api_key = os.getenv('API_KEY') or ''
SESSIONPATH = os.getenv('SESSIONPATH')
# endregion


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

    gdb.contract_insert(entry)


def _check_token(contract_address: Address):
    with BscScan(api_key=api_key, asynchronous=False) as bsc:  # type: ignore
        is_token = False
        contract = gdb.contractDB.get(contract_address)

        if not contract:
            circulating = api.get_circulating_supply(bsc, contract_address)
            if circulating > 0:
                is_token = True
                source = api.get_source(bsc, contract_address)
                bytecode = api.get_bytecode(bsc, contract_address)
                txdata = api.get_first_bep20_transaction(bsc, contract_address)
                _save_contract_information(
                    contract_address, txdata, source, bytecode, True)
        elif contract['type'] == 'token':
            is_token = True

        return is_token


def _get_recipients_from_receipt(transaction_hash):
    with BscScan(api_key=api_key, asynchronous=False) as bsc:  # type: ignore
        receipt = api.get_receipt_from_tx(bsc, transaction_hash)
        contract_address = Address(receipt['logs'][0]['address'])
        recipients = []

        for log in receipt['logs']:
            tmp = log['topics'][2]
            recipients.append(Address(f'0x{tmp[-40:]}'))

    return recipients, contract_address


def follow_tokenflow_by_address(address, options: SearchConfig):
    if options.contractFilter != '':
        _check_token(options.contractFilter)

    if isinstance(address, list):
        for addr in address.copy():
            if not isinstance(addr, Address):
                address.remove(addr)
                address.append(Address(addr))
    elif ',' in address:
        address = [Address(address)
                     for address in address.split(',')]
    else:
        address = [Address(address)]

    crawler.start_crawling(addresses=address, options=options)


def follow_tokenflow_by_tx(transaction_hash: Hash, options: SearchConfig):
    recipients, contract_address = _get_recipients_from_receipt(
        transaction_hash)
    options.contractFilter = contract_address
    options.filterBy = [Filter.Contract_and_NativeTransfers]

    _check_token(contract_address)

    crawler.start_crawling(addresses=recipients, options=options)


#def follow_tokenflow(by: SearchType, options: SearchOptions, tx=None, addresses=None):
#    if by == SearchType.TX and tx:
#        tx = Hash(tx)
#        follow_tokenflow_by_tx(transaction_hash=tx, options=options)
#
#    if by == SearchType.ADDR and addresses:
#        follow_tokenflow_by_address(addresses=addresses, options=options)


def follow_tokenflow(options: SearchConfig):
    if options.search_by and isinstance(options.search_by, Hash):
        follow_tokenflow_by_tx(transaction_hash=options.search_by, options=options)

    if options.search_by and isinstance(options.search_by, Address):
        follow_tokenflow_by_address(
            address=options.search_by, options=options)
        
def continue_from_session():
    crawler.resume_crawling()
