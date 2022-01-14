import modules.gatherer.api_functions as api
import modules.gatherer.crawler as crawler
import modules.logging as logger
from bscscan import BscScan
from modules.classes import *
import modules.gatherer.database as db

# In the garden we are growin'
# Many changes will be flowin'
# If you want to be amazin'
# See the flowers we are raisin'


# region SETUP
api_key = str()
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

    db.contract_insert(entry)


def _check_token(contract_address: ADDRESS):
    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        is_token = False
        contract = db.contract_data(contract_address)

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
    with BscScan(api_key=api_key, asynchronous=False) as bsc:
        receipt = api.get_receipt_from_tx(bsc, transaction_hash)
        contract_address = ADDRESS(receipt['logs'][0]['address'])
        recipients = []

        for log in receipt['logs']:
            tmp = log['topics'][2]
            recipients.append(ADDRESS('0x' + tmp[-40:]))

    return recipients, contract_address


def follow_tokenflow_by_address(addresses: list, options: SearchOptions):
    if options.contractFilter != '':
        _check_token(options.contractFilter)

    if isinstance(addresses, list):
        for addr in addresses.copy():
            if not isinstance(addr, ADDRESS):
                addresses.remove(addr)
                addresses.append(ADDRESS(addr))
    elif ',' in addresses:
        addresses = [ADDRESS(address)
                     for address in addresses.split(',')]
    else:
        addresses = [ADDRESS(addresses)]

    crawler.start_crawler_workers(addresses=addresses, options=options)


def follow_tokenflow_by_tx(transaction_hash: TXHASH, options: SearchOptions):
    recipients, contract_address = _get_recipients_from_receipt(
        transaction_hash)
    options.contractFilter = contract_address
    options.filterBy = [Filter.Contract_and_NativeTransfers]

    _check_token(contract_address)

    crawler.start_crawler_workers(addresses=recipients, options=options)


def follow_tokenflow(by: SearchType, options: SearchOptions, tx=None, addresses=None):
    logger.info('-----                  Starting                  -----')
    if by == SearchType.TX and tx:
        tx = TXHASH(tx)
        follow_tokenflow_by_tx(transaction_hash=tx, options=options)

    if by == SearchType.ADDR and addresses:
        follow_tokenflow_by_address(addresses=addresses, options=options)
    logger.info('-----                  Finished                  -----')
