from datetime import datetime

from web3 import Web3
from web3.main import Web3
from panoramix.abi import getAbiJson
from panoramix.decompiler import decompile_bytecode

import modules.gatherer.database as gdb
import modules.processor.database as pdb


def decompile_contract(contract_bytecode):
    return decompile_bytecode(contract_bytecode)


def generateABI(decompilation):
    return getAbiJson(decompilation.json)


def generate_missing_ABI_for_contractDB():
    queryresult = gdb.contractDB.find(
        'ABI == "Contract source code not verified"')
    elements = gdb.contractDB.getmany(queryresult)

    for entry in elements:
        decompilation = decompile_contract(entry['Bytecode'])
        abi = generateABI(decompilation)

        gdb.contractDB.update(entry['contractAddress'], {
                              'ABI': abi, 'SourceCode': decompilation.text})


def join_transaction_data():
    normal_keys = gdb.txDB_NATIVE.getallkeys()
    bep20_left = set(gdb.txDB_BEP20.getallkeys())

    for key in normal_keys:
        native = gdb.txDB_NATIVE.get(key)

        #! SUPPRESS LITERAL ERROR
        if not isinstance(native, dict):
            continue

        if native['input'] == '0x':
            pdb.transactionDB.insert({'hash': native['hash'],
                                      'method': '0x',
                                      'blockNumber': native['blockNumber'],
                                      'timeStamp': native['timeStamp'],
                                      'from': native['from'],
                                      'to': native['to'],
                                      'interactedWith': '',
                                      'value': native['value'],
                                      'contractAddress': native['contractAddress'],
                                      'tokenName': '',
                                      'tokenSymbol': '',
                                      'tokenDecimal': '',
                                      'txreceipt_status': native['txreceipt_status']})
        else:
            bep20_matches = gdb.txDB_BEP20.find(
                'hash == "' + native['hash'] + '"')

            if bep20_matches:
                for key in bep20_matches:
                    bep20 = gdb.txDB_BEP20.get(key)

                    #! SUPPRESS LITERAL ERROR
                    if not isinstance(bep20, dict):
                        raise TypeError

                    pdb.transactionDB.insert({'hash': native['hash'],
                                              'method': native['input'][:10],
                                              'blockNumber': native['blockNumber'],
                                              'timeStamp': native['timeStamp'],
                                              'from': bep20['from'],
                                              'to': bep20['to'],
                                              'interactedWith': native['to'],
                                              'value': bep20['value'],
                                              'contractAddress': bep20['contractAddress'],
                                              'tokenName': bep20['tokenName'],
                                              'tokenSymbol': bep20['tokenSymbol'],
                                              'tokenDecimal': bep20['tokenDecimal'],
                                              'txreceipt_status': native['txreceipt_status']})
                bep20_left -= set(bep20_matches)
            elif native['txreceipt_status'] == "1":
                pdb.transactionDB.insert({'hash': native['hash'],
                                          'method': native['input'][:10],
                                          'blockNumber': native['blockNumber'],
                                          'timeStamp': native['timeStamp'],
                                          'from': native['from'],
                                          'to': native['to'],
                                          'interactedWith': '',
                                          'value': native['value'],
                                          'contractAddress': native['contractAddress'],
                                          'tokenName': '',
                                          'tokenSymbol': '',
                                          'tokenDecimal': '',
                                          'txreceipt_status': native['txreceipt_status']})

    for key in bep20_left:
        bep20 = gdb.txDB_BEP20.get(key)

        #! SUPPRESS LITERAL ERROR
        if not isinstance(bep20, dict):
            raise TypeError

        pdb.transactionDB.insert({'hash': bep20['hash'],
                                  'method': 'NO_NAT_TX',
                                  'blockNumber': bep20['blockNumber'],
                                  'timeStamp': bep20['timeStamp'],
                                  'from': bep20['from'],
                                  'to': bep20['to'],
                                  'interactedWith': '',
                                  'value': bep20['value'],
                                  'contractAddress': bep20['contractAddress'],
                                  'tokenName': bep20['tokenName'],
                                  'tokenSymbol': bep20['tokenSymbol'],
                                  'tokenDecimal': bep20['tokenDecimal'],
                                  'txreceipt_status': ''})


def initialize_contracts():
    allcontracts = gdb.contractDB.getallkeys()
    contractdict = {}

    for contract in allcontracts:
        contractdata = gdb.contractDB.get(contract)
        #! SUPPRESS LITERAL ERROR
        if not isinstance(contractdata, dict):
            raise TypeError

        address = contractdata['contractAddress']
        ethcontract = w3.eth.contract(
            address=Web3.toChecksumAddress(contractdata['contractAddress']), abi=contractdata['ABI'])
        contractdict[address] = ethcontract

    return contractdict


def match_input_to_function(contractdict):
    all = pdb.transactionDB.find('txreceipt_status == "1"')
    transfers = pdb.transactionDB.find(
        'method == "0x" and txreceipt_status == "1"')
    interactions = list(set(all) - set(transfers))

    for tx in transfers:
        pdb.transactionDB.update(tx, {'method': 'transfer'})

    for tx in interactions:
        transaction = pdb.transactionDB.get(tx)

        #! SUPPRESS LITERAL ERROR
        if not transaction:
            continue

        if transaction['interactedWith'] == '':
            if transaction['to'] in contractdict:
                address = transaction['to']
            else:
                raise Exception()
        else:
            address = transaction['interactedWith']

        if contractdict[address]:
            function = contractdict[address].get_function_by_selector(
                transaction['method'])
            pdb.transactionDB.update(
                tx, {'method': function.function_identifier})


def create_report_md():
    wallets = gdb.walletDB.getall()

    report = ''

    for wallet in wallets:
        transaction_hashes = set(wallet['txBEP'] + wallet['txNAT'])

        query = [
            pdb.transactionDB.find(f'hash == {transaction_hash}')
            for transaction_hash in transaction_hashes
        ]

        transactions = pdb.transactionDB.getmany(query)
        transactions = sorted(transactions, key=lambda d: d['blockNumber'])

        # TODO: include sold, bought and transfered token amounts into overview
        report += (f'# {wallet["address"]}\n\n'
                   '| HASH  | BLOCK | METHOD | DATE | FROM | TO | VALUE | CONTRACT |\n')

        for transaction in transactions:
            ts = datetime.fromtimestamp(
                int(transaction["timeStamp"])).strftime("%m/%d/%Y, %H:%M:%S")
            report += f'| {transaction["hash"]} | {transaction["blockNumber"]} | {transaction["method"]} | {ts} | {transaction["from"]} | {transaction["to"]} | {transaction["value"]} | {transaction["contractAddress"]} |\n'

        report += '\n\n\n'

    # TODO: name handling
    with open('testreport.md', 'w') as f:
        f.write(report)

# TODO: Create wallet relation tree


# TODO: Calculate sells, buys, transfers


# TODO: fill visualization/report database


def process_data():
    join_transaction_data()
    generate_missing_ABI_for_contractDB()

    contractdict = initialize_contracts()
    match_input_to_function(contractdict)


w3 = Web3()
