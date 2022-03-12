from datetime import datetime
from functools import lru_cache
import os
import markdown2
import mdformat
import logging
import inspect

from web3 import Web3
from web3.main import Web3
from panoramix.abi import getAbiJson
from panoramix.decompiler import decompile_bytecode

import modules.gatherer.database as gdb
import modules.processor.database as pdb


w3 = Web3()
SESSIONPATH = os.getenv('SESSIONPATH')
NATIVE_COIN = os.getenv('NATIVE_COIN')
NATIVE_DECIMAL = os.getenv('NATIVE_DECIMAL')
BLOCK_EXPLORER = os.getenv('BLOCK_EXPLORER')


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


@lru_cache
def convert_to_amount(amount, decimals):
    if not isinstance(amount, int):
        amount = int(amount, base=10)
    if not isinstance(decimals, int):
        decimals = int(decimals, base=10)
    return amount / pow(10, decimals)


def join_transaction_data():
    normal_keys = gdb.txDB_NATIVE.getallkeys()
    bep20_left = set(gdb.txDB_BEP20.getallkeys())

    for key in normal_keys:
        native = gdb.txDB_NATIVE.get(key)

        #! SUPPRESS LITERAL ERROR
        if not isinstance(native, dict):
            continue

        if native['txreceipt_status'] == "1":
            if native['input'] == '0x':
                pdb.transactionDB.insert({'hash': native['hash'],
                                          'method': '0x',
                                          'blockNumber': int(native['blockNumber']),
                                          'timeStamp': native['timeStamp'],
                                          'from': native['from'],
                                          'to': native['to'],
                                          'interactedWith': '',
                                          'value': convert_to_amount(native['value'], NATIVE_DECIMAL),
                                          'contractAddress': native['contractAddress'],
                                          'tokenName': NATIVE_COIN,
                                          'tokenSymbol': NATIVE_COIN,
                                          'txreceipt_status': native['txreceipt_status']})
            elif bep20_matches := gdb.txDB_BEP20.find(
                    'hash == "' + native['hash'] + '"'):

                for key in bep20_matches:
                    bep20 = gdb.txDB_BEP20.get(key)

                    #! SUPPRESS LITERAL ERROR
                    if not isinstance(bep20, dict):
                        raise TypeError

                    pdb.transactionDB.insert({'hash': native['hash'],
                                              'method': native['input'][:10],
                                              'blockNumber': int(native['blockNumber']),
                                              'timeStamp': native['timeStamp'],
                                              'from': bep20['from'],
                                              'to': bep20['to'],
                                              'interactedWith': native['to'],
                                              'value': convert_to_amount(bep20['value'], bep20['tokenDecimal']),
                                              'contractAddress': bep20['contractAddress'],
                                              'tokenName': bep20['tokenName'],
                                              'tokenSymbol': bep20['tokenSymbol'],
                                              'txreceipt_status': native['txreceipt_status']})
                bep20_left -= set(bep20_matches)
            else:
                pdb.transactionDB.insert({'hash': native['hash'],
                                          'method': native['input'][:10],
                                          'blockNumber': int(native['blockNumber']),
                                          'timeStamp': native['timeStamp'],
                                          'from': native['from'],
                                          'to': native['to'],
                                          'interactedWith': '',
                                          'value': convert_to_amount(native['value'], NATIVE_DECIMAL),
                                          'contractAddress': native['contractAddress'],
                                          'tokenName': NATIVE_COIN,
                                          'tokenSymbol': NATIVE_COIN,
                                          'txreceipt_status': native['txreceipt_status']})

    for key in bep20_left:
        bep20 = gdb.txDB_BEP20.get(key)

        #! SUPPRESS LITERAL ERROR
        if not isinstance(bep20, dict):
            raise TypeError

        pdb.transactionDB.insert({'hash': bep20['hash'],
                                  'method': 'NO_NAT_TX',
                                  'blockNumber': int(bep20['blockNumber']),
                                  'timeStamp': bep20['timeStamp'],
                                  'from': bep20['from'],
                                  'to': bep20['to'],
                                  'interactedWith': '',
                                  'value': convert_to_amount(bep20['value'], bep20['tokenDecimal']),
                                  'contractAddress': bep20['contractAddress'],
                                  'tokenName': bep20['tokenName'],
                                  'tokenSymbol': bep20['tokenSymbol'],
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


def define_receiver(wallet, transaction):
    if transaction['to'] == wallet['address']:
        return f'[THIS WALLET](#{wallet["address"]})'
    elif gdb.walletDB.exists(transaction['to']):
        return f'[{transaction["to"]}](#{transaction["to"]})'
    elif contract := gdb.contractDB.get(transaction['to']):
        return f'[{contract["Name"]}</br>({transaction["to"]})](https://{BLOCK_EXPLORER}/address/{transaction["from"]})'

    else:
        return f'[{transaction["to"]}](https://{BLOCK_EXPLORER}/address/{transaction["to"]})'


def define_sender(wallet, transaction):
    if transaction['from'] == wallet['address']:
        return f'[THIS WALLET](#{wallet["address"]})'
    elif gdb.walletDB.exists(transaction['from']):
        return f'[{transaction["from"]}](#{transaction["from"]})'
    elif contract := gdb.contractDB.get(transaction['from']):
        return f'[{contract["Name"]}</br>({transaction["from"]})](https://{BLOCK_EXPLORER}/address/{transaction["from"]})'
    else:
        return f'[{transaction["from"]}](https://{BLOCK_EXPLORER}/address/{transaction["from"]})'


def define_action(wallet, transaction):
    if "swap" not in transaction['method'].casefold():
        return transaction['method']
    if wallet['address'] == transaction['from']:
        return f'<abbr title="{transaction["method"]}">swap (sell)</abbr>'
    elif wallet['address'] == transaction['to']:
        return f'<abbr title="{transaction["method"]}">swap (buy)</abbr>'
    else:
        return f'<abbr title="{transaction["method"]}">swap</abbr>'


def create_report_md():
    wallets = gdb.walletDB.getall()

    report = '# TABLE OF CONTENTS\n\n<!-- mdformat-toc start -->\n\n'

    for wallet in wallets:
        transaction_hashes = set(wallet['txBEP'] + wallet['txNAT'])
        query = []

        for transaction_hash in transaction_hashes:
            query.extend(pdb.transactionDB.find(
                f'hash == "{transaction_hash}"'))

        transactions = pdb.transactionDB.getmany(query)
        transactions = sorted(transactions, key=lambda d: d['blockNumber'])

        # TODO: include sold, bought and transfered token amounts into overview
        report += (f'# {wallet["address"]}\n\n'
                   '| HASH | <div style="width:100px">METHOD</div> | <div style="width:100px">BLOCK</div> | <div style="width:300px">FROM</div> | <div style="width:300px">TO</div> | <div style="width:150px">VALUE</div> | CONTRACT |\n'
                   '| :-- | :--    | :-:   | :--  | :--| --:   | :--      |\n')

        for transaction in transactions:
            receiver = define_receiver(wallet, transaction)
            sender = define_sender(wallet, transaction)

            tokenText = f'<abbr title="{transaction["contractAddress"]}">{transaction["tokenName"]} ({transaction["tokenSymbol"]})</abbr>'
            shorthash = f'[{transaction["hash"][:15]}...](https://{BLOCK_EXPLORER}/tx/{transaction["hash"]})'

            action = define_action(wallet, transaction)

            report += f'| {shorthash} | {action} | {transaction["blockNumber"]} | {sender} | {receiver} | {"{:10.4f}".format(transaction["value"])} | {tokenText} |\n'

        report += '\n\n\n'

    mdreport = mdformat.text(report, extensions=["tables", "toc", "gfm"])

    htmlrep = markdown2.markdown(mdreport, extras=["tables"])

    # TODO: name handling
    with open(os.path.join(SESSIONPATH, 'report.md'), 'w') as f:
        f.write(mdreport)
    with open(os.path.join(SESSIONPATH, 'report.html'), 'w') as f:
        f.write(htmlrep)


# TODO: Create wallet relation tree
# ? https://github.com/networkx/networkx

# TODO: Calculate sells, buys, transfers


# TODO: fill visualization/report database


def process_data():
    join_transaction_data()
    generate_missing_ABI_for_contractDB()

    contractdict = initialize_contracts()
    match_input_to_function(contractdict)

    create_report_md()
