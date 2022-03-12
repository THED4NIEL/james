from re import S
import modules.gatherer.gatherer as gatherer
import modules.processor.processor as processor
import logging
import inspect
import os
import pickle
from modules.classes import *
from modules.sessions import select_session, save_search_config
from dotenv import load_dotenv

# from chatterbot import ChatBot
# from chatterbot.trainers import ChatterBotCorpusTrainer

# def james():
#   chatbot = ChatBot('JAMES')
#   trainer = ChatterBotCorpusTrainer(chatbot)
#   trainer.train('chatterbot.corpus.english')
#   chatbot.get_response('Hello, I\'m JAMES, how are you today?')


# region STATIC ADDRESSES

binance_hotwallets = {
    '0x8894e0a0c962cb723c1976a4421c95949be2d4e3',
    '0x515b72ed8a97f42c568d6a143232775018f133c8',
    '0x161ba15a5f335c9f06bb5bbb0a9ce14076fbb645',
    '0xe2fc31f816a9b94326492132018c3aecc4a93ae1',
    '0xa180fe01b906a1be37be6c534a3300785b20d947',
    '0x3c783c21a0383057d128bae431894a5c19f9cf06',
    '0xbd612a3f30dca67bf60a39fd0d35e39b7ab80774',
    '0x73f5ebe90f27b46ea12e5795d16c4b408b19cc6f',
    '0x29bdfbf7d27462a2d115748ace2bd71a2646946c',
    '0xeb2d2f1b8c558a40207669291fda468e50c8a0bb',
    '0xdccf3b77da55107280bd850ea519df3705d1a75a',
}

mint_addresses = {
    '0x0000000000000000000000000000000000000000',
    '0x0000000000000000000000000000000000000001'
}

dead_addresses = {
    '0x000000000000000000000000000000000000dead'
}

# endregion

if __name__ == "__main__":
    load_dotenv()

    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s', force=True)
    # region SETUP GATHERER

    # set exclusions
    gatherer.crawler.donotfollow = set.union(
        dead_addresses,
        mint_addresses,
        binance_hotwallets
    )
    # endregion

    sessionpath = select_session()

    # region TESTING
    gatherer.gdb.reset_crawler_db()
    processor.pdb.transactionDB.clear()
    opt = SearchConfig(Direction.RIGHT,
                       filterBy=Filter.Contract_and_NativeTransfers,
                       trackConfig=TrackConfig.BEP20,
                       search_by='0x547fd18144efef02b8347473d28ec408bc78ef64c6edeb79e2aece4084026bce',
                       contractFilter='0x19263F2b4693da0991c4Df046E4bAA5386F5735E')

    save_search_config(opt)

    gatherer.follow_tokenflow(opt)

    gatherer.gdb.save_crawler_db()
    processor.process_data()
    processor.pdb.transactionDB.save(indent=0)
    # endregion

    ifmain_end = True


prog_end = True
