import modules.data_gathering as gatherer
import modules.data_processing as processor
from modules.data_gathering import (ADDRESS, TXHASH, Direction, Filter,
                                    SearchOptions, SearchType, TrackConfig)

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
    '0xdccf3b77da55107280bd850ea519df3705d1a75a'
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
    # region SETUP GATHERER
    # get API key for bscscan
    with open('api_key.txt', 'r') as file:
        gatherer.api_key = file.read()

    # set API limit
    # gatherer.APICALLS_PER_SECOND = 5

    # set max. threads
    gatherer.api_threads = 1
    gatherer.processing_threads = 1

    # set exclusions
    gatherer.donotfollow = set.union(
        dead_addresses,
        mint_addresses,
        binance_hotwallets
    )
    # endregion

    ifmain_end = True


prog_end = True
