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
    gatherer.donotfollow = set.union(dead_addresses, mint_addresses)
    # endregion

    ifmain_end = True


prog_end = True
