from enum import Enum, auto


class Direction(Enum):
    LEFT = auto()
    RIGHT = auto()
    ALL = auto()


class SearchType(Enum):
    TX = auto()
    ADDR = auto()


class Filter(Enum):
    Contract = auto()
    Contract_and_NativeTransfers = auto()
    TimeStamp = auto()
    Blocks = auto()
    NONE = auto()


class TrackConfig(Enum):
    BEP20 = auto()
    NATIVE = auto()
    NFT = auto()
    ALL = auto()


class ContractType(Enum):
    TOKEN = auto()
    NFT = auto()
    CONTRACT = auto()


class Address(str):
    def __init__(self, input: str):
        if not isinstance(input, str):
            raise ValueError('input must be string')
        input = input.strip()
        if len(input) != 42:
            raise ValueError('address length is incorrect')
        if input[:2] != '0x':
            raise ValueError('address must start with 0x')
        self = input.lower()

    def __eq__(self, other):
        return (self.casefold() == other.casefold())

    def __ne__(self, other):
        return not (self.casefold() == other.casefold())

    def __hash__(self) -> int:
        return super().__hash__()


class Hash(str):
    def __init__(self, input: str):
        if not isinstance(input, str):
            raise ValueError('input must be string')
        input = input.strip()
        if len(input) != 66:
            raise ValueError('transaction hash length is incorrect')
        if input[:2] != '0x':
            raise ValueError('transaction hash must start with 0x')
        self = input.casefold()

    def __eq__(self, other):
        return (self.casefold() == other.casefold())

    def __ne__(self, other):
        return not (self.casefold() == other.casefold())

    def __hash__(self) -> int:
        return super().__hash__()


class SearchConfig():
    def __init__(self, direction: Direction, filterBy=None, trackConfig=TrackConfig.ALL, search_by='', contractFilter='', startBlock=0, endBlock=9999999999, startTimestamp=0, endTimestamp=2147483647):
        if filterBy is None:
            filterBy = [Filter.NONE]
        if not isinstance(filterBy, list):
            filterBy = [filterBy]
        if not isinstance(contractFilter, Address):
            contractFilter = Address(contractFilter)

        if len(search_by) == 66:
            search_by = Hash(search_by)
        elif len(search_by) == 42:
            search_by = Address(search_by)

        self.direction = direction
        self.filterBy = filterBy
        self.trackConfig = trackConfig
        self.search_by = search_by
        self.contractFilter = contractFilter
        self.startBlock = startBlock
        self.endBlock = endBlock
        self.startTimestamp = startTimestamp
        self.endTimestamp = endTimestamp
