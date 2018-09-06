class ResolveContractDetailsException(Exception):
    pass


class IBTimeoutException(Exception):
    pass


class UnresolvedContractException(Exception):
    pass


class MultipleContractException(Exception):
    pass


class HistoricalDataRetrieveException(Exception):
    pass


class HistoricalDataTimeoutException(Exception):
    pass


class GetAccountDataException(Exception):
    pass


class CheckInstrumentExistenceException(Exception):
    pass


class GetCategoryIDException(Exception):
    pass


class InsertNewInstrumentException(Exception):
    pass


class TransformEODDataException(Exception):
    pass


class InsertNewUnitException(Exception):
    pass


class AddStopOrdersException(Exception):
    pass


class UpdateStopOrderException(Exception):
    pass


class AddContractToQueueException(Exception):
    pass


class GetDataFromMarketDataException(Exception):
    pass


class NoBidAskPricesAvailable(Exception):
    pass
