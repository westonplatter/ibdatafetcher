from enum import Enum
from ib_insync import Contract, util, ComboLeg, Future, IB
from typing import Any
from pydantic import BaseModel


INDIVIDUAL_CONTRACT_DATA_POINTS = ["TRADES", "ASK", "BID"]
SPREAD_CONTRACT_DATA_POINTS = ["BID_ASK"]


class UnderlyingSymbol(Enum):
    ES = "ES"
    MES = "MES"
    NQ = "NQ"
    MNQ = "MNQ"
    RTY = "RTY"
    M2K = "M2K"


class Currency(Enum):
    USD = "USD"


class ActionType(Enum):
    BUY = "BUY"
    SELL = "SELL"


class SecType(Enum):
    BAG = "BAG"
    FUT = "FUT"


class Exchange(Enum):
    GLOBEX = "GLOBEX"
    NYMEX = "NYMEX"


class FutureExpirationDate(Enum):
    M1 = "20201218"
    M2 = "20210319"


class FutureCalendarSpread(BaseModel):
    underlying_symbol: str
    exchange: Exchange = Exchange.GLOBEX
    action: ActionType = None
    m1_expiry: str = "20201218"
    m2_expiry: str = "20210319"
    m1: Any = None
    m2: Any = None
    contract: Any = None
    currency: Currency = Currency.USD
    sec_type: SecType = SecType.BAG

    def __init__(self, **data: Any):
        # pre init
        super().__init__(**data)
        # post init

    def __opposite_action(self, action: ActionType) -> ActionType:
        return ActionType.SELL if action == ActionType.BUY else ActionType.BUY

    def init_contracts(self, ib: IB):
        m1 = Future(
            symbol=self.underlying_symbol,
            lastTradeDateOrContractMonth=self.m1_expiry,
            exchange=self.exchange.value,
        )
        m2 = Future(
            symbol=self.underlying_symbol,
            lastTradeDateOrContractMonth=self.m2_expiry,
            exchange=self.exchange.value,
        )

        self.m1, self.m2 = ib.qualifyContracts(*[m1, m2])

        contract = Contract()
        contract.symbol = self.underlying_symbol
        contract.secType = self.sec_type.value
        contract.currency = self.currency.value
        contract.exchange = self.exchange.value
        leg1 = ComboLeg()
        leg1.conId = self.m1.conId
        leg1.ratio = 1
        leg1.action = self.__opposite_action(self.action).value
        leg1.exchange = self.exchange.value
        leg2 = ComboLeg()
        leg2.conId = self.m2.conId
        leg2.ratio = 1
        leg2.action = self.action.value
        leg2.exchange = self.exchange.value
        contract.comboLegs = []
        contract.comboLegs.append(leg1)
        contract.comboLegs.append(leg2)
        self.contract = contract

    def description(self, short=True) -> str:
        m1ex = self.m1.lastTradeDateOrContractMonth
        m2ex = self.m2.lastTradeDateOrContractMonth

        def shorten(ex):
            return ex[0:6]

        if short:
            m1ex, m2ex = shorten(m1ex), shorten(m2ex)
        return f"{self.underlying_symbol} {m1ex}/{m2ex}"
