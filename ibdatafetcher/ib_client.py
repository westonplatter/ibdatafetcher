import time
import pandas as pd
from loguru import logger
import calendar
from dateutil.relativedelta import relativedelta
from datetime import datetime, date, timedelta
from trading_calendars import get_calendar, TradingCalendar
from typing import Optional, Dict, Any, List, Tuple, Optional
from ib_insync import IB, Future, ContFuture, Stock, Contract
from ib_insync import util as ib_insync_util

# internal code/modules
from spreads import FutureCalendarSpread, ActionType, UnderlyingSymbol, SecType
from models import (
    Quote,
    gen_engine,
    init_db,
    db_insert_df_conflict_on_do_nothing,
    transform_rename_df_columns,
)

######################################################################
### Configs

# because we're limited to 60 requests every 10 minutes
# See https://interactivebrokers.github.io/tws-api/historical_limitations.html
SLEEP_TIME = 2.0

FETCH_LAST_X_DAYS = 50
INDIVIDUAL_CONTRACT_DATA_POINTS = ["TRADES", "ASK", "BID"]
SPREAD_CONTRACT_DATA_POINTS = ["BID_ASK"]
IB_CLIENT_HOST_IP = "127.0.0.1"
IB_CLIENT_PORT = 4001
IB_CLIENT_ID = 1


engine = gen_engine()
init_db(engine)
ib = IB()
ib.connect(IB_CLIENT_HOST_IP, IB_CLIENT_PORT, IB_CLIENT_ID)

###################
# in code lookup for LocalSymbol / ConId

# key[int]: conId
# value[str]: localSymbol
contract_reference = {}


def set_contracts_reference(spread) -> None:
    contract_reference[int(spread.m1.conId)] = spread.m1.localSymbol
    contract_reference[int(spread.m2.conId)] = spread.m2.localSymbol


def get_contract_reference(conId: int) -> str:
    return contract_reference[conId]


###################


# def get_symbol(contract):
#     return contract.symbol


# def get_local_symbol(contract):
#     if contract.secType == SecType.BAG.value:
#         front = get_contract_reference(contract.comboLegs[0].conId)
#         back = get_contract_reference(contract.comboLegs[1].conId)
#         return f"{front}/{back}"
#     else:
#         return contract.localSymbol


def is_not_weekday(__date) -> bool:
    result = __date.weekday() > 4  # 4 = Friday
    # skip thanksgiving
    if "2020-11-26" == __date.strftime("%Y-%m-%d"):
        return True
    # TODO(weston) ignore trading holidays with trading_calendars module
    return result


def clean_query(query: str) -> str:
    return query.replace("\n", "").replace("\t", "")


def data_already_fetched(engine, contract, value_type, __date) -> bool:
    date_str: str = __date.strftime("%Y-%m-%d")

    query = f"""
        select count(*)
        from {Quote.__tablename__}
        where
            local_symbol = '{get_local_symbol(contract)}'
            and date(ts) = date('{date_str}')
            and value_type = '{value_type}'
    """
    with engine.connect() as con:
        result = con.execute(clean_query(query))
        counts = [x for x in result]

    count = counts[0][0]
    return count != 0


def fetch_data(contract, value_type, last_x_days: int = 10):
    for i in range(1, last_x_days):
        ago = date.today() - relativedelta(days=i)
        __date = ago.strftime("%Y%m%d")
        end_date_time = f"{__date} 23:59:59"

        # Skip if Saturday and Sunday
        if is_not_weekday(ago):
            logger.debug(
                f"{get_local_symbol(contract)} - {value_type} - {__date} => not a market day"
            )
            continue

        # Skip if we already have the data
        if data_already_fetched(engine, contract, value_type, ago):
            logger.debug(
                f"{get_local_symbol(contract)} - {value_type} - {__date} => data already fetched"
            )
            continue

        logger.debug(
            f"{get_local_symbol(contract)} - {value_type} - {__date} -> fetching"
        )

        # extract / fetch data from IB
        bars = ib.reqHistoricalData(
            contract,
            endDateTime=end_date_time,
            durationStr="1 D",
            barSizeSetting="1 min",
            whatToShow=value_type,
            useRTH=True,
            # formatDate=2,  # return as UTC time
        )
        df = ib_insync_util.df(bars)

        # manually fill in data
        df["symbol"] = get_symbol(contract)
        df["local_symbol"] = get_local_symbol(contract)
        df["value_type"] = value_type
        df["rth"] = True

        # transform
        transform_rename_df_columns(df)

        # load / insert data in DB
        db_insert_df_conflict_on_do_nothing(engine, df, Quote.__tablename__)

        # sleep so we don't overload IB and get throttled
        ib.sleep(SLEEP_TIME)


future_spread_symbols = [
    UnderlyingSymbol.MES,
    UnderlyingSymbol.ES,
    UnderlyingSymbol.MNQ,
    UnderlyingSymbol.NQ,
    UnderlyingSymbol.RTY,
    UnderlyingSymbol.M2K,
]

spreads = []

for future_spread_symbol in future_spread_symbols:
    spread = FutureCalendarSpread(
        underlying_symbol=future_spread_symbol.value, action=ActionType.BUY
    )
    spread.init_contracts(ib)
    set_contracts_reference(spread)
    spreads.append(spread)


def execute_fetch(contracts, value_types, last_x_days):
    for contract in contracts:
        for value_type in value_types:
            fetch_data(contract, value_type, last_x_days)


for spread in spreads:
    # front and back contracts
    contracts = spread.m1, spread.m2
    value_types = INDIVIDUAL_CONTRACT_DATA_POINTS
    execute_fetch(contracts, value_types, FETCH_LAST_X_DAYS)
    # spread contract
    contracts = [spread.contract]
    value_types = SPREAD_CONTRACT_DATA_POINTS
    execute_fetch(contracts, value_types, FETCH_LAST_X_DAYS)