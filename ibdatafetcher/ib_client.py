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

from spreads import FutureCalendarSpread, ActionType, UnderlyingSymbol, SecType
from models import Quote, gen_engine, init_db, db_insert_df_conflict_on_do_nothing, transform_rename_df_columns

engine = gen_engine()
init_db(engine)
ib = IB()
ib.connect("127.0.0.1", 4001, 1)


def get_symbol(contract):
    return contract.symbol

def get_local_symbol(contract):
    return contract.localSymbol

def is_not_weekday(__date) -> bool:
    return __date.weekday() < 5


def fetch_data(contract, value_type):
    for i in range(1, 90):
        ago = date.today() - relativedelta(days=i)

        if is_not_weekday(ago):
            pass

        __date = ago.strftime("%Y%m%d")
        end_date_time = f"{__date} 23:59:59"
        
        logger.debug(f"{contract.localSymbol} - {value_type} - {__date}")

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
        # TODO(weston) handle contracts and spreads here
        df["symbol"] = "MES" # get_symbol(contract)
        df["local_symbol"] = "MES1-2" # get_local_symbol(contract)
        df['value_type'] = value_type
        df['rth'] = True

        # transform
        transform_rename_df_columns(df)

        # load
        db_insert_df_conflict_on_do_nothing(engine, df, Quote.__tablename__)

mes_spread = FutureCalendarSpread(
    underlying_symbol=UnderlyingSymbol.MES.value,
    action=ActionType.BUY
)
mes_spread.init_contracts(ib)

cs = [mes_spread.m1, mes_spread.m2]
value_types = ["TRADES", "ASK", "BID"]

for contract in cs:
    for value_type in value_types:
        fetch_data(contract, value_type)
        ib.sleep(2.0)

# for contract in [mes_spread.contract]:
#     for value_type in ["BID_ASK", "TRADES"]:
#         fetch_data(contract, value_type)
#         ib.sleep(2.0)


# value_types = ["TRADES", "ASK"]

# contracts = [mes_spread.m1, mes_spread.m2]

# contracts = [mes_spread]

# value_types = ["TRADES", "BID"]
# value_types = ["BID_ASK"]
# value_types = ["TRADES", "ASK"]

# contracts = [mes_spread2.contract]
# value_types = ["TRADES", "ASK"]