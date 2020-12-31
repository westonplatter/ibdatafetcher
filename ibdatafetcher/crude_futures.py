#
# goal - init and download the /CL curve
#

# external deps
from enum import Enum
from ib_insync import Contract, Future, IB
from ib_insync import util as ib_insync_util
from loguru import logger
from pydantic import BaseModel
from typing import Any, List


# internal deps
from spreads import FutureCalendarSpread, Exchange, ActionType, SecType
from models import (
    Quote,
    gen_engine,
    init_db,
    db_insert_df_conflict_on_do_nothing,
    transform_rename_df_columns,
)
from util import get_symbol, get_local_symbol


# contstants
IB_CLIENT_HOST_IP = "127.0.0.1"
IB_CLIENT_PORT = 4001
IB_CLIENT_ID = 1
INDIVIDUAL_CONTRACT_DATA_POINTS = ["TRADES", "ASK", "BID"]
SPREAD_CONTRACT_DATA_POINTS = ["BID_ASK"]

# global variables
ib = IB()
ib.connect(IB_CLIENT_HOST_IP, IB_CLIENT_PORT, IB_CLIENT_ID)
engine = gen_engine()
init_db(engine)

###################
# this is a hacky version of of a securities master
# in code lookup for LocalSymbol / ConId

# key[int]: conId
# value[str]: localSymbol
contract_reference = {}


def set_contracts_reference(spread) -> None:
    contract_reference[int(spread.m1.conId)] = spread.m1.localSymbol
    contract_reference[int(spread.m2.conId)] = spread.m2.localSymbol


def get_contract_reference(conId: int) -> str:
    return contract_reference[conId]


def get_local_symbol(contract):
    if contract.secType == SecType.BAG.value:
        front = get_contract_reference(contract.comboLegs[0].conId)
        back = get_contract_reference(contract.comboLegs[1].conId)
        return f"{front}/{back}"
    else:
        return contract.localSymbol


###################


def gen_fm_bm_pairings(spread: int) -> List[str]:
    """
    spread in months. 1 -> 1 month spread
    """
    ys = ["2021", "2022", "2023"]
    ms = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    exdates = []
    for y in ys:
        for m in ms:
            if y == "2021" and m == "01":
                continue
            exdates.append(f"{y}{m}")
    result = []
    imax = len(exdates) - 1
    for i, ed in enumerate(exdates):
        if i + spread > imax:
            continue
        fm, bm = exdates[i], exdates[i + spread]
        result.append([fm, bm])
    return result


def gen_spread_on_nymex(symbol: str, m1: str, m2: str) -> FutureCalendarSpread:
    return FutureCalendarSpread(
        underlying_symbol=symbol,
        exchange=Exchange.NYMEX,
        action=ActionType.BUY,
        m1_expiry=m1,
        m2_expiry=m2,
    )


qm_curve = []

spread_dates = gen_fm_bm_pairings(1)

for m1, m2 in spread_dates:
    spread = gen_spread_on_nymex("/QM", m1, m2)
    spread.init_contracts(ib)
    set_contracts_reference(spread)
    qm_curve.append(spread)


def fetch_data(contract, yyyymmdd: str, value_type: str):
    bars = ib.reqHistoricalData(
        contract,
        endDateTime=f"{yyyymmdd} 23:59:59",
        durationStr="1 D",
        barSizeSetting="1 min",
        whatToShow=value_type,
        useRTH=True,
        # formatDate=2,  # return as UTC time
    )
    df = ib_insync_util.df(bars)
    return df


def save_df(contract, value_type, df):
    if df is None:
        return

    # transform
    transform_rename_df_columns(df)

    # manually fill in data
    df["symbol"] = get_symbol(contract)
    df["local_symbol"] = get_local_symbol(contract)
    df["value_type"] = value_type
    df["rth"] = True

    # load / insert data in DB
    db_insert_df_conflict_on_do_nothing(engine, df, Quote.__tablename__)

    # sleep so we don't overload IB and get throttled
    ib.sleep(2.02)


def execute_fetch(contracts, yyyymmdd, value_types):
    for contract in contracts:
        for value_type in value_types:
            logger.debug(
                f"Fetching {get_local_symbol(contract)} - {value_type} on {yyyymmdd}"
            )
            df = fetch_data(contract, yyyymmdd, value_type)
            save_df(contract, value_type, df)


for spread in qm_curve:
    yyyymmdd = "20201220"  # this is a hack
    # front and back contracts
    contracts = spread.m1, spread.m2
    value_types = INDIVIDUAL_CONTRACT_DATA_POINTS
    execute_fetch(contracts, yyyymmdd, value_types)
    # spread contract
    contracts = [spread.contract]
    value_types = SPREAD_CONTRACT_DATA_POINTS
    execute_fetch(contracts, yyyymmdd, value_types)
