from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from ib_insync import util as ib_insync_util
from ib_insync import Contract
from loguru import logger
from typing import List

from ibdatafetcher.util import is_trading_day
from ibdatafetcher.security_master import InMemSecuritiesMaster
from ibdatafetcher.spreads import ActionType, FutureCalendarSpread, Exchange
from ibdatafetcher.ib_client import gen_ib_client, fetch_data
from ibdatafetcher.models import (
    Quote,
    gen_engine,
    init_db,
    db_insert_df_conflict_on_do_nothing,
    transform_rename_df_columns,
    clean_query,
    data_already_fetched,
)
from ibdatafetcher.spreads import (
    INDIVIDUAL_CONTRACT_DATA_POINTS,
    SPREAD_CONTRACT_DATA_POINTS,
)


def save_df(contract, value_type, df):
    if df is None:
        return

    # transform
    transform_rename_df_columns(df)

    # manually fill in data
    df["symbol"] = sec_master.get_symbol(contract)
    df["local_symbol"] = sec_master.get_local_symbol(contract)
    # df["con_id"] = get_cond_id(conttract)
    # TODO(weston) when we setup the securities master
    df["value_type"] = value_type
    df["rth"] = True

    # load / insert data in DB
    db_insert_df_conflict_on_do_nothing(engine, df, Quote.__tablename__)

    # sleep so we don't overload IB and get throttled
    ib.sleep(2.02)


def execute_fetch(contracts, yyyymmdd, value_types):
    for contract in contracts:
        for value_type in value_types:
            local_symbol = sec_master.get_local_symbol(contract)
            logger.debug(f"{yyyymmdd} -- {local_symbol} -- {value_type} -- fetching")
            df = fetch_data(ib, sec_master, engine, contract, yyyymmdd, value_type, rth=True)
            save_df(contract, value_type, df)


def gen_spread(symbol: str, front_exp: str, back_exp: str) -> FutureCalendarSpread:
    return FutureCalendarSpread(
        underlying_symbol=symbol,
        exchange=Exchange.NYMEX,
        action=ActionType.BUY,
        m1_expiry=front_exp,
        m2_expiry=back_exp,
    )


def gen_fm_bm_pairings(spread: int) -> List[str]:
    """
    spread in months. 1 -> 1 month spread
    """
    ys = ["2021"]
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


ib = gen_ib_client()
engine = gen_engine()
init_db(engine)
sec_master = InMemSecuritiesMaster()


if __name__ == "__main__":
    last_x_days = 20
    symbols = ["/CL", "/QM"]
    front_back_expirations = gen_fm_bm_pairings(1)
    calendar_spreads = []

    for symbol in symbols:
        for fm, bm in front_back_expirations:
            spread = gen_spread(symbol, fm, bm)
            spread.init_contracts(ib)
            sec_master.set_ref(spread.m1.conId, spread.m1.localSymbol)
            sec_master.set_ref(spread.m2.conId, spread.m2.localSymbol)
            calendar_spreads.append(spread)

    for fs in calendar_spreads:
        for i in range(1, last_x_days):
            ago = date.today() - relativedelta(days=i)
            yyyymmdd = ago.strftime("%Y%m%d")

            # --------------- front month and back month contracts
            contracts = fs.m1, fs.m2
            value_types = INDIVIDUAL_CONTRACT_DATA_POINTS
            execute_fetch(contracts, yyyymmdd, value_types)

            # --------------- spread contracts
            contracts = [fs.contract]
            value_types = SPREAD_CONTRACT_DATA_POINTS
            execute_fetch(contracts, yyyymmdd, value_types)
