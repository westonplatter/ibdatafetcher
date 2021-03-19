from datetime import date
from dateutil.relativedelta import relativedelta
from ib_insync import Future
from loguru import logger

from ibdatafetcher.security_master import InMemSecuritiesMaster
from ibdatafetcher.spreads import Exchange
from ibdatafetcher.ib_client import gen_ib_client, fetch_data
from ibdatafetcher.models import (
    Quote,
    gen_engine,
    init_db,
    db_insert_df_conflict_on_do_nothing,
    transform_rename_df_columns,
)
from ibdatafetcher.spreads import INDIVIDUAL_CONTRACT_DATA_POINTS


def save_df(sm, contract, value_type, df):
    if df is None:
        return

    # transform
    transform_rename_df_columns(df)

    # manually fill in data
    df["symbol"] = sm.get_symbol(contract)
    df["local_symbol"] = sm.get_local_symbol(contract)
    df["con_id"] = contract.conId
    df["value_type"] = value_type
    df["rth"] = True

    # load / insert data in DB
    db_insert_df_conflict_on_do_nothing(engine, df, Quote.__tablename__)

    # sleep so we don't overload IB and get throttled
    ib.sleep(2.02)


def execute_fetch(sm, contracts, yyyymmdd, value_types):
    for contract in contracts:
        for value_type in value_types:
            local_symbol = sec_master.get_local_symbol(contract)
            logger.debug(f"{yyyymmdd} - {local_symbol} - {value_type} - fetch")
            df = fetch_data(
                ib, sec_master, engine, contract, yyyymmdd, value_type, rth=True
            )
            save_df(sm, contract, value_type, df)


def gen_contract(symbol, exp) -> Future:
    return Future(
        symbol=symbol,
        lastTradeDateOrContractMonth=exp,
        exchange=Exchange.GLOBEX.value,
    )


def init_contracts(symbols, exps):
    contracts = []
    for symbol in symbols:
        for ex in expirations:
            contract = gen_contract(symbol, ex)
            contracts.append(contract)
    return contracts


def register_contracts_with_sec_master(sm, contracts):
    for x in contracts:
        sm.set_ref(x.conId, x.localSymbol)


ib = gen_ib_client()
engine = gen_engine()
init_db(engine)
sec_master = InMemSecuritiesMaster()

if __name__ == "__main__":
    last_x_days = 20
    symbols = ["/MES", "/M2K", "/MNQ"]
    expirations = ["202106", "202103"]

    contracts = init_contracts(symbols, expirations)
    contracts = ib.qualifyContracts(contracts)
    register_contracts_with_sec_master(sec_master, contracts)

    for i in range(1, last_x_days):
        ago = date.today() - relativedelta(days=i)
        yyyymmdd = ago.strftime("%Y%m%d")
        execute_fetch(sec_master, contracts, yyyymmdd, INDIVIDUAL_CONTRACT_DATA_POINTS)
