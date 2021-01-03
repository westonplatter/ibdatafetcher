# external
from datetime import datetime
from ib_insync import IB
from ib_insync import util as ib_insync_util
from loguru import logger

# internal
from ibdatafetcher.util import is_trading_day
from ibdatafetcher.models import data_already_fetched

# IB default configs
IB_CLIENT_HOST_IP = "127.0.0.1"
IB_CLIENT_PORT = 4001
IB_CLIENT_ID = 1


def gen_ib_client(
    ip: str = IB_CLIENT_HOST_IP,
    port: int = IB_CLIENT_PORT,
    client_id: int = IB_CLIENT_ID,
):
    """
    Initialize and connect IB Client
    """
    ib = IB()
    ib.connect(ip, port, client_id)
    return ib


def fetch_data(ib, sm, engine, contract, yyyymmdd: str, value_type: str, rth: bool):
    __date = datetime.strptime(yyyymmdd, "%Y%m%d")
    local_symbol = sm.get_local_symbol(contract)

    if data_already_fetched(engine, local_symbol, value_type, __date):
        msg = f"{yyyymmdd} -- {local_symbol} -- {value_type} -- already fetched"
        logger.debug(msg)
        return

    if not is_trading_day(contract, __date):
        msg = f"{yyyymmdd} -- {local_symbol} -- {value_type} -- not during rth"
        logger.debug(msg)
        return
s
    bars = ib.reqHistoricalData(
        contract,
        endDateTime=f"{yyyymmdd} 23:59:59",
        durationStr="1 D",
        barSizeSetting="1 min",
        whatToShow=value_type,
        useRTH=rth,
        # formatDate=2,  # TODO(weston) return as UTC time
    )
    df = ib_insync_util.df(bars)
    return df
