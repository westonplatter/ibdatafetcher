from ib_insync import Contract
from datetime import date


def is_trading_day(contract: Contract, __date: date) -> bool:
    """
    This only determines if market is open during RTH
    TODO(weston) ignore trading holidays with trading_calendars module
    """
    result = __date.weekday() < 5  # 4 = Friday
    # hack, skip thanksgiving
    if __date.strftime("%Y-%m-%d") in ["2020-11-26", "2020-12-25"]:
        return False
    return result
