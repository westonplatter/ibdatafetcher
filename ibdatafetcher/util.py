from spreads import SecType


def get_symbol(contract):
    return contract.symbol


def get_local_symbol(contract):
    if contract.secType == SecType.BAG.value:
        front = get_contract_reference(contract.comboLegs[0].conId)
        back = get_contract_reference(contract.comboLegs[1].conId)
        return f"{front}/{back}"
    else:
        return contract.localSymbol