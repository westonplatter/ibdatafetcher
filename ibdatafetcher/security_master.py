# external
from ib_insync import Contract

# internal
from ibdatafetcher.spreads import SecType


class InMemSecuritiesMaster:
    contract_reference = {}

    def __init__(self):
        pass

    def get_ref(self, con_id: int) -> str:
        return self.contract_reference[con_id]

    def set_ref(self, con_id: int, ref: str) -> None:
        self.contract_reference[con_id] = ref

    def get_symbol(self, contract: Contract):
        return contract.symbol

    def get_local_symbol(self, contract: Contract):
        # ONLY handles futures calendar spread at this time @TODO(weston) expand functionality
        if contract.secType == SecType.BAG.value:
            front = self.get_ref(contract.comboLegs[0].conId)
            back = self.get_ref(contract.comboLegs[1].conId)
            return f"{front}/{back}"
        else:
            return contract.localSymbol
