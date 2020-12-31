from ib_insync import IB


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