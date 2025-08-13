import numpy as np

from binance.error import ClientError
from bilob.utils import get_api_key
from bilob.model.strategy import strategy
from binance.um_futures import UMFutures

api_key, api_secret = get_api_key()

params = {
    "symbol": "BTCUSDC",
    "side": "BUY",
    "type": "LIMIT",
    "priceMatch": "QUEUE",
    "timeInForce": "GTC",
    "quantity": 0.002,
#    "price": 70000,
}


um_futures_client = UMFutures(key=api_key, secret=api_secret)
def note_leverage_and_order():
    try:
        response = um_futures_client.change_leverage(
                symbol="BTCUSDC", leverage = 25, recvWindow=6000
            )
        print(response)
        response = um_futures_client.new_order(**params)
        print(response)
    except ClientError as error:
        print(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            )
        )

#def test_strategy():
    ex_prediction = np.array([0.2,0.3,0.5])
    strategy(um_futures_client, ex_prediction)
    ex_prediction = np.array([0.5,0.3,0.2])
    strategy(um_futures_client, ex_prediction)
    ex_prediction = np.array([0.34,0.33,0.33])
    strategy(um_futures_client, ex_prediction)
    ex_prediction = np.array([0.4,0,0.6])

    strategy(um_futures_client, ex_prediction)


