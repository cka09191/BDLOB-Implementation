import numpy as np

from binance.error import ClientError
from bilob.utils import get_api_key
from bilob.model.strategy import strategy
from binance.um_futures import UMFutures

api_key, api_secret = get_api_key()




um_futures_client = UMFutures(key=api_key, secret=api_secret)
def test_leverage_and_order():
    try:
        response = um_futures_client.change_leverage(
                symbol="BTCUSDC", leverage = 25, recvWindow=6000
            )
        print(response)
        params = {
            "symbol": "BTCUSDC",
            "side": "SELL",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": 0.001,
            "price": 70000,
        }
        total = 0
        #ranges = list(range(82000, 83000,25)) + list(range(83000,84000,50)) + list(range(84000,84401,100))
        #ranges = list(range(84600, 86000, 100)) + list(range(86000,87000, 50)) + list(range(87000,88000,25))
        ranges = range(96290, 96325, 10)
        for price in ranges:
            params['side'] = 'SELL'
            params['quantity'] = 0.002
            params['price'] = price
#            params['reduceOnly'] = True
            response = um_futures_client.new_order(**params)
            print(response)
            total += params["quantity"]*price
        print(f'total:{total:01f}')
    except ClientError as error:
        print(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            )
        )


