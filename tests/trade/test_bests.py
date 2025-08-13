import numpy as np

from binance.error import ClientError
from bilob.utils import get_api_key
from bilob.model.strategy import strategy
from binance.um_futures import UMFutures

api_key, api_secret = get_api_key()




um_futures_client = UMFutures(key=api_key, secret=api_secret)
def test_current_order():
    try:
        #response = um_futures_client.change_leverage(
        #        symbol="BTCUSDC", leverage = 25, recvWindow=6000
        #    )
        #print(response)
        params = {
            "symbol": "BTCUSDC",
        }
        total_bid = 0
        total_ask = 0
#{'orderId': 12624990077, 'symbol': 'BTCUSDC', 'status': 'NEW', 'clientOrderId': 'LOavjyfmYv0FkzZrFSV3JN', 'price': '86550', 'avgPrice': '0', 'origQty': '0.001', 'executedQty': '0', 'cumQuote': '0.0000', 'timeInForce': 'GTC', 'type': 'LIMIT', 'reduceOnly': True, 'closePosition': False, 'side': 'SELL', 'positionSide': 'BOTH', 'stopPrice': '0', 'workingType': 'CONTRACT_PRICE', 'priceProtect': False, 'origType': 'LIMIT', 'priceMatch': 'NONE', 'selfTradePreventionMode': 'EXPIRE_MAKER', 'goodTillDate': 0, 'time': 1744619937832, 'updateTime': 1744619937832}]
        responses = um_futures_client.get_orders(**params)
        ask_best = 1000000
        bid_best = 0
        ask_best_response, bid_best_response = None, None
        for response in responses:
            if response['side']=='SELL':
                _best = min(ask_best, float(response['price']))
                if ask_best != _best:
                    ask_best_response = response
                    ask_best = _best
                total_ask += (float(response['origQty']) - float(response['executedQty']))*float(response['price'])
            if response['side']=='BUY':
                _best = max(bid_best, float(response['price']))
                if bid_best != _best:
                    bid_best_response = response
                    bid_best = _best

                total_bid += (float(response['origQty']) - float(response['executedQty']))*float(response['price'])

        print(f'total bid({total_bid:9.2f})')
        print(f'total ask({total_ask:9.2f})')
        print('ask best')
        print(ask_best_response)
        print('bid best')
        print(bid_best_response)
    except ClientError as error:
        print(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            )
        )


