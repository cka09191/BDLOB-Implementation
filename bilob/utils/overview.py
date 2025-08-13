import numpy as np

from binance.error import ClientError
from bilob.utils import get_api_key, get_position_risk
from bilob.model.strategy import strategy
from binance.um_futures import UMFutures

api_key, api_secret = get_api_key()




um_futures_client = UMFutures(key=api_key, secret=api_secret)
def overview():
    try:
        params = {
            "symbol": "BTCUSDC",
        }
        total_bid = 0
        total_ask = 0
#{'orderId': 12624990077, 'symbol': 'BTCUSDC', 'status': 'NEW', 'clientOrderId': 'LOavjyfmYv0FkzZrFSV3JN', 'price': '86550', 'avgPrice': '0', 'origQty': '0.001', 'executedQty': '0', 'cumQuote': '0.0000', 'timeInForce': 'GTC', 'type': 'LIMIT', 'reduceOnly': True, 'closePosition': False, 'side': 'SELL', 'positionSide': 'BOTH', 'stopPrice': '0', 'workingType': 'CONTRACT_PRICE', 'priceProtect': False, 'origType': 'LIMIT', 'priceMatch': 'NONE', 'selfTradePreventionMode': 'EXPIRE_MAKER', 'goodTillDate': 0, 'time': 1744619937832, 'updateTime': 1744619937832}]
#.[{'symbol': 'BTCUSDC', 'positionSide': 'BOTH', 'positionAmt': '1.898', 'entryPrice': '96101.39348709', 'breakEvenPrice': '96034.43983517', 'markPrice': '96165.98456108', 'unRealizedProfit': '122.59385843', 'liquidationPrice': '89500.73077950', 'isolatedMargin': '0', 'notional': '182523.03869692', 'marginAsset': 'USDC', 'isolatedWallet': '0', 'initialMargin': '7300.92154787', 'maintMargin': '862.61519348', 'positionInitialMargin': '7300.92154787', 'openOrderInitialMargin': '0', 'adl': 2, 'bidNotional': '0', 'askNotional': '17876.55000000', 'updateTime': 1746324331082}]

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

        print(f'total bid({total_bid:9.2f})') # bidNotional...
        print(f'total ask({total_ask:9.2f})')
        if ask_best_response:
            print('ask best')
            print(f"price({ask_best_response['price']}), qty({float(ask_best_response['origQty']) - float (ask_best_response['executedQty'])})")
        if bid_best_response:
            print('bid best')
            print(f"price({bid_best_response['price']}), qty({float(bid_best_response['origQty']) - float (bid_best_response['executedQty'])})")
        positions = get_position_risk(um_futures_client)
        for position in positions:
            if position['symbol'] == 'BTCUSDC':
                break
        else:
            return None
        print(f"positionAmt({position['positionAmt']}), entryPrice({position['entryPrice']}), liqudationPrice({position['liquidationPrice']})")

 
    except ClientError as error:
        print(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            )
        )


