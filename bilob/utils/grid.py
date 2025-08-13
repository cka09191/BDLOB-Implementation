import numpy as np

from binance.error import ClientError
from . import get_api_key
from bilob.model.strategy import strategy
from binance.um_futures import UMFutures
import argparse

api_key, api_secret = get_api_key()




um_futures_client = UMFutures(key=api_key, secret=api_secret)
def bid():

    args = _parser_argument('bid').parse_args()
    _order_grid('BUY', args.quantity, args.pricefrom, args.priceto, args.step, args.reduceonly)
def ask():
    args = _parser_argument('ask').parse_args()
    _order_grid('SELL', args.quantity, args.pricefrom, args.priceto, args.step, args.reduceonly)

def _parser_argument(description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("quantity", type=float)
    parser.add_argument("pricefrom", type=int)
    parser.add_argument("priceto", type=int)
    parser.add_argument("step", type=int)
    parser.add_argument("--reduceonly", default=False, type=bool)
    return parser

def _order_grid(side, quantity, price_from, price_to, step, reduce_only):
    try:
        response = um_futures_client.change_leverage(
                symbol="BTCUSDC", leverage = 25, recvWindow=6000
            )
        print(response)
        params = {
            "symbol": "BTCUSDC",
            "side": side,
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": quantity,
            "price": 70000,
            "reduceOnly": reduce_only,
        }
        total = 0
        ranges = range(price_from, price_to, step) 
        for price in ranges:
            params['price'] = price
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


