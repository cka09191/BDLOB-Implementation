import time
import json

from bilob.utils import get_api_key, get_position
from binance.um_futures import UMFutures
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
from binance.error import ClientError

def message_handler(_, message):
    print(message)
    print(type(message))

api_key, api_secret = get_api_key()
um_futures_web_client = UMFuturesWebsocketClient(on_message=message_handler)
um_futures_client = UMFutures(key = api_key, secret = api_secret)

def test_partial_book():
    um_futures_web_client.partial_book_depth(symbol="BTCUSDC", level=20, speed=100)

    time.sleep(0.5)

    um_futures_web_client.stop()

def test_exchange_info():
    print(um_futures_client.exchange_info()['symbols'][0])
    #json_info = json.loads()


def test_get_balance():
    try:
        response = um_futures_client.balance()
        print(response[0])
        print(type(response))
    except ClientError as error:
        print(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            )
        )

def test_get_all_orders():
    response = um_futures_client.get_all_orders(symbol="BTCUSDC")
    print(response[0])
    print(type(response))


def test_get_all_positions():
    response = um_futures_client.get_position_risk(recvWindow=6000)
    print(response)
    print(type(response))

    amount = get_position(um_futures_client)
    print('position function')
    print(amount)
    print(type(amount))

def test_get_mark_price_klines():
    response = um_futures_client.mark_price_klines("BTCUSDC", "15m", limit= 500, endTime= int((time.time()-60*60*24*30*12)*1000))
    print('test_get_mark_price_klines')
    print(response[0])
    print(len(response))
    print(len(response[0]))
