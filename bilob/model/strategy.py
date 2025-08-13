import time
from enum import Enum
import numpy as np
from binance.error import ClientError
from bilob.utils.get_position import get_position
 
class CaseSignal(Enum):
    SELL = 0
    CLOSE_SELL = 1
    CLOSE_BUY = 2
    BUY = 3


def _signal_s(prediction):
    if not isinstance(prediction, np.ndarray):
        raise ValueError(f"type of parameter({type(prediction)}) is not np.ndarray")
    if not prediction.shape == (3,):
        raise ValueError(f"prediction shape({prediction.shape}) is not (3)")

    d, n, u = prediction

    s = (u-d)/max(0.2, n)/20
   
    if s > 0.002:
        case_signal = CaseSignal.BUY
    elif s > 0:
        case_signal = CaseSignal.CLOSE_BUY
    elif s > -0.002:
        case_signal = CaseSignal.CLOSE_SELL
    else:
        case_signal = CaseSignal.SELL

    return float(s), case_signal


def strategy(client, prediction) :
    client.cancel_open_orders(symbol="BTCUSDC", recvWindow=2000)
    position = get_position(client)
    amount, signal = _signal_s(prediction)
    print(f'amount({amount}), position({position})')
    if position != None:
        amount = abs(round((amount-position)/2, 3))
    else:
        amount = abs(round(amount, 3))
    match signal:
        case CaseSignal.BUY:
            reduce_only = False
            side = "BUY"
        case CaseSignal.SELL:
            side = "SELL"
            reduce_only = False
        case CaseSignal.CLOSE_BUY:
            reduce_only = True
            side = "BUY"
        case CaseSignal.CLOSE_SELL:
            reduce_only = True
            side = "SELL"

    params = {
        "symbol": "BTCUSDC",
        "side": side,
        "type": "LIMIT",
        "priceMatch": "QUEUE",
        "timeInForce": "GTD",
        "quantity": amount,
        "reduceOnly":'true' if reduce_only else "false",
        "goodTillDate":int((time.time()+650)*1000),
    }
    print(f'side({side}), amount({amount}), reduceOnly({reduce_only})')

    client.change_leverage(
                symbol="BTCUSDC", leverage = 25, recvWindow=6000
    )
    
    try:
        response = client.new_order(**params)
        print(f"amount:{response['origQty']}, side:{response['side']}, reduce only:{response['reduceOnly']}")
    except ClientError as error:
        match error.error_code:
#            case -1111:
#                pass
#            case -2022:
#                pass
#            case -4003:
#                pass
            case default:
                print(
                    "Found error. status: {}, error code: {}, error message: {}".format(
                        error.status_code, error.error_code, error.error_message
                    )
                )

 

#    print(response)
#{'orderId': 12384717232, 'symbol': 'BTCUSDC', 'status': 'NEW', 'clientOrderId': 'Vk5VDd3Er5MLR8dwBPtdzv', 'price': '81194.7', 'avgPrice': '0.00', 'origQty': '0.005', 'executedQty': '0.000', 'cumQty': '0.000', 'cumQuote': '0.0000', 'timeInForce': 'GTD', 'type': 'LIMIT', 'reduceOnly': False, 'closePosition': False, 'side': 'SELL', 'positionSide': 'BOTH', 'stopPrice': '0.0', 'workingType': 'CONTRACT_PRICE', 'priceProtect': False, 'origType': 'LIMIT', 'priceMatch': 'QUEUE', 'selfTradePreventionMode': 'EXPIRE_MAKER', 'goodTillDate': 1744361139000, 'updateTime': 1744360489193}
#binance.error.ClientError: (400, -1111, 'Precision is over the maximum defined for this asset.', {'Date': 'Fri, 11 Apr 2025 08:34:49 GMT', 'Content-Type': 'application/json', 'Content-Length': '76', 'Connection': 'keep-alive', 'Server': 'Tengine', 'x-mbx-used-weight-1m': '-1', 'x-mbx-order-count-10s': '3', 'x-mbx-order-count-1m': '3', 'x-response-time': '0ms', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS'})
#binance.error.ClientError: (400, -2022, 'ReduceOnly Order is rejected.', {'Date': 'Fri, 11 Apr 2025 08:30:31 GMT', 'Content-Type': 'application/json', 'Content-Length': '52', 'Connection': 'keep-alive', 'Server': 'Tengine', 'x-mbx-used-weight-1m': '-1', 'x-mbx-order-count-10s': '1', 'x-mbx-order-count-1m': '1', 'x-response-time': '1ms', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS'})
