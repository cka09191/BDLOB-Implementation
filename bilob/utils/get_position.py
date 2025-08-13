import time
import json


def get_position(um_futures_client):
    response = get_position_risk(um_futures_client)
    for position in response:
        if position['symbol'] == 'BTCUSDC':
            break
    else:
        return None

    position_amount = position['positionAmt']
    
    return float(position_amount)

def get_position_risk(um_futures_client):
    return um_futures_client.get_position_risk(recvWindow=6000)

