import json
import websockets
import requests
import re
import asyncio
import time

API_URL = 'https://api-cloud.bitmart.com'
API_SPOT_SYMBOLS_URL = '/spot/v1/symbols'
API_SPOT_SYMBOLS_BOOK_URL = '/spot/v1/symbols/book'

WS_URL = 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1'
WS_PUBLIC_SPOT_TRADE = 'spot/trade'
WS_PUBLIC_SPOT_DEPTH50 = 'spot/depth50'

SUBSCRIBE_MAX_SIZE = 10

def create_channel(channel, symbol):
    return f"{channel}:{symbol}"

def create_spot_subscribe_params(channels):
    return json.dumps({
        'op': 'subscribe',
        'args': channels
    })

def divide_chunks(l, n):
    # Divide list on chunks of length N
    for i in range(0, len(l), n):
        yield l[i:i + n]

async def wait_for_recv(websocket, time_out, url):
    k = 0
    while k < 10:
        # Recent trades print
        try:
            data = await asyncio.wait_for(websocket.recv(), timeout=time_out)
            dicted_data = eval(data)
            if url == WS_PUBLIC_SPOT_TRADE:
                if 'data' in dicted_data:
                    for i in dicted_data['data']:
                        if i['side'] == 'buy':
                            print('!', time.time(), "bitmart", i['symbol'].replace('_', "-"), 'B', i['price'], i['size'], end='\n')
                        else:
                            print('!', time.time(), "bitmart", i['symbol'].replace('_', "-"), 'S', i['price'], i['size'],
                                  end='\n')

            # Delta print
            else:
                for i in dicted_data['data']:
                    for j in range(2):
                        if j == 0:
                            print("$", i['ms_t'], 'bitmart', i['symbol'].replace('_', '-'), 'S',
                                  re.sub(r".$", "", ''.join(str(x[1]) + "@" + str(x[0])+"|" for x in i['asks']))
                                  , end='\n')
                        else:
                            print("$", i['ms_t'], 'bitmart', i['symbol'].replace('_', '-'), 'B',
                                  re.sub(r".$", "", ''.join(str(x[1]) + "@" + str(x[0])+"|" for x in i['bids']))
                                  , end='\n')
            if data:
                k+=1
        except asyncio.TimeoutError:
            continue
        except websockets.exceptions.ConnectionClosed as e:
            print('[websockets] ConnectionClosed')
            raise e

async def ws_get_info(symbols, url):
    # get info from delta and trades of pair by websocket
    channels = []
    for j in symbols:
        channels.append(create_channel(url, j))
    message = create_spot_subscribe_params(channels)
    try:
        async with websockets.connect(WS_URL, timeout=10, close_timeout=10) as ws:
            await ws.send(message=message)
            await wait_for_recv(ws, 10, url)
    except (OSError, asyncio.TimeoutError) as e:
        print("Error:",e)

async def rest_get_full_orderbook(symbol):
    # use rest api to get snapshot of pair and print it
    for i in symbol:
        try:
            response = requests.get(API_URL + API_SPOT_SYMBOLS_BOOK_URL + "?" + "symbol=" + i )
            if response.status_code == 200:
               data = response.json()['data']
               for j in range(3):
                    if j == 1:
                        print("$", time.time(), 'bitmart', i.replace("_", "-"), 'B',
                              re.sub(r".$", "", ''.join(str(x['amount']) + "@" + str(x['price']) + "|" for x in data['buys'])),
                              "R", end="\n")
                    elif j == 2:
                        print("$", time.time(), 'bitmart', i.replace("_", "-"), 'S',
                              re.sub(r".$", "", ''.join(str(x['amount']) + "@" + str(x['price']) + "|" for x in data['sells'])),
                              "R", end='\n')
        except:
            print("Error getting snapshot of:", i, "bitmart")

def main():
    # Get all pairs on exschange and break them on chunks
    response = requests.get(API_URL + API_SPOT_SYMBOLS_URL)
    symbols = response.json()['data']['symbols']
    chuncks_symbols = list(divide_chunks(symbols, SUBSCRIBE_MAX_SIZE))
    while True:
        for i in chuncks_symbols:
            asyncio.run(ws_get_info(i, WS_PUBLIC_SPOT_TRADE))
            asyncio.run(ws_get_info(i, WS_PUBLIC_SPOT_DEPTH50))
            asyncio.run(rest_get_full_orderbook(i))
            time.sleep(2)
main()