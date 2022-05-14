
# %%

from binance.futures import Futures as Client
from binance.lib.utils import config_logging
from binance.error import ClientError
import time
# import json
import jsonlines as jl
import os
from binance import ThreadedWebsocketManager

api_key = 'api_key'
api_secret = 'api_secret'

# %%
symbol = "ETHUSDT"

# %%

def main(symbol, obfile, klfile):
    
    symbol = symbol.lower()
    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
    # start is required to initialise its internal loop
    
    twm.start()
    

    def handle_socket_message(msg):

        etype = msg['e']

        if etype == "kline":
            # with open(klfile, 'a') as f:
            with jl.open(klfile, "a") as writer:
                writer.write(msg)
        elif etype == "depthUpdate":
            with jl.open(obfile, "a") as writer:
                writer.write(msg)



    twm.start_kline_socket(callback=handle_socket_message, symbol=symbol)

    # multiple sockets can be started
    twm.start_depth_socket(callback=handle_socket_message, symbol=symbol)

    # or a multiplex socket can be started like this
    # see Binance docs for stream names
    # streams = ['bnbbtc@bookTicker']
    streams = [f'{symbol}@depth20@500ms']
    twm.start_multiplex_socket(callback=handle_socket_message, streams=streams)

    twm.join()


# %%

if __name__ == "__main__":
    t0 = time.time()
    wdir = os.getcwd()
    path = os.path.join(wdir, 'data')
    if not os.path.exists(path):
        os.mkdir(path)
    path = os.path.join(path, symbol)
    if not os.path.exists(path):        
        os.mkdir(path)
    orderbook_file = os.path.join(path, f"{t0}_orderbook.jsonl")
    klines_file = os.path.join(path, f"{t0}_klines.jsonl")
    # obdata = []
    # kldata = []
    # with open(orderbook_file, 'w') as f:
    #     json.dump(obdata, f)

    # with open(klines_file, 'w') as f:
    #     json.dump(kldata, f)

    main(symbol, orderbook_file, klines_file)
#%%

def load_order_data(file):
    data = []

    with jl.open(file) as reader:
        for obj in reader:
            data.append(obj)
    return data
#%%
obpath = "data\\ETHUSDT\\1637117370.3067677_orderbook.jsonl"
klpath = "data\\ETHUSDT\\1637117370.3067677_klines.jsonl"
#%%
obdata = load_order_data(obpath)

# %%
len(obdata)
#%%
kldata = load_order_data(klpath)
#%%
len(kldata)
#%%
