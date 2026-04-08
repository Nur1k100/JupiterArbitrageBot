import asyncio
import aiohttp
from CEX.CEXJsonTest import Orderbook
from DEX.requestDEX import RequestDEX as reqDEX
from All_services.MSS_server.PWsC import PersistenceWebSocketsConnection
from Configs.config import Config
import numpy as np
import logging
import json


"""
#######################################
# logger setting                      #
#######################################
"""

logging_path = 'logging/scanner.log'
logger = logging.getLogger('Scanner')
logger.setLevel(logging.INFO)
handler = logging.FileHandler(logging_path)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.propagate = False

logging_path_message = 'logging/all_messages.log'
logger_message = logging.getLogger('MESSAGES')
logger_message.setLevel(logging.INFO)
handler_message = logging.FileHandler(logging_path_message)
formatter_message = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler_message.setFormatter(formatter_message)
logger_message.addHandler(handler_message)
logger_message.propagate = False

logging_not_in_same_net_path = 'logging/not_in_same_net.log'
logger_not_in_same_net = logging.getLogger('NOT_IN_SAME_NET')
logger_not_in_same_net.setLevel(logging.INFO)
handler_not_in_same_net = logging.FileHandler(logging_not_in_same_net_path)
handler_not_in_same_net.setFormatter(formatter)
logger_not_in_same_net.addHandler(handler_not_in_same_net)
logger_not_in_same_net.propagate = False

applicant_messages_path = 'logging/applicant_messages.log'
logger_applicant_messages = logging.getLogger('APPLICANT_MESSAGES')
logger_applicant_messages.setLevel(logging.INFO)
handler_applicant_messages = logging.FileHandler(applicant_messages_path)
handler_applicant_messages.setFormatter(formatter_message)
logger_applicant_messages.addHandler(handler_applicant_messages)
logger_applicant_messages.propagate = False

lost_connections_token_path = 'logging/lost_token_messages.log'
logger_lost_connections = logging.getLogger('LOST_CONNECTIONS')
logger_lost_connections.setLevel(logging.INFO)
handler_lost_connections = logging.FileHandler(lost_connections_token_path)
handler_lost_connections.setFormatter(formatter_message)
logger_lost_connections.addHandler(handler_lost_connections)
logger_lost_connections.propagate = False

"""
################################################################
"""


conf = Config()
pwsc = None
MINIMAL_PROFIT = conf.MINIMAL_PROFIT
MINIMAL_PRED_PROFIT = conf.MINIMAL_PRED_PROFIT
cex_fee = conf.CEX_FEE
CEX_LINKS = conf.CEX_LINKS
LOST_CONNECTIONS_CEX = conf.LOST_CONNECTION_CEX_DICT_PATH
MINIMAL_DIFF = conf.MINIMAL_DIFF
MAXIMAL_DIFF = conf.MAXIMAL_DIFF


"""
Swap model functions 
"""

def __swap_model(x_in, X_eff, Y_eff):
    denominator = X_eff + x_in
    if np.any(denominator <= 1e-9):
        return np.inf
    return (Y_eff * x_in) / denominator

def __swap_anti_model(y_in, X_eff, Y_eff):
    denominator = Y_eff - y_in
    if np.any(denominator <= 1e-9):
        return np.inf
    return (X_eff * y_in) / denominator

"""
#######################################
# Ticker fetcher functions and object #
#######################################
"""

def __load_json(file_path) -> dict or None:
    try:
        with open(file_path) as f:
            __data = json.load(f)
        return __data
    except Exception as e:
        return None

def __get_asks_DEX() -> dict or None:
    __asks_jupiter_json_path = "DEX/DEX_json/x_eff_y_eff_ask.json"
    return __load_json(__asks_jupiter_json_path)

def __get_bids_DEX() -> dict or None:
    __bids_jupiter_json_path = "DEX/DEX_json/x_eff_y_eff_bids.json"
    return __load_json(__bids_jupiter_json_path)

def __is_ticker_alive(symbol, cex) -> bool:
    path_to_lost_connections = LOST_CONNECTIONS_CEX.get(cex)
    if path_to_lost_connections is None:
        return True

    lost_connections_json= __load_json(path_to_lost_connections)
    if lost_connections_json is None:
        return True

    _is_coin_in_lost = lost_connections_json.get(symbol)
    if _is_coin_in_lost:
        return False
    else:
        return True

orderbook = Orderbook()     # asinc get_orderbook_for_ticker(pair)

"""
################################################################
"""




"""
########################################
# Arbitrage situation finder functions #
#  v1.0 Jupiter and CEX (DEX <-> CEX)  #    
########################################
"""

"""
________________________________
- CEX->DEX arbitrage functions -
________________________________
"""
async def find_all_cex_dex_arbitrage_situation(session: aiohttp.ClientSession):
    """
    Find all CEX->DEX arbitrage situations
    """
    dex_bids = __get_bids_DEX()

    tasks = []
    for symbol, dex_bid_data in dex_bids.items():
        tasks.append(__find_cex_dex_arbitrage_situation_for_pair(symbol, dex_bid_data, session))

    await asyncio.gather(*tasks)

"""
________________________________________________________________________________________________________
    HELPER FUNCTIONS
"""

async def __find_cex_dex_arbitrage_situation_for_pair(symbol: str, dex_bid_data: dict, session: aiohttp.ClientSession):
    ticker = f"{symbol}/USDT"
    orderbook_data = await orderbook.get_orderbook_for_ticker(ticker)
    time_of_orderbook = orderbook_data["time"]
    cex_data = orderbook_data.get("data", {})
    task = []

    if len(cex_data) == 0:
        logger.info(f"Error: No orderbook data for {ticker}")
        return

    for cex, cex_orderbook in cex_data.items():
        task.append(__find_cex_dex_arbitrage_situation(cex, cex_orderbook.get("asks"), dex_bid_data, session, time_of_orderbook))

    await asyncio.gather(*task)


async def __find_cex_dex_arbitrage_situation(cex: str, cex_ask_orderbook: dict, dex_bid_data: dict, session: aiohttp.ClientSession, time_of_orderbook):
    """
    cex:  bingx
    dex_bid_data: {'address': 'WENWENvqqNya429ubCdR81ZmD69brwQaaBYY6p3LCpk', 'symbol': 'WEN', 'decimals': 5, 'daily_volume': 202341.6920578665, 'X_eff_bid': 34822862567.77979, 'Y_eff_bid': 1747868.560445433, 'price_per_token': 5.1e-05}
    cex_ask_orderbook: [['0.5026', '320000'], ['0.5032', '99175'], ['0.5035', '1555602'], ['0.5038', '484484']]

    """
    dex_price_per_token = dex_bid_data.get("price_per_token")
    cex_price_per_token = float(cex_ask_orderbook[0][0])
    symbol = dex_bid_data.get("symbol")

    is_ticker_alive = __is_ticker_alive(symbol=symbol, cex=cex)
    if not is_ticker_alive:
        logger_lost_connections.info(f"(NOT){dex_bid_data.get('symbol')} is lost connections | CEX:{cex} | is_ticker_alive: {is_ticker_alive}")
        return

    if dex_price_per_token is None or cex_price_per_token is None:
        return

    diff = (dex_price_per_token - cex_price_per_token) / cex_price_per_token

    if diff < MINIMAL_DIFF or diff > MAXIMAL_DIFF:
        return

    current_amount = 0
    current_price = 0
    current_profit = -0.01
    for i, order in enumerate(cex_ask_orderbook):
        is_it_broke, current_amount, current_price, current_profit, is_it_profitable = await __find_cex_dex_for_per_order(
            order, dex_bid_data, current_amount, current_price, current_profit)

        if is_it_broke:
            break

    message = await __verify_and_create_message_cex_dex(
        is_it_profitable=is_it_profitable, current_amount=current_amount, dex_bid_data=dex_bid_data,
        current_price=current_price,
        dex_price_per_token=dex_price_per_token, cex_price_per_token=cex_price_per_token, i=i, cex=cex, symbol=symbol,
        session=session, time_of_orderbook=time_of_orderbook, predicted_profit=current_profit
    )

    if message is None or not message.get('is_it_in_one_network'):
        logger.info(f"Didn't send messages with context: {message}\n"
                    f"is_it_in_one_network: {message.get('is_it_in_one_network')} | is_ticker_aliveL: {is_ticker_alive}")
        return

    await __send_messages(message)


async def __find_cex_dex_for_per_order(order: list, dex_bid_data: dict,
                                       current_token_amount: float, current_price: float, current_profit: float) -> tuple[bool, float, float, float, bool]:
    cex_order_level_price = float(order[0])
    cex_order_level_amount = float(order[1])
    X_eff_bid = dex_bid_data.get("X_eff_bid")
    Y_eff_bid = dex_bid_data.get("Y_eff_bid")


    amount = current_token_amount
    price = current_price
    profit = current_profit

    previous_amount = None
    previous_price = None

    for i in range(1, 11):
        part_amount = cex_order_level_amount * i / 10
        part_price = part_amount * cex_order_level_price

        what_if_amount = amount + part_amount
        what_if_price = price + part_price

        dex_sum_for_amount = __swap_model(x_in=what_if_amount, X_eff=X_eff_bid, Y_eff=Y_eff_bid)

        what_if_profit = dex_sum_for_amount - what_if_price


        if what_if_profit > profit:
            profit = what_if_profit
            previous_amount = what_if_amount
            previous_price = what_if_price

            # if dex_bid_data.get("symbol") == "SSE":
            #     print(f"PROFIT")
            #     print("dex_sum_for_amount: ", dex_sum_for_amount)
            #     print(f"amount: {what_if_amount} price: {what_if_price} profit: {what_if_profit}")
            #     print(f"previous amount: {previous_amount} previous price: {previous_price} previous profit: {profit}")

        else:
            # if dex_bid_data.get("symbol") == "SSE":
            #     print(f"LOST")
            #     print("dex_sum_for_amount: ", dex_sum_for_amount)
            #     print(f"amount: {what_if_amount} price: {what_if_price} profit: {what_if_profit}")
            #     print(f"previous amount: {previous_amount} previous price: {previous_price} previous profit: {profit}")

            is_it_broke = True
            is_it_profitable = profit > 0

            if previous_amount and previous_price is not None:
                amount = previous_amount
                price = previous_price

            # if dex_bid_data.get("symbol") == "SSE":
            #     print(f"is_it_broke: {is_it_broke} is_it_profitable: {is_it_profitable}, amount: {amount}, price: {price}, profit: {profit}")
            return is_it_broke, amount, price, profit, is_it_profitable
    else:
        is_it_profitable = profit > 0
        is_it_broke = False
        amount += cex_order_level_amount
        price += cex_order_level_price * cex_order_level_amount

        # if dex_bid_data.get("symbol") == "SSE":
        #     print(f"is_it_broke: {is_it_broke} is_it_profitable: {is_it_profitable}, amount: {amount}, price: {price}, profit: {profit}")
        return is_it_broke, amount, price, profit, is_it_profitable


async def __verify_and_create_message_cex_dex(**kwargs) -> dict or None:
    is_it_profitable = kwargs['is_it_profitable']
    token_amount = kwargs['current_amount']
    dex_bid_data = kwargs['dex_bid_data']
    cex_sum_of_price = kwargs['current_price']
    dex_price_per_token = kwargs["dex_price_per_token"]
    cex_price_per_token = kwargs["cex_price_per_token"]
    orders = kwargs['i'] + 1
    cex = kwargs['cex']
    symbol = kwargs['symbol']
    session = kwargs['session']
    time_of_orderbook = kwargs['time_of_orderbook']
    predicted_profit = kwargs['predicted_profit']

    mod = 'CEX->DEX'
    cex_taker_fee = 1 + cex_fee.get(cex).get('taker')
    cex_sum_of_price = cex_sum_of_price * cex_taker_fee
    is_ticker_alive = __is_ticker_alive(symbol=symbol, cex=cex)
    _is_it_in_one_network, token_net_inf = await orderbook.get_networks_inf(token=symbol, cex=cex, network='Solana')

    if token_amount <= 0:
        return None

    if not is_it_profitable:
        return None

    if isinstance(predicted_profit, (float, int)) and predicted_profit < MINIMAL_PRED_PROFIT:
        return None

    if not is_ticker_alive:
        logger_lost_connections.info(f"(NOT){symbol} is lost connections | CEX:{cex} | is_ticker_alive: {is_ticker_alive}")
        return None

    logger_applicant_messages.info(f'''

    --- [APPLICANT] CEX->DEX Signal Analysis ---

    [IDENTIFIERS]
    - Symbol:               {symbol}
    - CEX:                  {cex}
    - DEX Address:          {dex_bid_data.get("address")}
    - Orderbook Time:       {time_of_orderbook}

    [INITIAL TRIGGER]
    - Initial CEX Ask:      {cex_price_per_token:.6f} USDT
    - Initial DEX Bid:      {dex_price_per_token:.6f} USDT
    - Initial Diff %:       {((dex_price_per_token / cex_price_per_token - 1) * 100):.4f}%

    [MODEL-BASED PREDICTION]
    - Profitable by Model?: {is_it_profitable}
    - Trade Size:           {token_amount:.4f} {symbol}
    - CEX Orders Consumed:  {orders}

    - CEX Taker Fee Rate:   {cex_fee.get(cex).get('taker')}
    - CEX Cost (post-fee):  {cex_sum_of_price:,.2f} USDT

    - Predicted DEX Revenue:{__swap_model(x_in=token_amount, X_eff=dex_bid_data['X_eff_bid'], Y_eff=dex_bid_data['Y_eff_bid']):,.2f} USDT
    - PREDICTED PROFIT:     {__swap_model(x_in=token_amount, X_eff=dex_bid_data['X_eff_bid'], Y_eff=dex_bid_data['Y_eff_bid']) - cex_sum_of_price:,.2f} USDT

    --- [END APPLICANT] ---
    ''')

    if _is_it_in_one_network and isinstance(token_net_inf, dict):
        depositEnabled = token_net_inf.get("depositEnabled")
        withdrawEnabled = token_net_inf.get("withdrawEnabled")
        withdrawFee = token_net_inf.get("withdrawFee")
        withdrawFeeUSD = token_net_inf.get("withdrawFeeUSD")
    else:
        if _is_it_in_one_network is False:
            logger_not_in_same_net.info(
                f'{symbol} is not in one network( cex: {cex} )\n token_net_inf: {token_net_inf} _is_it_in_one_network: {_is_it_in_one_network}')
        elif _is_it_in_one_network is None:
            logger_not_in_same_net.info(
                f'{symbol} is not in Network Information( cex: {cex} )\n token_net_inf: {token_net_inf} _is_it_in_one_network: {_is_it_in_one_network}')
        else:
            logger_not_in_same_net.info(
                f' Something else for {symbol} ( cex: {cex} )\n token_net_inf: {token_net_inf} _is_it_in_one_network: {_is_it_in_one_network}')
        return None

    if withdrawEnabled is False:
        logger_not_in_same_net.info(f'THERE IS NO WITHDRAW ENABLED for {symbol} ( cex: {cex} ) withdrawEnabled: withdrawEnabled')
        return None



    predicted_dex_sum_of_price = __swap_model(x_in=token_amount, X_eff=dex_bid_data['X_eff_bid'],
                                              Y_eff=dex_bid_data['Y_eff_bid'])

    real_dex_sum_of_price = await reqDEX(session).request_Jupiter(tokenIn=dex_bid_data.get("address"),
                                                                  amount=token_amount * 10 ** dex_bid_data.get(
                                                                      "decimals"),
                                                                  formatedAsUSDT=True)

    if real_dex_sum_of_price is None:
        logger.info(f'Could not verify this signal | mod: {mod} | cex: {cex} | symbol: {symbol} |\n'
                    f'predicted_dex_sum_of_price: {predicted_dex_sum_of_price} for amount {token_amount} {symbol}\n'
                    f'real_dex_sum_of_price: {real_dex_sum_of_price} for amount {token_amount} {symbol}')
        return None
    logger.info(f'Going to check real price for signal:\n'
                f'mod: {mod} | cex: {cex} | symbol: {symbol} | token_amount: {token_amount} | predDexSumPrice: {predicted_dex_sum_of_price}\n'
                f'_is_it_in_one_network: {_is_it_in_one_network} | Predicted Profit: {predicted_dex_sum_of_price - cex_sum_of_price} | Real Profit: {real_dex_sum_of_price - cex_sum_of_price}\n'
                f'SCANNER INFO: real_dex_sum_of_price: {real_dex_sum_of_price} and predicted_dex_sum_of_price: {predicted_dex_sum_of_price} for amount {token_amount} {symbol} {cex}')

    real_profit = real_dex_sum_of_price - cex_sum_of_price
    profit_in_percentage = ((real_dex_sum_of_price / cex_sum_of_price) - 1) * 100
    AVG_price_dex = real_dex_sum_of_price / token_amount
    AVG_price_cex = cex_sum_of_price / token_amount
    address = dex_bid_data.get("address")

    if real_profit < MINIMAL_PROFIT:
        return None

    message = get_message(
        mod=mod,
        symbol=symbol,
        address_dex=address,
        CEX=cex,
        _is_it_in_one_network=_is_it_in_one_network,
        token_net_inf=token_net_inf,
        depositEnabled=depositEnabled,
        withdrawEnabled=withdrawEnabled,
        withdrawFee=withdrawFee,
        withdrawFeeUSD=withdrawFeeUSD,
        dex_price_per_token=dex_price_per_token,
        cex_price_per_token=cex_price_per_token,
        AVG_price_dex=AVG_price_dex,
        AVG_price_cex=AVG_price_cex,
        volume_dex=real_dex_sum_of_price,
        volume_cex=cex_sum_of_price,
        profit=real_profit,
        profit_in_percentage=profit_in_percentage,
        orders=orders,
        token_amount=token_amount,
        time_of_orderbook=time_of_orderbook
    )

    return message



"""
________________________________________________________________________________________________________
"""





"""
________________________________
- DEX->CEX arbitrage functions -
________________________________
"""
async def find_all_dex_cex_arbitrage_situation(session: aiohttp.ClientSession):
    """
    Find all DEX->CEX arbitrage situations
    """
    dex_asks = __get_asks_DEX()


    tasks = []
    for symbol, dex_ask_data in dex_asks.items():
        tasks.append(__find_dex_cex_arbitrage_situation_for_pair(symbol, dex_ask_data, session))

    await asyncio.gather(*tasks)

"""
________________________________________________________________________________________________________
    HELPER FUNCTIONS
"""


async def __find_dex_cex_arbitrage_situation_for_pair(symbol: str, dex_ask_data: dict, session: aiohttp.ClientSession):
    ticker = f"{symbol}/USDT"
    orderbook_data = await orderbook.get_orderbook_for_ticker(ticker)
    time_of_orderbook = orderbook_data.get("time")
    cex_data = orderbook_data.get("data", {})
    task = []

    if len(cex_data) == 0:
        logger.info(f"Error: No orderbook data for {ticker}")
        return

    for cex, cex_orderbook in cex_data.items():
        task.append(__find_dex_cex_arbitrage_situation(cex, cex_orderbook.get("bids"), dex_ask_data, session, time_of_orderbook))

    await asyncio.gather(*task)


async def __find_dex_cex_arbitrage_situation(cex: str, cex_bid_orderbook: dict, dex_ask_data: dict, session: aiohttp.ClientSession, time_of_orderbook):

    """
    symbol: SOL
    cex: bingx
    cex_bid_orderbook: [['163.43', '405.667'], ['163.41', '576.033'], ['163.39', '957.205'], ['163.37', '1.224'], ['163.36', '479.207'], ['163.34', '474.068'], ['163.32', '548.776'], ['163.31', '0.612'], ['163.30', '559.911'], ['163.28', '383.711'], ['163.25', '566.526'], ['163.23', '409.924'], ['163.19', '432.368'], ['163.18', '728.383'], ['163.17', '801.643'], ['163.15', '1586.189'], ['163.12', '1391.109'], ['163.09', '438.342'], ['163.07', '298.764'], ['163.06', '372.325'], ['163.02', '840.366'], ['163.00', '124.937'], ['162.98', '275.802'], ['162.96', '60.779'], ['162.93', '435.964'], ['162.91', '70.929'], ['162.89', '0.015'], ['162.88', '667.393'], ['162.87', '0.056'], ['162.85', '0.028'], ['162.84', '0.014'], ['162.83', '0.005'], ['162.82', '240.351'], ['162.80', '0.213'], ['162.79', '61.590'], ['162.78', '0.001'], ['162.77', '0.007'], ['162.76', '586.442'], ['162.74', '0.010'], ['162.73', '661.742'], ['162.70', '117.039'], ['162.69', '0.023'], ['162.67', '28.369'], ['162.66', '0.005'], ['162.63', '510.829'], ['162.62', '0.066'], ['162.60', '0.044'], ['162.58', '0.001'], ['162.57', '435.278'], ['162.56', '679.972']]
    dex_ask_data: {'address': 'So11111111111111111111111111111111111111112', 'symbol': 'SOL', 'decimals': 9, 'daily_volume': 1611726302.767623, 'X_eff_ask': 700075644.0893548, 'Y_eff_ask': 4572754.938757299, 'price_per_token': 153.060673}
    """


    is_ticker_alive = __is_ticker_alive(symbol=dex_ask_data.get("symbol"), cex=cex)
    if not is_ticker_alive:
        logger_lost_connections.info(f"(NOT){dex_ask_data.get('symbol')} is lost connections | CEX:{cex} | is_ticker_alive: {is_ticker_alive}")
        return

    dex_price_per_token = dex_ask_data.get("price_per_token")
    cex_price_per_token = float(cex_bid_orderbook[0][0])
    if dex_price_per_token is None or cex_price_per_token is None:
        return

    diff = (cex_price_per_token - dex_price_per_token) / dex_price_per_token
    if diff < MINIMAL_DIFF or diff > MAXIMAL_DIFF:
        return

    current_amount = 0
    current_price = 0
    current_profit = -0.01
    orders = 0

    for i, order in enumerate(cex_bid_orderbook):
        orders += 1

        is_it_broke, current_amount, current_price, current_profit, is_it_profitable = await __find_dex_cex_for_per_order(
            order=order, dex_ask_data=dex_ask_data, current_amount=current_amount, current_price=current_price,
            current_profit=current_profit
        )

        # if dex_ask_data.get("symbol") == "GOONC":
        #     print('_'*100)
        #     print(f'After one order:\nis_it_broke: {is_it_broke}\nis_it_profitable: {is_it_profitable}\ncurrent_amount: {current_amount}\ncurrent_price: {current_price}\ncurrent_profit: {current_profit}')
        #     print('_'*100)

        if is_it_broke:
            break

    message = await __verify_and_create_message_dex_cex(
        dex_ask_data=dex_ask_data, current_amount=current_amount, cex=cex, dex_price_per_token=dex_price_per_token,
        session=session,
        cex_price_per_token=cex_price_per_token, orders=orders, cex_bid_orderbook=cex_bid_orderbook,
        predicted_profit=current_profit,
        current_price=current_price, time_of_orderbook=time_of_orderbook
    )

    if message is None or not message.get('is_it_in_one_network'):
        logger.info(f"Didn't send messages with context: {message}\n"
                    f"is_it_in_one_network: {message.get('is_it_in_one_network')} | is_ticker_aliveL: {is_ticker_alive}")
        return

    await __send_messages(message)

async def __find_dex_cex_for_per_order(order: list, dex_ask_data: dict,
                                       current_amount: float, current_price: float, current_profit: float) -> tuple[bool, float, float, float, bool]:
    cex_order_level_price = float(order[0])
    cex_order_level_amount = float(order[1])
    X_eff_ask = dex_ask_data["X_eff_ask"]
    Y_eff_ask = dex_ask_data["Y_eff_ask"]

    amount = current_amount
    price = current_price
    profit = current_profit

    previous_amount = None
    previous_price = None

    for i in range(1, 11):
        part_amount = cex_order_level_amount * i / 10
        part_price = part_amount * cex_order_level_price

        what_if_amount = amount + part_amount
        what_if_price = price + part_price

        dex_price_for_what_if_amount = __swap_anti_model(X_eff=X_eff_ask, Y_eff=Y_eff_ask, y_in=what_if_amount)

        what_if_profit = what_if_price - dex_price_for_what_if_amount

        if what_if_profit > profit:
            profit = what_if_profit
            previous_price = what_if_price
            previous_amount = what_if_amount

            # if dex_ask_data.get("symbol") == "GOONC":
            #     print(f'PROFIT')
            #     print(f'amount in current order: {cex_order_level_amount}, price for current order: {cex_order_level_price}')
            #     print(f'parent amount: {amount}, price: {price}, profit: {profit} ')
            #     print(f'current part amount: {part_amount}, price: {part_price}')
            #     print(f'what if amount: {what_if_amount}, price: {what_if_price}, profit: {what_if_profit}')
            #     print(f'dex price for what if amount: {dex_price_for_what_if_amount}')
            #     print('_' * 50)

        else:
            is_it_broken = True

            if previous_amount and previous_price is not None:
                amount = previous_amount
                price = previous_price

            # if dex_ask_data.get("symbol") == "GOONC":
            #     print(f'LOST')
            #     print(f'amount in current order: {cex_order_level_amount}, price for current order: {cex_order_level_price}')
            #     print(f'parent amount: {amount}, price: {price}, profit: {profit} ')
            #     print(f'current part amount: {part_amount}, price: {part_price}')
            #     print(f'what if amount: {what_if_amount}, price: {what_if_price}, profit: {what_if_profit}')
            #     print(f'dex price for what if amount: {dex_price_for_what_if_amount}')
            #     print('_' * 50)

            return is_it_broken, amount, price, profit, profit > 0
    else:
        is_it_broken = False
        amount += cex_order_level_amount
        price += cex_order_level_price * cex_order_level_amount

        return is_it_broken, amount, price, profit, profit > 0


async def __re_calculating_for_less_amount(orders, limit) -> tuple[float, float]:
    current_amount, current_price = 0, 0

    for order in orders:
        price = float(order[0])
        amount = float(order[1])

        if amount <= limit:
            current_amount += amount
            current_price += price * amount

            limit -= amount

            if limit == 0:
                break
        else:
            current_amount += limit
            current_price += limit * price

            break


    return current_amount, current_price


async def __verify_and_create_message_dex_cex(**kwargs) -> dict or None:
    dex_ask_data = kwargs.get('dex_ask_data')
    token_amount = kwargs.get('current_amount')
    cex = kwargs.get('cex')
    dex_price_per_token = kwargs.get('dex_price_per_token')
    cex_price_per_token = kwargs.get('cex_price_per_token')
    orders = kwargs.get('orders')
    cex_bid_orderbook = kwargs.get('cex_bid_orderbook')
    CEX_total_price = kwargs.get('current_price')
    session = kwargs.get('session')
    time_of_orderbook = kwargs.get('time_of_orderbook')
    predicted_profit = kwargs.get('predicted_profit')

    symbol = dex_ask_data.get('symbol')
    address = dex_ask_data.get('address')
    cex_taker_fee = 1 - cex_fee.get(cex).get('taker')
    mod = 'DEX->CEX'

    is_ticker_alive = __is_ticker_alive(symbol=symbol, cex=cex)
    _is_it_in_one_network, token_net_inf = await orderbook.get_networks_inf(token=symbol, cex=cex, network='Solana')

    dex_price_for_amount = __swap_anti_model(X_eff=dex_ask_data.get("X_eff_ask"),
                                             Y_eff=dex_ask_data.get("Y_eff_ask"), y_in=token_amount)

    deposit_enabled_status = token_net_inf.get("depositEnabled") if token_net_inf else "N/A"
    withdraw_enabled_status = token_net_inf.get("withdrawEnabled") if token_net_inf else "N/A"


    if isinstance(predicted_profit, (float, int)) and predicted_profit < MINIMAL_PRED_PROFIT:
        return None

    if not is_ticker_alive:
        logger_lost_connections.info(f"(NOT){symbol} is lost connections | CEX:{cex} | is_ticker_alive: {is_ticker_alive}")
        return None

    logger_applicant_messages.info(f'''

    --- [APPLICANT] DEX->CEX Signal Analysis ---

    [IDENTIFIERS]
    - Symbol:               {symbol}
    - CEX:                  {cex}
    - DEX Address:          {address}
    - Orderbook Time:       {time_of_orderbook}

    [INITIAL TRIGGER]
    - Initial DEX Ask:      {dex_price_per_token:.6f} USDT
    - Initial CEX Bid:      {cex_price_per_token:.6f} USDT
    - Initial Diff %:       {((cex_price_per_token / dex_price_per_token - 1) * 100):.4f}%

    [MODEL-BASED PREDICTION]
    - Trade Size:           {token_amount:.4f} {symbol}
    - CEX Orders Consumed:  {orders}

    - CEX Revenue (pre-fee):{CEX_total_price:,.2f} USDT
    - CEX Taker Fee Rate:   {cex_fee.get(cex).get('taker')}
    - CEX Revenue (post-fee):{CEX_total_price * cex_taker_fee:,.2f} USDT

    - Predicted DEX Cost:   {dex_price_for_amount:,.2f} USDT
    - PREDICTED PROFIT:     {CEX_total_price - dex_price_for_amount:,.2f} USDT

    [PRE-VALIDATION CHECKS]
    - Networks Match?:      {_is_it_in_one_network}
    - Deposit Enabled?:     {deposit_enabled_status}
    - Withdraw Enabled?:    {withdraw_enabled_status}
    - Full Network Info:    {token_net_inf}

    --- [END APPLICANT] ---
    ''')

    if _is_it_in_one_network and isinstance(token_net_inf, dict):
        depositEnabled = token_net_inf.get("depositEnabled")
        withdrawEnabled = token_net_inf.get("withdrawEnabled")
        withdrawFee = token_net_inf.get("withdrawFee")
        withdrawFeeUSD = token_net_inf.get("withdrawFeeUSD")
    else:
        if _is_it_in_one_network is False:
            logger_not_in_same_net.info(
                f'{symbol} is not in one network( cex: {cex} )\n token_net_inf: {token_net_inf} _is_it_in_one_network: {_is_it_in_one_network}')
        elif _is_it_in_one_network is None:
            logger_not_in_same_net.info(
                f'{symbol} is not in Network Information( cex: {cex} )\n token_net_inf: {token_net_inf} _is_it_in_one_network: {_is_it_in_one_network}')
        else:
            logger_not_in_same_net.info(
                f' Something else for {symbol} ( cex: {cex} )\n token_net_inf: {token_net_inf} _is_it_in_one_network: {_is_it_in_one_network}')
        return None

    if depositEnabled is False:
        logger_not_in_same_net.info(f'THERE IS NOT depositEnabled for symbol: {symbol} depositEnabled: {depositEnabled}')
        return None



    if dex_price_for_amount <= 0:
        logger.info(f'dex_price_for_amount is less than zero {dex_ask_data.get("address")} {dex_price_for_amount}')
        return None


    real_amount_of_token_for_dex = await reqDEX(session=session).request_Jupiter(
        amount=dex_price_for_amount * 10 ** 6,
        tokenIn="Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", tokenOut=dex_ask_data.get("address"),
        formatDepToken=True, formatDepTokenDecimals=dex_ask_data.get("decimals")
    )
    logger.info(f'Going to check real price for signal:\n'
                f'mod: {mod} | cex: {cex} | symbol: {symbol} | token_amount: {token_amount} | predDexSumPrice: {dex_price_for_amount}\n'
                f'_is_it_in_one_network: {_is_it_in_one_network} | Predicted Profit: {CEX_total_price - dex_price_for_amount}\n'
                f'SCANNER INFO: real_amount_of_token_for_dex: {real_amount_of_token_for_dex} and token_amount: {token_amount} for {dex_price_for_amount} USDT {cex}')

    if real_amount_of_token_for_dex is None:
        logger.info(f'real_amount_of_token_for_dex is None | address: {address} | symbol: {symbol} | token_amount: {token_amount} | cex: {cex}')
        return None


    if real_amount_of_token_for_dex < token_amount:
        token_amount, CEX_total_price = await __re_calculating_for_less_amount(cex_bid_orderbook,real_amount_of_token_for_dex)
        CEX_total_price = CEX_total_price * cex_taker_fee
        profit = CEX_total_price - dex_price_for_amount
        profit_in_percentage = ((CEX_total_price / dex_price_for_amount) - 1) * 100
        AVG_price_dex = dex_price_for_amount / token_amount
        AVG_price_cex = CEX_total_price / token_amount
    else:
        CEX_total_price = CEX_total_price * cex_taker_fee
        profit = CEX_total_price - dex_price_for_amount
        profit_in_percentage = ((CEX_total_price / dex_price_for_amount) - 1) * 100
        AVG_price_dex = dex_price_for_amount / real_amount_of_token_for_dex
        AVG_price_cex = CEX_total_price / token_amount

    if profit < MINIMAL_PROFIT:
        return None

    message = get_message(
        mod=mod,
        symbol=symbol,
        address_dex=address,
        CEX=cex,
        _is_it_in_one_network=_is_it_in_one_network,
        token_net_inf=token_net_inf,
        depositEnabled=depositEnabled,
        withdrawEnabled=withdrawEnabled,
        withdrawFee=withdrawFee,
        withdrawFeeUSD=withdrawFeeUSD,
        dex_price_per_token=dex_price_per_token,
        cex_price_per_token=cex_price_per_token,
        AVG_price_dex=AVG_price_dex,
        AVG_price_cex=AVG_price_cex,
        volume_dex=dex_price_for_amount,
        volume_cex=CEX_total_price,
        profit=profit,
        profit_in_percentage=profit_in_percentage,
        orders=orders,
        token_amount=token_amount,
        time_of_orderbook=time_of_orderbook
    )
    return message

"""
################################################################
"""

async def __send_messages(message: dict):
    if pwsc is not None:
        if not isinstance(message, dict):
            logger.info(f"Message must be a dict: {message}")
            return
        try:
            wrap_message = json.dumps(message)
        except Exception as exc:
            logger.info(f"During turning message into json raised exception {message} {exc}")
            return
        await pwsc.send(wrap_message)

def get_message(symbol, address_dex, CEX, _is_it_in_one_network, token_net_inf, depositEnabled, withdrawEnabled, withdrawFee,
                withdrawFeeUSD, dex_price_per_token, cex_price_per_token, AVG_price_dex, AVG_price_cex, volume_dex,
                volume_cex, profit, profit_in_percentage, orders, token_amount, mod, time_of_orderbook):

    """
    create messages
    :param symbol:
    :param address_dex:
    :param CEX:
    :param _is_it_in_one_network:
    :param token_net_inf:
    :param depositEnabled:
    :param withdrawEnabled:
    :param withdrawFee:
    :param withdrawFeeUSD:
    :param dex_price_per_token:
    :param cex_price_per_token:
    :param AVG_price_dex:
    :param AVG_price_cex:
    :param volume_dex:
    :param volume_cex:
    :param profit:
    :param profit_in_percentage:
    :param orders:
    :param token_amount:
    :param mod:
    :param time_of_orderbook:
    :return: message dict
    """

    if depositEnabled is None and withdrawEnabled is None and withdrawFee is None and withdrawFeeUSD is None:
        _is_it_in_one_network = None

    message = {
        'mod': mod,
        'symbol': symbol,
        'address_dex': address_dex,
        'CEX': CEX,
        'is_it_in_one_network': _is_it_in_one_network,
        'token_net_inf': token_net_inf,
        'depositEnabled': depositEnabled,
        'withdrawEnabled': withdrawEnabled,
        'withdrawFee': withdrawFee,
        'withdrawFeeUSD': withdrawFeeUSD,
        'dex_price_per_token': dex_price_per_token,
        'cex_price_per_token': cex_price_per_token,
        'AVG_price_dex': AVG_price_dex,
        'AVG_price_cex': AVG_price_cex,
        'volume_dex': volume_dex,
        'volume_cex': volume_cex,
        'profit': profit,
        'profit_in_percentage': profit_in_percentage,
        'orders': orders,
        'token_amount': token_amount,
    }

    logger_message.info(f'message: {message} \n order fetched time: {time_of_orderbook}')

    message['signal'] = signal_message_constructor(message)

    return message


def signal_message_constructor(message: dict) -> str:
    """Constructs a formatted arbitrage signal message for a Telegram bot."""
    mod, symbol, address_dex, cex_name = message.get('mod'), message.get('symbol', 'TOKEN').upper(), message.get(
        'address_dex'), message.get('CEX')

    is_one_network = message.get('is_it_in_one_network')
    deposit_enabled = message.get('depositEnabled')
    withdraw_enabled = message.get('withdrawEnabled')
    withdraw_fee_usd = message.get('withdrawFeeUSD')

    dex_price, cex_price, avg_price_dex, avg_price_cex = message.get('dex_price_per_token', 0), message.get(
        'cex_price_per_token', 0), message.get('AVG_price_dex', 0), message.get('AVG_price_cex', 0)
    volume_dex, volume_cex, profit, profit_percent = message.get('volume_dex', 0), message.get('volume_cex',
                                                                                               0), message.get('profit',
                                                                                                               0), message.get(
        'profit_in_percentage', 0)
    orders, token_amount = message.get('orders', 0), message.get('token_amount', 0)

    normalized_cex_name = cex_name.lower().strip()
    cex_data = CEX_LINKS.get(normalized_cex_name)
    if not cex_data: return f"Ошибка: Биржа '{cex_name}' не найдена в конфигурации."

    symbol_for_link = symbol.lower() if cex_data['symbol_case'] == 'lower' else symbol.upper()
    spot_link, deposit_link, withdraw_link = cex_data['spot_link'].format(symbol=symbol_for_link), cex_data[
        'deposit_link'], cex_data['withdraw_link']

    if mod == 'CEX->DEX':
        # Buy on CEX (🟢), Sell on DEX (🔴)
        source, destination, cex_icon, dex_icon = cex_name.lower(), 'jup', "🟢", "🔴"
        jup_link, total_volume_usdt = f"https://jup.ag/swap/{address_dex}-USDT", volume_cex
    elif mod == 'DEX->CEX':
        # Buy on DEX (🟢), Sell on CEX (🔴)
        source, destination, dex_icon, cex_icon = 'jup', cex_name.lower(), "🟢", "🔴"
        jup_link, total_volume_usdt = f"https://jup.ag/swap/USDT-{address_dex}", volume_dex
    else:
        return f"Ошибка: Неизвестный режим '{mod}'"

    solscan_link = f"https://solscan.io/token/{address_dex}"
    dexscreener_link = f"https://dexscreener.com/solana/{address_dex.lower()}"

    header = f"<b>{symbol}USDT: {source} → {destination} {total_volume_usdt:,.2f} +{profit:,.2f}$ ({profit_percent:.2f}%)</b>"
    token_info = f"{symbol}\n#{symbol}USDT"
    dex_block = (
        f'{dex_icon}|<a href="{jup_link}">JUP</a>|\n<code>{address_dex}</code>\n<a href="{solscan_link}">Сканер</a>\n'
        f"Цена: {dex_price:.4f} (Средняя: {avg_price_dex:.4f})\nОбъем: {volume_dex:,.2f} USDT ({token_amount:,.2f})")
    cex_display_name = cex_data['display_name']
    cex_block_header = f'{cex_icon}|<a href="{spot_link}">{cex_display_name}</a>|<a href="{deposit_link}">депозит</a>|<a href="{withdraw_link}">вывод</a>|'
    cex_block_body = (
        f"Цена: {cex_price:.4f} (Средняя: {avg_price_cex:.4f})\nОбъем: {volume_cex:,.2f} USDT ({token_amount:,.2f})\nОрдеров: {orders}")
    cex_block = f"{cex_block_header}\n{cex_block_body}"

    # Status checks for CEX
    depo_status = "✅" if deposit_enabled is True else "❌" if deposit_enabled is False else "❓"
    with_status = "✅" if withdraw_enabled is True else "❌" if withdraw_enabled is False else "❓"
    network_match_status = "✅" if is_one_network is True else "❌" if is_one_network is False else "❓"

    if withdraw_fee_usd is not None:
        fee_string = f"Комиссия сети (вывод): ~${withdraw_fee_usd:.2f}"
    else:
        fee_string = "Комиссия сети (вывод): ❓"

    fee_block = (f"Состояние CEX: Депозит: {depo_status} Вывод: {with_status}\nСети совпадают: {network_match_status}\n"
                 f"{fee_string}\n<b>Чистый спред: {profit:,.2f} USDT ({profit_percent:.2f}%)</b>")

    dexscreener_block = f'<a href="{dexscreener_link}">Dexscreener</a>'

    if mod == 'DEX->CEX':
        main_info_block = f"{dex_block}\n\n{cex_block}"
    elif mod == 'CEX->DEX':
        main_info_block = f"{cex_block}\n\n{dex_block}"
    else:
        main_info_block = f"{dex_block}\n\n{cex_block}"

    return f'{header}\n\n{token_info}\n\n{main_info_block}\n\n{fee_block}\n\n{dexscreener_block}'

# async def __check_suspicious_signal(predict, real):
#     pass




"""
################################################################
"""

async def __find_arbitrage_situation(session):
    task = [find_all_dex_cex_arbitrage_situation(session), find_all_cex_dex_arbitrage_situation(session)]
    res = await asyncio.gather(*task, return_exceptions=True)

async def __main():
    """
    run __find_arbitrage_situation()
    :return:
    """
    logger.info("--- New run(Scanner.py) ---")
    session = None
    global pwsc

    try:
        WEBSOCKET_URI = 'ws://localhost:8765'
        pwsc = PersistenceWebSocketsConnection(WEBSOCKET_URI)
        await pwsc.connect()

        session = aiohttp.ClientSession()
        while True:
            try:
                await asyncio.wait_for(__find_arbitrage_situation(session), timeout=30)
            except asyncio.TimeoutError:
                logger.warning("find_arbitrage_situation() timed out overall in main loop.")
            except Exception as e:
                logger.error(f"An error occurred in the main loop running find_arbitrage_situation: {e}")
    except Exception as e:
        logger.error(e)
    finally:
        if session is not None and not session.closed:
            logger.debug("Closing session...")
            await session.close()
            logger.debug("Session closed.")
        await pwsc.close()


def main_entry_scanner():
    """
    main entry point for scanner
    :return:
    """
    asyncio.run(__main())
