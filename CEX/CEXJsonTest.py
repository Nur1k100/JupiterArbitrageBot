import aiofiles
import json
import logging
import asyncio
import time
from datetime import datetime

main_path = 'CEX/'
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, filename=f"{main_path}logging_cex/CEXJsonTest.log", format="%(asctime)s %(levelname)s %(message)s")


class Orderbook:
    MAIN_PATH_TO_ORDERBOOKS = f'{main_path}orderbooks/'
    MAIN_PATH_TO_NETWORKS = f'{main_path}all_exchange_fees_async_integratedXXXX.json'
    CEX_ORDERBOOKS = {
        "mexc": "mexc_orderbooks",
        "bingx": "bingx_orderbooks",
        "coinex": "coinex_orderbooks",
        "bybit": "bybit_orderbooks",
        "htx": "htx_orderbooks",
        "gateio": "gateio_orderbooks",
        'bitmart': 'bitmart_orderbooks',
        'kucoin': 'kucoin_orderbooks',
        # "bitget": "bitget_orderbooks",
    }


    async def get_orderbook_for_ticker(self, pair) -> dict or None:
        base, quote = self.__base_quote(pair)

        exception_token = ["Bonk", "Mog", "WEN", "CAT", "MUMU", "MAD", "USA", "GUAC"]

        if base is None or quote is None:
            return None

        ticker_file_path = f"{base}{quote}.json"
        ticker_across_cex = {"data": {}}
        max_amount_bids = 0
        max_amount_asks = 0
        max_sum_bids = 0
        max_sum_asks = 0
        for cex, path in self.CEX_ORDERBOOKS.items():
            ticker_json = await self.__load_json(f"{self.MAIN_PATH_TO_ORDERBOOKS}{path}/{ticker_file_path}")
            if ticker_json is None:
                continue

            bids = ticker_json.get('bids', [])
            asks = ticker_json.get('asks', [])

            if bids is None or asks is None:
                logger.info(f"Invalid ticker data for {cex}{base} {quote}")
                continue

            bids_amount, bids_sums = self.__calculate_max(bids)
            asks_amount, asks_sums = self.__calculate_max(asks)
            if max_amount_bids < bids_amount:
                max_amount_bids = bids_amount
                max_sum_bids = bids_sums
            if max_amount_asks < asks_amount:
                max_amount_asks = asks_amount
                max_sum_asks = asks_sums

            if cex in ["bingx"]:
                asks = asks[::-1]

            ticker_across_cex["data"][cex] = {"bids": bids, "asks": asks}

        ticker_across_cex["max_amount_bids"] = max_amount_bids
        ticker_across_cex["max_amount_asks"] = max_amount_asks
        ticker_across_cex["max_sum_bids"] = max_sum_bids
        ticker_across_cex["max_sum_asks"] = max_sum_asks
        ticker_across_cex['time'] = datetime.now().strftime("%H:%M:%S")
        max_point_bid = max(max_amount_asks, max_amount_bids)
        max_point_ask = max(max_sum_asks, max_sum_bids)

        if base in exception_token:
            ticker_across_cex["point_ask"] = [1, 5, 10, 100]
        else:
            ticker_across_cex["point_ask"] = [max_point_ask * 0.01, max_point_ask * 0.25, max_point_ask * 0.50, max_point_ask]

        ticker_across_cex["point_bid"] = [1, max_point_bid * 1/16, max_point_bid * 3/4, max_point_bid]
        ticker_across_cex["test_point_bid"] = max_point_bid * 14/16
        ticker_across_cex["test_point_ask"] = max_point_ask * 14/16

        return ticker_across_cex

    async def get_networks_inf(self, token: str, network: str, cex: str) -> tuple[bool, dict] or tuple[bool, bool] or tuple[bool, str]:
        CEX_net_inf_json = await self.__load_json(f"{self.MAIN_PATH_TO_NETWORKS}")
        print(f'token: {token} network: {network} cex: {cex}')

        if CEX_net_inf_json is None:
            return False, None

        token_net_inf = CEX_net_inf_json.get(cex.lower(), {}).get(token.upper(), {}).get('networks', None)
        if token_net_inf is not None:
            inf: dict = token_net_inf.get(network, None)
            if inf is None:
                return False, None
            return True, inf
        else:
            return False, None


    @staticmethod
    async def __load_json(file_path) -> dict or None:
        try:
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                json_data = await f.read()
            return json.loads(json_data)
        except FileNotFoundError:
            return None
        except Exception as e:
            logger.info(f"Unexpected error: {e}")
            return None

    @staticmethod
    def __base_quote(pair) -> tuple or None:
        if '/' not in pair:
            logger.info(f"Invalid pair format: {pair}")
            return None, None
        return tuple(pair.split('/'))

    @staticmethod
    def __calculate_max(data) -> tuple[float, float] or None:
        amounts = 0
        sums = 0

        for i, amount in enumerate(data):
            if sums + float(amount[1]) * float(amount[0]) < 120_000:
                amounts += float(amount[1])
                sums += float(amount[1]) * float(amount[0])
            else:
                break
        return amounts, sums

def get_info():
    module = input('Please enter module name | token OrderBook (o) | net info of token (n): ')
    cex_json = Orderbook()
    if module == 'o':
        symbol = input("Please enter your orderbook symbol: ").upper()
        ticker = asyncio.run(cex_json.get_orderbook_for_ticker(f"{symbol}/USDT"))
        print(json.dumps(ticker, indent=4))
    elif module == 'n':
        symbol = input("Please enter your orderbook symbol: ")
        cex_name = input("Please enter your CEX: ")
        network = input("Please enter your network: ")
        res = asyncio.run(cex_json.get_networks_inf(token=symbol, cex=cex_name, network=network))
        print(res)


if __name__ == "__main__":
    get_info()



