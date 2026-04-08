import numpy as np
from DEX.requestDEX import RequestDEX
from CEX.CEXJsonTest import Orderbook
import logging
from scipy.optimize import curve_fit
import asyncio
import aiohttp
import time
import json
from datetime import datetime

logging_path = 'logging/swap_MC.log'
logger = logging.getLogger('SwapModelCalculator')
logger.setLevel(logging.INFO)
handler = logging.FileHandler(logging_path)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.propagate = False
logging.basicConfig(level=logging.INFO, filename="logging/swap_MC.log", format="%(asctime)s %(levelname)s %(message)s", force=True)


class SwapModelCalculator:
    @staticmethod
    def __load_data(path) -> dict or None:
        try:
            with open(path, "r") as f:
                data = json.load(f)
            return data
        except Exception as e:
            logger.info(f"Error loading data from {path}: {e}")
            return None

    @staticmethod
    async def __get_y_outputs(reqDEX, mod, points, token_addr,
                              token_decimals, USDT_token_addr="Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB") -> list:
        tasks = []
        if mod == "bid":
            for point in points:
                tasks.append(
                    reqDEX.request_Jupiter(tokenIn=token_addr,
                                           amount=int(point * 10 ** token_decimals),
                                           formatedAsUSDT=True)
                )

            return await asyncio.gather(*tasks, return_exceptions=True)
        elif mod == "ask":
            for point in points:
                tasks.append(
                    reqDEX.request_Jupiter(tokenIn=USDT_token_addr, tokenOut=token_addr,
                                           amount=int(point * 10 ** 6),
                                           formatDepToken=True, formatDepTokenDecimals=token_decimals)
                )

            return await asyncio.gather(*tasks, return_exceptions=True)
        else:
            logger.info(f"Error: Unknown mod: {mod}")
            return None

    @staticmethod
    def __get_quote(points_x, points_y) -> list:
        quote = []
        for i, point_y in enumerate(points_y):
            if point_y and not isinstance(point_y, Exception):
                quote.append((points_x[i], point_y))
            elif isinstance(point_y, Exception):
                logger.info(f"An error occurred during quote collection for {points_x[i]}: {point_y}")

        return quote

    @staticmethod
    def swap_model(x_in, X_eff, Y_eff) -> float:
        denominator = X_eff + x_in
        if np.any(denominator <= 1e-9):
            return np.inf
        return (Y_eff * x_in) / denominator

    def __find_x_eff_y_eff(self, quotes) -> tuple[float, float] or tuple[None, None]:
        if not quotes or len(quotes) < 3:
            logger.info(f"Error: At least three quote data points are required for prediction. Your quotes: {quotes}")
            return None, None

        x_data = np.array([q[0] for q in quotes])
        y_data = np.array([q[1] for q in quotes])

        try:
            bounds = ([0, 0], [np.inf, np.inf])
            params, _ = curve_fit(self.swap_model, x_data, y_data, bounds=bounds, maxfev=5000)
            X_eff, Y_eff = params
        except RuntimeError:
            logger.info("Error: curve_fit could not find optimal parameters.")
            return None, None
        except ValueError as e:
            logger.info(f"Error during curve_fit: {e}.")
            return None, None
        except Exception as e:
            logger.info(f"Unexpected error during curve_fit: {type(e).__name__} - {e}")
            return None, None

        if X_eff is None or Y_eff is None:
            logger.info("Error: Could not determine model parameters.")
            return None, None

        if X_eff <= 0:
            logger.info("Error: X_eff must be positive.")
            return None, None

        return X_eff, Y_eff


    """
    ###################################
    # X eff Y eff prediction for Bids #
    ###################################
    """
    async def __find_x_eff_y_eff_for_bid_pair(self, token, reqDEX, orderbook_json) -> tuple[float, float, float] or None:
        token_addr = token.get("address")
        token_symbol = token.get("symbol")
        token_decimals = token.get("decimals")
        pair = f"{token_symbol}/USDT"
        orderbook = await orderbook_json.get_orderbook_for_ticker(pair)

        if len(orderbook.get("data", {})) == 0:
            return None

        points = orderbook.get("point_bid")

        responses = await self.__get_y_outputs(reqDEX=reqDEX, mod="bid", points=points,
                                               token_addr=token_addr, token_decimals=token_decimals)

        quote = self.__get_quote(points_x=points, points_y=responses)

        X_eff, Y_eff = self.__find_x_eff_y_eff(quote)
        if X_eff is None or Y_eff is None:
            logger.info(f"Error: Could not determine model parameters for {pair}.")
            return None

        def get_price_per_token():
            first_point = quote[0]
            wrapped_price_per_token = None
            if first_point[0] == 1:
                wrapped_price_per_token = first_point[1]

            return wrapped_price_per_token
        price_per_token = get_price_per_token()

        return X_eff, Y_eff, price_per_token

    async def get_all_x_eff_y_eff_bid(self) -> dict:
        jupiter_data = self.__load_data("DEX/DEX_json/tokens_Jupiter.json")

        if jupiter_data is None:
            logger.info("Error: Could not load Jupiter data.")
            return None

        session = aiohttp.ClientSession()
        try:
            reqDEX = RequestDEX(session)
            orderbook_json = Orderbook()

            tokens_jupiter = jupiter_data.get("tokens")
            tasks_bids = []
            context = []
            for token in tokens_jupiter:
                tasks_bids.append(self.__find_x_eff_y_eff_for_bid_pair(token, reqDEX, orderbook_json))
                context.append(token)

            results_bids_asks = await asyncio.gather(*tasks_bids, return_exceptions=True)

        except Exception as e:
            logger.info(f"Unexpected Error: {e} {type(e).__name__}\n")
            return None
        finally:
            await session.close()

        processed_results = {}
        for i, res in enumerate(results_bids_asks):
            if res and not isinstance(res, Exception) and res is not None:
                context[i]["X_eff_bid"], context[i]["Y_eff_bid"], context[i]["price_per_token"] = res
                context[i]['time'] = datetime.now().strftime("%H:%M:%S")
                processed_results[context[i]["symbol"]] = context[i]
            elif isinstance(res, Exception):
                logger.info(f"An error occurred during X_eff/Y_eff calculation for {context[i]}: {res}")

        try:
            if len(processed_results) > 0:
                with open("DEX/DEX_json/x_eff_y_eff_bids.json", "w") as f:
                    json.dump(processed_results, f, indent=4)
                logger.info(f"Processed results saved to DEX/DEX_json/x_eff_y_eff_bids.json")
            else:
                logger.info(f"No results were found for processed_results results.")

        except FileNotFoundError:
            logger.info(f"No file named x_eff_y_eff_bids.json")
        except Exception as e:
            logger.info(f"Unexpected error during saving processed_results: {e}")

        return processed_results
    """#################################################################################################################"""

    """
    ###################################
    # X eff Y eff prediction for Asks #
    ###################################"""

    async def __find_x_eff_y_eff_for_ask_pair(self, token: dict, reqDEX, orderbook_json) -> tuple[float, float, float] or None:
        USDT_token_addr = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
        token_addr = token.get("address")
        token_symbol = token.get("symbol")
        token_decimals = token.get("decimals")
        pair = f"{token_symbol}/USDT"
        orderbook = await orderbook_json.get_orderbook_for_ticker(pair)

        if len(orderbook.get("data", {})) == 0:
            return None

        points = orderbook.get("point_ask")

        responses = await self.__get_y_outputs(reqDEX=reqDEX, mod="ask", points=points, token_addr=token_addr,
                                               token_decimals=token_decimals, USDT_token_addr=USDT_token_addr)

        quote = self.__get_quote(points_x=points, points_y=responses)

        X_eff, Y_eff = self.__find_x_eff_y_eff(quote)
        if X_eff is None or Y_eff is None:
            logger.info(f"Could not determine model parameters for {pair}.")
            return None
        price_per_token = await reqDEX.request_Jupiter(tokenIn=token_addr, amount=int(10 ** token_decimals), formatedAsUSDT=True)

        return X_eff, Y_eff, price_per_token

    async def get_all_x_eff_y_eff_ask(self):
        jupiter_data = self.__load_data("DEX/DEX_json/tokens_Jupiter.json")

        if jupiter_data is None:
            logger.info("Error: Could not load Jupiter data.")
            return None

        tokens_jupiter = jupiter_data.get("tokens")

        if len(tokens_jupiter) == 0:
            logger.info("Error: Could not load Jupiter data not found tokens list.")
            return None

        session = aiohttp.ClientSession()
        try:
            reqDEX = RequestDEX(session)
            orderbook_json = Orderbook()

            tasks_asks = []
            context = []
            for token in tokens_jupiter:
                tasks_asks.append(self.__find_x_eff_y_eff_for_ask_pair(token, reqDEX, orderbook_json))
                context.append(token)

            results_asks = await asyncio.gather(*tasks_asks, return_exceptions=True)
        except Exception as e:
            logger.info(f"Unexpected Error: {e} {type(e).__name__}\n")
            return None
        finally:
            await session.close()

        processed_results = {}
        for i, res in enumerate(results_asks):
            if res and not isinstance(res, Exception) and res is not None:
                context[i]["X_eff_ask"], context[i]["Y_eff_ask"], context[i]["price_per_token"] = res
                context[i]['time'] = datetime.now().strftime("%H:%M:%S")
                processed_results[context[i]["symbol"]] = context[i]
            elif isinstance(res, Exception):
                logger.info(f"An error occurred during X_eff/Y_eff calculation for {context[i]}: {res}")

        try:
            if len(processed_results) > 0:
                with open("DEX/DEX_json/x_eff_y_eff_ask.json", "w") as f:
                    json.dump(processed_results, f, indent=4)
                logger.info(f"Processed results saved to DEX/DEX_json/x_eff_y_eff_ask.json")
            else:
                logger.info(f"No results were found for processed_results) results.")

        except FileNotFoundError:
            logger.info(f"No file named x_eff_y_eff_ask.json")
        except Exception as e:
            logger.info(f"Unexpected error during saving processed_results: {e}")

        return processed_results
    """#################################################################################################################"""


    async def get_bids_and_asks_eff(self):
        start_time = time.time()

        tasks = [self.get_all_x_eff_y_eff_ask(), self.get_all_x_eff_y_eff_bid()]
        await asyncio.gather(*tasks)
        logger.info("X eff and Y eff calculated for all pairs in Bids and Asks forms.")

        end_time = time.time()
        logger.info(f"Time elapsed: {end_time - start_time:.2f} seconds")




def main_entry_swapMC():
    swap_model = SwapModelCalculator()
    while True:
        logger.info(f"----------- New run -----------")
        asyncio.run(swap_model.get_bids_and_asks_eff())
