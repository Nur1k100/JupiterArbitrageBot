import aiohttp
import json
import copy
import asyncio
import logging
from Configs import config

logging_path = 'logging/RequestDexTest.log'
logger = logging.getLogger('requestDEX')
logger.setLevel(logging.INFO)
handler = logging.FileHandler(logging_path)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.propagate = False

class RequestDEXError(Exception): pass

class RequestDEX:

    CONF = config.Config
    PROXY = CONF.PROXY
    HEADER_UNISWAP = CONF.HEADER
    URL_UNISWAP = CONF.URL_UNISWAP
    PAYLOAD_UNISWAP = CONF.PAYLOAD_UNISWAP
    BASE_URL_JUPITER = CONF.BASE_URL_JUPITER
    PAYLOAD_JUPITER = CONF.PAYLOAD_JUPITER
    PATH_Uniswap_tokens = {
        "Arbitrum": "configs/tokens_Arbitrum.json",
        "Avalanche": "configs/tokens_Avalanche.json",
        "Base": "configs/tokens_Base.json",
        "BNB": "configs/tokens_BNB Chain.json",
        "Celo": "configs/tokens_Celo.json",
        "Ethereum": "configs/tokens_Ethereum.json",
        "OP_Mainnet": "configs/tokens_OP Mainnet.json",
        "Polygon": "configs/tokens_Polygon.json",
    }
    PATH_Jupiter_tokens = 'configs/tokens_Jupiter.json'

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    #################################
    # General Methods Class provide #
    #################################

    """
        #### UNISWAP ####
    """
    async def request_Uniswap(self, chainId: int, amount: int, tokenIn: str, tokenOut: str, iteration=0) -> float or int or None:
        if iteration > 3:
            logger.info(f"Uniswap Run out of iterations information and payload: \ntokenIn: {tokenIn}\ntokenOut{tokenOut}\nchainId: {chainId}\namount: {amount}\nproxy: {self.PROXY}\n")
            return None

        is_validated, error = await self.__data_validation_Uniswap(tokenIn, tokenOut, chainId, amount)

        if not is_validated:
            logger.info(f"data was not validated Uniswap: {error} \n")
            return None

        payload = copy.deepcopy(self.PAYLOAD_UNISWAP)
        payload["amount"] = str(int(amount))
        payload["tokenIn"] = tokenIn
        payload["tokenOut"] = tokenOut
        payload["tokenInChainId"] = chainId
        payload["tokenOutChainId"] = chainId

        try:
            async with self.session.post(url=self.URL_UNISWAP, json=payload, timeout=15, proxy=self.PROXY,
                                         headers=self.HEADER_UNISWAP) as res:
                res.raise_for_status()
                data = await res.json()

                is_price_fetched, price = await self.__fetching_aggregatedPrice_Uniswap(data)
                if not is_price_fetched:
                    logger.info(f"Error with aggregatedPrice Uniswap: {price} \n")
                    return None
                else:
                    return price

        except (aiohttp.ServerTimeoutError, aiohttp.ClientConnectorError):
            return await self.request_Uniswap(tokenIn, tokenOut, chainId, amount, iteration + 1)
        except aiohttp.ClientProxyConnectionError:
            logger.info(f"Error with proxy connection Uniswap \n"
                        f"information and payload: \ntokenIn: {tokenIn}\ntokenOut{tokenOut}\nchainId: {chainId}\namount: {amount}\nproxy: {self.PROXY}\n")
            return await self.request_Uniswap(tokenIn, tokenOut, chainId, amount, iteration + 1)
        except aiohttp.ClientError as e:
            logger.info(f"Error with response Uniswap: {e} \n")
            return None
        except Exception as e:
            logger.info(f"Unexpected Error Uniswap: {e}")
            return None

    async def get_price_Uniswap(self) -> list or None:

        def creating_Uniswap_request_tasks() -> tuple[list, list]:
            w_context = []
            w_tasks = []
            for w_network, w_network_path in self.PATH_Uniswap_tokens.items():
                w_tokens_dict = self.__load_json(w_network_path)
                w_USDT_addr = w_tokens_dict.get("tokenOutUSDT")
                w_USD_decimal = w_tokens_dict.get("decimals")
                w_tokens_list = w_tokens_dict.get("tokens")
                w_chain_id = w_tokens_dict.get("chainId")

                for w_token in w_tokens_list:
                    w_tasks.append(self.__bid_ask_request_Uniswap(w_token.get("address"), w_USDT_addr, w_chain_id, w_token.get("decimals"), w_USD_decimal))
                    w_token["network"] = w_network
                    w_context.append(w_token)

            return w_context, w_tasks

        context, tasks = creating_Uniswap_request_tasks()

        bid_ask = await asyncio.gather(*tasks, return_exceptions=True)
        processed_results = []

        for i, res in enumerate(bid_ask):
            if isinstance(res, Exception) or res is None:
                logger.info(f"Error with bid Uniswap: {res} \n")
            else:
                context[i] = context[i] | res
                processed_results.append(context)

        return processed_results[0]

    """
        #### Jupiter ####
        * default token Out is USDT
    """

    async def request_Jupiter(self, amount: int, tokenIn: str,
                              tokenOut="Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", iteration=0,
                              formatedAsUSDT=False, formatDepToken=False, formatDepTokenDecimals=None) -> float or None:
        if iteration > 3:
            logger.info(f"Jupiter Run out of iterations information and payload: \ntokenIn: {tokenIn}\ntokenOut{tokenOut}\namount: {amount}\nproxy: {self.PROXY}\n")
            return None

        is_validated, error = await self.__data_validation_Jupiter(tokenIn, tokenOut, amount)

        if not is_validated:
            logger.info(f"data was not validated: {error} \n")
            return None

        payload = self.PAYLOAD_JUPITER[0] + tokenIn + self.PAYLOAD_JUPITER[1] + tokenOut + self.PAYLOAD_JUPITER[2] + str(int(amount))

        try:
            async with self.session.get(url=payload, proxy=self.PROXY, timeout=15) as res:
                res.raise_for_status()
                data = await res.json()

                is_price_fetched, price = await self.__fetching_price_Jupiter(data)
                if not is_price_fetched:
                    logger.info(f"Error Jupiter: {price} \n")
                    return None
                elif isinstance(price, (int, float)):

                    if formatedAsUSDT:
                        return price / 10 ** 6
                    if formatDepToken and formatDepTokenDecimals is not None:
                        return price / 10 ** formatDepTokenDecimals

                    return price
                else:
                    logger.info(f"Unexpected Error Jupiter: {price} \n")
                    return None

        except (aiohttp.ServerTimeoutError, aiohttp.ClientConnectorError, aiohttp.ClientResponseError):
            return await self.request_Jupiter(tokenIn, tokenOut, amount, iteration + 1)
        except aiohttp.ClientProxyConnectionError:
            logger.info(f"Error with proxy connection Jupiter \n"
                        f"information and payload: \ntokenIn: {tokenIn}\ntokenOut{tokenOut}\namount: {amount}\nproxy: {self.PROXY}\n")
            return await self.request_Jupiter(tokenIn, tokenOut, amount, iteration + 1)
        except aiohttp.ClientError as e:
            logger.info(f"Error with response Jupiter: {e} \n"
                        f"payload url: {payload}")
            return None
        except Exception as e:
            logger.info(f"Unexpected Error Jupiter: {e}"
                        f"\ntokenIn: {tokenIn}\ntokenOut{tokenOut}\namount: {str(int(amount))}\nproxy: {self.PROXY}\ntype of session{type(self.session)}\n"
                        f"session id: {id(self.session)}, closed: {self.session.closed}\n"
                        f"iteration: {iteration}\n")
            return None

    async def get_price_Jupiter(self) -> list or None:
        tokens_dict = self.__load_json(self.PATH_Jupiter_tokens)
        tokens_list = tokens_dict.get('tokens')
        USDT_token = tokens_dict.get('tokenOutUSDT')

        def creating_Jupiter_requests_tasks() -> tuple[list, list]:
            w_tasks = []
            w_context = []
            for w_item in tokens_list:
                w_tasks.append(self.request_Jupiter(
                    tokenIn=w_item.get("address"),
                    tokenOut=USDT_token,
                    amount=10 ** w_item.get("decimals")
                ))
                w_context.append(w_item)

            return w_tasks, w_context

        tasks, context = creating_Jupiter_requests_tasks()

        try:
            res = await asyncio.gather(*tasks, return_exceptions=True)
            processed_results = []

            for i, price in enumerate(res):
                if isinstance(price, Exception) or price is None:
                    logger.info(f"ERROR occurred during Jupiter request: {price} \n {context[i]}")
                else:
                    context[i]["price"] = price / 10 ** 6
                    processed_results.append(context[i])

            return processed_results
        except Exception as e:
            logger.info(f"Unexpected Error Jupiter: {e}")
            return None

    #################################
    #    Helper Methods of Class    #
    #################################

    async def __bid_ask_request_Uniswap(self, token_addr: str, USD_addr: str, chainId: int, token_decimal: int, USD_decimal: int) -> dict or None:
        is_validated, error = await self.__data_validation_Uniswap(token_addr, USD_addr, chainId, token_decimal)

        if not is_validated:
            logger.info(f"data was not validated Uniswap: {error} \n")
            return None

        bid = await self.request_Uniswap(tokenIn=token_addr, tokenOut=USD_addr, chainId=chainId, amount=10 ** token_decimal)

        if bid is None:
            logger.info(f"Error with bid Uniswap: check the data \n")
            return None

        ask_in_decimals = await self.request_Uniswap(tokenIn=USD_addr, tokenOut=token_addr, chainId=chainId, amount=bid)

        if ask_in_decimals is None:
            return {"bid": bid, "ask": ask_in_decimals}

        if not isinstance(ask_in_decimals, (int, float)) or ask_in_decimals < 0:
            return {"bid": bid, "ask": ask_in_decimals}

        bid = bid / 10 ** USD_decimal
        ask = bid / (ask_in_decimals / 10 ** token_decimal)

        return {"bid": bid, "ask": ask}

    #################################
    #    static method of Class     #
    #################################

    @staticmethod
    def __load_json(file_path) -> dict:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    """
        #### UNISWAP ####
    """

    @staticmethod
    async def __data_validation_Uniswap(*args) -> tuple[bool, str]:
        if len(args) != 4:
            return False, "Not enough arguments"

        tokenIn, tokenOut, chainId, amount = args

        if not isinstance(tokenIn, str):
            return False, f"tokenIn is not string {tokenIn}"
        elif not isinstance(tokenOut, str):
            return False, f"tokenOut is not string {tokenOut}"
        elif not isinstance(chainId, int):
            return False, f"chainId is not integer {chainId}"
        elif not isinstance(amount, int):
            return False, f"amount is not integer {amount}"
        else:
            if tokenIn == tokenOut:
                return False, "tokenIn and tokenOut are the same"

        return True, ""

    @staticmethod
    async def __fetching_aggregatedPrice_Uniswap(data: dict) -> tuple[bool, (int or str)]:
        if not isinstance(data, dict):
            return False, "data is not dict"

        aggregatedOutputs = data.get("quote", {}).get("aggregatedOutputs", [])
        if not aggregatedOutputs:
            return False, f"Check this data: {data}"

        price = aggregatedOutputs[0].get("amount", None)
        if not price:
            return False, f"Check this data: {data}"

        return True, int(price)


    """
        #### Jupiter ####
    """

    @staticmethod
    async def __data_validation_Jupiter(*args) -> tuple[bool, str]:
        if len(args) != 3:
            return False, "Not enough arguments"

        tokenIn, tokenOut, amount = args
        if not isinstance(tokenIn, str):
            return False, f"tokenIn is not string {tokenIn}"
        elif not isinstance(tokenOut, str):
            return False, f"tokenOut is not string {tokenOut}"
        elif not isinstance(amount, (int, float)):
            return False, f"amount is not integer or float {amount}"
        else:
            if tokenIn == tokenOut:
                return False, "tokenIn and tokenOut are the same"

        return True, ""

    @staticmethod
    async def __fetching_price_Jupiter(data: dict) -> tuple[bool, (int or str)]:
        if not isinstance(data, dict):
            return False, "data is not dict"
        price = data.get("outAmount")

        if not price:
            return False, f"Check this data: {data}"

        return True, float(price)


    #################################



async def main():
    session_ = aiohttp.ClientSession()
    request_ = RequestDEX(session_)
    print(request_.__repr__())

    quotes = [
        1,
        1152.3133749999984,
        3456.940124999995,
        4609.253499999993
    ]

    for quote in quotes:
        print(await request_.request_Jupiter(amount=quote*10**8, tokenIn="7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs", formatedAsUSDT=True))

    await session_.close()