class Config(object):
    HEADER = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-GB,en;q=0.9,ru;q=0.8",
        "Content-Type": "application/json",
        "Origin": "https://app.uniswap.org",
        "Referer": "https://app.uniswap.org/",
        "Sec-CH-UA": '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": '"macOS"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/133.0.0.0 Safari/537.36",
        "x-api-key": "JoyCGj29tT4pymvhaGciK4r1aIPvqW6W53xT1fwo",
        "x-app-version": "",
        "x-request-source": "uniswap-web",
        "x-universal-router-version": "2.0"
    }

    PAYLOAD_UNISWAP = {
                        "amount": None,
                        "autoSlippage": "DEFAULT",
                        "gasStrategies": [
                            {
                                "limitInflationFactor": 1.15,
                                "displayLimitInflationFactor": 1,
                                "priceInflationFactor": 1.5,
                                "baseFeeHistoryWindow": 100,
                                "baseFeeMultiplier": 1.05,
                                "maxPriorityFeeGwei": 9,
                                "minPriorityFeeGwei": 2,
                                "percentileThresholdFor1559Fee": 75,
                                "thresholdToInflateLastBlockBaseFee": 0
                            }
                        ],
                        "protocols": [
                            "V4",
                            "V3",
                            "V2"
                        ],
                        "swapper": "0xAAAA44272dc658575Ba38f43C438447dDED45358",
                        "tokenIn": None,
                        "tokenInChainId": None,
                        "tokenOut": None,
                        "tokenOutChainId": None,
                        "type": "EXACT_INPUT",
                        "urgency": "normal"
                    }

    PAYLOAD_JUPITER = ["https://ultra-api.jup.ag/order?inputMint=", "&outputMint=", "&amount="]

    PROXY = "http://customer--cc-US:@pr.oxylabs.io:7777"

    URL_UNISWAP = "https://trading-api-labs.interface.gateway.uniswap.org/v1/quote"

    BASE_URL_JUPITER = "https://quote-api.jup.ag/v6/quote"

    USER_FILE = "user.json"

    API_TOKEN = '7802133038:AAEEt62Rlb2OeDz7C6X2NaiO0_QLMvLEZts'

    MINIMAL_PROFIT = 20
    MINIMAL_PRED_PROFIT = 10
    MINIMAL_DIFF = 0.005
    MAXIMAL_DIFF = 0.5

    CEX_FEE = {
        "bybit": {
            "maker": 0.001,
            "taker": 0.001,
        },
        "gateio": {
            "maker": 0.001,
            "taker": 0.001,
        },
        "mexc": {
            "maker": 0.0,
            "taker": 0.0005,
        },
        "htx": {
            "maker": 0.0001,
            "taker": 0.002,
        },
        "coinex": {
            "maker": 0.002,
            "taker": 0.002,
        },
        "bingx": {
            "maker": 0.001,
            "taker": 0.001,
        },
        'bitmart': {
            "maker": 0.006,
            "taker": 0.006,
        },
        'lbank': {
            "maker": 0.001,
            "taker": 0.001,
        },
        'kucoin':{
            "maker": 0.003,
            "taker": 0.003,
        }
    }

    CEX_LINKS = {
        'bingx': {'display_name': 'BINGX', 'spot_link': "https://bingx.com/ru-ru/spot/{symbol}USDT/",
                  'deposit_link': "https://bingx.com/ru-ru/assets/recharge/",
                  'withdraw_link': "https://bingx.com/ru-ru/assets/withdraw/", 'symbol_case': 'upper'},
        'gateio': {'display_name': 'GATE.IO', 'spot_link': "https://www.gate.com/trade/{symbol}_USDT",
                   'deposit_link': "https://www.gate.com/myaccount/deposit/USDT",
                   'withdraw_link': "https://www.gate.com/myaccount/withdraw/usdt", 'symbol_case': 'upper'},
        'kucoin': {'display_name': 'KUCOIN', 'spot_link': "https://www.kucoin.com/ru/trade/{symbol}-USDT",
                   'deposit_link': "https://www.kucoin.com/ru/assets/coin/",
                   'withdraw_link': "https://www.kucoin.com/ru/assets/withdraw/", 'symbol_case': 'upper'},
        'htx': {'display_name': 'HTX', 'spot_link': "https://www.htx.com/trade/{symbol}_usdt?type=spot",
                'deposit_link': "https://www.htx.com/en-us/finance/deposit/usdt",
                'withdraw_link': "https://www.htx.com/en-us/finance/withdraw/usdt", 'symbol_case': 'lower'},
        'bybit': {'display_name': 'BYBIT', 'spot_link': "https://www.bybit.com/en/trade/spot/{symbol}/USDT",
                  'deposit_link': "https://www.bybit.com/user/assets/deposit",
                  'withdraw_link': "https://www.bybit.com/user/assets/withdraw", 'symbol_case': 'upper'},
        'bitmart': {'display_name': 'BITMART',
                    'spot_link': "https://www.bitmart.com/trade/ru-RU?symbol={symbol}_USDT&type=spot",
                    'deposit_link': "https://www.bitmart.com/asset-deposit/ru-RU",
                    'withdraw_link': "https://www.bitmart.com/asset-withdrawal/ru-RU", 'symbol_case': 'upper'},
        'mexc': {'display_name': 'MEXC', 'spot_link': "https://www.mexc.com/ru-RU/exchange/{symbol}_USDT",
                 'deposit_link': "https://www.mexc.com/ru-RU/assets/deposit/USDT",
                 'withdraw_link': "https://www.mexc.com/ru-RU/assets/withdraw/USDT", 'symbol_case': 'upper'},
        'lbank': {'display_name': 'LBANK', 'spot_link': "https://www.lbank.com/trade/{symbol}_usdt",
                  'deposit_link': "https://www.lbank.com/wallet/account/main/deposit/crypto",
                  'withdraw_link': "https://www.lbank.com/wallet/account/main/withdrawal/crypto",
                  'symbol_case': 'lower'},
        'coinex': {'display_name': 'COINEX', 'spot_link': "https://www.coinex.com/ru/exchange/{symbol}-usdt#spot",
                   'deposit_link': "https://www.coinex.com/ru/asset/deposit?type=BTC",
                   'withdraw_link': "https://www.coinex.com/ru/asset/withdraw?type=BTC", 'symbol_case': 'lower'},
    }

    LOST_CONNECTION_CEX_DICT_PATH = {
        'gateio': 'CEX/lost_connections/gateio_lost.json',
        'bitmart': 'CEX/lost_connections/bitmart_lost.json',
        'htx': 'CEX/lost_connections/htx_lost.json',
        'kucoin': 'CEX/lost_connections/kucoin_lost.json',
        'lbank': 'CEX/lost_connections/lbank_lost.json',
        'bingx': 'CEX/lost_connections/bingx_lost.json',
        'coinex': 'CEX/lost_connections/coinex_lost.json',
        'mexc': 'CEX/lost_connections/mexc_lost.json',
        'bybit': 'CEX/lost_connections/bybit_lost.json',
    }
