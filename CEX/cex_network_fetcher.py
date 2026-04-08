import ccxt.async_support as ccxt_async  # Use the async version of ccxt
import json
import time
from decimal import Decimal, getcontext
import logging
from collections import defaultdict
import random
import string
import asyncio
import aiohttp
import re
import base64
import hashlib
from collections import OrderedDict
from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
import sys

# --- Configuration ---
getcontext().prec = 18
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- List of Exchanges to Process ---
EXCHANGES_TO_PROCESS = [
    'lbank',
    'coinex',
    'binance',
    'bingx',
    'gateio',
    'kucoin',
    'huobi',
    'bybit',
    'mexc',
    'bitget',
    'okx',
]

# --- API Credentials ---
API_CREDENTIALS = {
    'binance': {'apiKey': "0Sdsm77CodRIi4Q8YWXPD3ApHgh3KadcRJVEM9uSMJBpbk5IFxgRb8a6HrgBgCKh",
                'secret': "iEH9TqBBRcosRw9RijmrtLd7FTScMxeS8ZhyxdF80V7Cy2x9WpynkhVIEg1SzxyY"},
    'bingx': {'apiKey': "U2hGqxMmEWVLklrQlMJANKTPwI30tn55uQgvJxSoZPbWJaS0Mu6nJkADVb06IlKnOKlcZeIBm4pXbbUUdLeA",
              'secret': "ntutN2dkqReuzy3wknyf93aZa6lUrAcjLmU4hEIGeVTBJhnbrZWzYfX6SUNaNWYg9VOBxYZd6lnasViW6wDVQ"},
    'gateio': {'apiKey': "54766c3ddbb372cf66d87facde6dde23",
               'secret': "99e47a0cf950b1721eb5b9a384098f6f6f96cd08b9a36d03f1839537ec1c805b"},
    'kucoin': {'apiKey': "680c849cc1dfd9000104ff53", 'secret': "2156cac2-efb6-4423-8b9d-744b8624924c",
               'password': "arbitrage-bot-password"},
    'huobi': {'apiKey': "da7fadaa-vfd5ghr532-2aa9e678-7d475", 'secret': "1d730d5a-8fa0592d-71582b83-12622"},
    'bybit': {'apiKey': "VQWKIGKdo0CXttw1kc", 'secret': "4xOeoPaRPTgqQUsFNEIUAYiMpEL98590xI3D"},
    'mexc': {'apiKey': "mx0vglvr6hBgtQ4GiP", 'secret': "7dc62ebefcc94f23a35f9a05fadf0835"},
    'bitget': {'apiKey': "bg_fa868584a512308a7c95b28612aa9147",
               'secret': "73597ba93105ded1a12dd4b6b2b30264822ac3c48249f5e6561b0360b46fd035", 'password': "4rbitragee"},
    'okx': {'apiKey': 'a02bf8c7-92f6-4f7e-ab89-ed887baee1b1', 'secret': '0C503A77BFAD53768C83E5B6A82ED22C',
            'password': 'Arbitrage-password-b0t'},
    'coinex': {'apiKey': "9A61A81A84654C9CB6C34DD7094AB992",
               'secret': "3D355B98E840F94AF8BD6557CEDEA18239AA4BAF3B915C50"},
    'lbank': {'apiKey': "708f8fab-64c3-4f81-bf73-4409e813a75e",
              'secret': "MIICdwIBADANBgkqhkiG9w0BAQEFAASCAmEwggJdAgEAAoGBAKKMokOjL2VcuLm2d6v0ZNTWEwzN6YJv3YPLabYU1yTv0FAZ2SH+h2Xyp5iuz8VAIP02xI3OZCi0ddlerRtXmWEzZk4UsPDgmRxBDiWtdqEYjd5OJNpxT9iCuwKur6ziwvvDkgQ7+Bq5PtgQOiLpb+sBmB22v3xbezuP7GkBc4+rAgMBAAECgYA9IjdEyPLiZGT1M7L+cQQbKAgyIJ9Z90iQcMhpOp1PvwHRnmcTae7yYLi3q8dooB3IHfj+fEkfJf/MqNbiGagNVNdro9X4VdeMxwVQ2PARqI0DXZQscZee6Slfm2frmygLXFn2zGsnaPuJUqQ5nfJXjnobiCaixpsdJ0ECvziDAQJBANifV0MT+Lrilk8geGY5KVwcuemFrp14f+1xViH/gsrJhMtG8qJGIXtKi1PjrC2VHgkjBdbWZi2cDhAukARb+tsCQQDAGPgIBTQuhY5aGfcUi7YhU7AKTs1gouk8C9jes+nqkcivSPVzHwfn7PbBrlQlVnbhgd5FF2ufz1oCtdmmpQ9xAkEAlOEeZjmprzffuk5EsW0D4gzUMYzxL8ULlzNX0VH8oEbT/6mpRKISjNv02hkV0tYzqG/rqXX9D8e7Wp/F11koFQJBAJz2SgTTq7Wvr4dz2+5P0nwc06U4ipxBwW4tCAHA5Iukn2SKqvRsEJuzhlTvyxXPNshAw4uU/X3RgezhBn0iNiECQEC4xNFEntZiVVclOn/izLcPeFA/eQt/1zLPzzW7GpzZgTNwK11643ab+5w8FwFFTrebjgSqYezwG8sh7+XyvGo="}
}

# --- Other Settings ---
QUOTE_CURRENCY = 'USDT'
OUTPUT_FILENAME = 'CEX/all_exchange_fees_async_integratedXXXX.json'
TICKER_FETCH_CHUNK_SIZE = 100
REQUEST_DELAY = 0.1  # Can be lower for async operations
UPDATE_INTERVAL_SECONDS = 300  # 5 minutes

# --- Global store for last known good data ---
last_known_good_data = {}

# --- Network Name Mapping ---
NETWORK_NAME_MAP = {
    "ERC20": "Ethereum", "erc20": "Ethereum", "ARBITRUM": "Arbitrum",
    "ARB": "Arbitrum", "BEP20": "BNB", "OPTIMISM": "OP_Mainnet",
    "solana": "Solana", "SOL": "Solana", "polygon": "Polygon",
    "POLYGON": "Polygon", "BASE": "Base", "AVAXCCHAIN": "Avalanche",
    "AVAX": "Avalanche", "CELO": "Celo", "TRC20": "Tron",
}


# ==============================================================================
# LBANK NATIVE IMPLEMENTATION (Adapted to be a clean worker function)
# ==============================================================================

async def _lbank_fetch_single_asset_config(session: aiohttp.ClientSession, asset_code: str, api_key: str,
                                           secret_key_bytes: bytes) -> tuple[str, list | None]:
    """Helper to fetch config for one LBank asset. Returns (asset_code, data)."""
    # This internal logic remains mostly the same
    BASE_URL = "https://api.lbank.info"
    ENDPOINT_PATH = "/v2/assetConfigs.do"
    URL = BASE_URL + ENDPOINT_PATH
    timestamp = str(int(time.time() * 1000))
    echostr = "".join(random.choices(string.ascii_letters + string.digits, k=32))
    params_for_md5 = {'api_key': api_key, 'signature_method': 'RSA', 'timestamp': timestamp, 'echostr': echostr,
                      'assetCode': asset_code}
    ordered_params = OrderedDict(sorted(params_for_md5.items()))
    query_string = '&'.join([f"{k}={v}" for k, v in ordered_params.items()])
    prepared_str = hashlib.md5(query_string.encode('utf-8')).hexdigest().upper()

    try:
        private_key = RSA.import_key(secret_key_bytes)
        hash_obj = SHA256.new(prepared_str.encode('utf-8'))
        signer = pkcs1_15.new(private_key)
        sign = base64.b64encode(signer.sign(hash_obj)).decode('utf-8')
    except Exception as e:
        logging.error(f"[lbank] Error creating signature for {asset_code}: {e}")
        return asset_code, None

    final_params_to_send = {**params_for_md5, 'sign': sign}

    try:
        async with session.get(URL, params=final_params_to_send, timeout=20) as response:
            if response.status != 200 or 'application/json' not in response.headers.get('Content-Type', ''):
                logging.warning(
                    f"[lbank] Bad response for {asset_code}: Status {response.status}, Content-Type {response.headers.get('Content-Type')}")
                return asset_code, None
            data = await response.json()
            if data.get('result') == 'true':
                return asset_code, data.get('data', [])
            else:
                logging.warning(
                    f"[lbank] API returned error for {asset_code}: {data.get('error_code')}, {data.get('msg')}")
                return asset_code, None
    except Exception as e:
        logging.error(f"[lbank] Network error fetching {asset_code}: {e}")
        return asset_code, None


async def process_lbank_natively_async(api_key: str, secret_key: str):
    """
    Main worker function to fetch and process LBank fees using its native API.
    """
    logging.info("[lbank] Starting native processing...")
    CONCURRENT_REQUESTS = 20
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    # Note: Using synchronous CCXT here is a small compromise for simplicity to get the market list.
    # The performance-critical part (fetching data for each token) is fully async.
    try:
        lbank_ccxt = ccxt_async.lbank()
        await lbank_ccxt.load_markets(True)
        all_symbols = lbank_ccxt.symbols
        tickers = await lbank_ccxt.fetch_tickers()
        await lbank_ccxt.close()
    except Exception as e:
        logging.error(f"[lbank] Could not load markets/tickers via CCXT. Error: {e}")
        return 'lbank', {"error": "Failed to load markets/tickers via CCXT."}

    base_tokens = {
        s.split('/')[0] for s in all_symbols
        if s.endswith('/USDT') and not re.match(r'.*(3L|3S|5L|5S)$', s.split('/')[0])
    }
    logging.info(f"[lbank] Found {len(base_tokens)} non-leveraged tokens to process.")

    try:
        secret_key_bytes = base64.b64decode(secret_key)
    except Exception:
        logging.error(f"[lbank] FATAL - Could not decode SECRET_KEY. It must be Base64 encoded.")
        return 'lbank', {"error": "Invalid LBank secret key format."}

    lbank_result = {}
    async with aiohttp.ClientSession() as session:
        tasks = []
        for token in sorted(list(base_tokens)):
            # Use a semaphore to limit concurrency
            await semaphore.acquire()
            task = asyncio.create_task(_lbank_fetch_single_asset_config(session, token, api_key, secret_key_bytes))
            task.add_done_callback(lambda t: semaphore.release())
            tasks.append(task)

        for future in asyncio.as_completed(tasks):
            token, network_data_list = await future
            if not network_data_list:
                continue

            lbank_result[token] = {"networks": {}}
            last_price = tickers.get(f"{token}/USDT", {}).get('last')

            for network_info in network_data_list:
                chain_name = network_info.get('chainName')
                if not chain_name: continue

                fee_from_api = network_info.get('assetFee', {}).get('feeAmt')
                withdraw_fee = float(fee_from_api) if fee_from_api is not None else None

                withdraw_fee_usd = (last_price * withdraw_fee) if last_price and withdraw_fee is not None else None

                # Apply network name mapping right here
                final_network_key = NETWORK_NAME_MAP.get(chain_name, chain_name)

                lbank_result[token]["networks"][final_network_key] = {
                    "depositEnabled": network_info.get('canDeposit', False),
                    "withdrawEnabled": network_info.get('canDraw', False),
                    "withdrawFee": withdraw_fee,
                    "withdrawFeeUSD": withdraw_fee_usd
                }

    logging.info(f"[lbank] Native processing finished. Found data for {len(lbank_result)} tokens.")
    return 'lbank', lbank_result


# ==============================================================================
# GENERIC CCXT WORKER IMPLEMENTATION
# ==============================================================================

async def process_ccxt_exchange_async(exchange_id: str):
    """
    Handles the entire data fetching and processing pipeline for a single CCXT exchange.
    """
    logging.info(f"[{exchange_id}] Starting processing...")
    config = {
        'enableRateLimit': True,
        'options': {'defaultType': 'spot'},
        'timeout': 180000  # Increased timeout to 30 seconds
    }
    creds = API_CREDENTIALS.get(exchange_id, {})
    if creds.get('apiKey') and creds.get('secret'):
        config.update(creds)
    else:
        logging.info(f"[{exchange_id}] No valid API credentials. Using public access.")

    exchange = getattr(ccxt_async, exchange_id)(config)

    try:
        # --- Load Markets & Tickers Concurrently ---
        logging.info(f"[{exchange_id}] Loading markets and fetching tickers...")
        await exchange.load_markets()

        usdt_symbols = [s for s in exchange.symbols if s.endswith(f'/{QUOTE_CURRENCY}')]
        ticker_prices = {}
        if usdt_symbols:
            ticker_tasks = [
                exchange.fetch_tickers(usdt_symbols[i:i + TICKER_FETCH_CHUNK_SIZE])
                for i in range(0, len(usdt_symbols), TICKER_FETCH_CHUNK_SIZE)
            ]
            for result_batch in await asyncio.gather(*ticker_tasks, return_exceptions=True):
                if isinstance(result_batch, dict):
                    for symbol, data in result_batch.items():
                        if data and data.get('last') is not None:
                            ticker_prices[symbol] = Decimal(str(data['last']))

        logging.info(f"[{exchange_id}] Fetched {len(ticker_prices)} ticker prices.")

        # --- Fetch Fees ---
        explicit_fee_data = {}
        if exchange.has.get('fetchDepositWithdrawFees'):
            try:
                explicit_fee_data = await exchange.fetchDepositWithdrawFees()
                logging.info(f"[{exchange_id}] Fetched explicit fee data.")
            except Exception as e:
                logging.warning(f"[{exchange_id}] Could not fetch explicit fees: {e}")

        # --- Process Data (CPU-bound, so no async needed here) ---
        exchange_results = {}
        all_currency_codes = set(exchange.currencies.keys()) | set(explicit_fee_data.keys())

        for code in all_currency_codes:
            currency_data_lm = exchange.currencies.get(code, {})
            if not currency_data_lm.get('active', True): continue

            explicit_fee_info = explicit_fee_data.get(code, {})
            processed_networks = {}
            last_price = ticker_prices.get(f"{code}/{QUOTE_CURRENCY}")

            networks_lm = currency_data_lm.get('networks', {})
            all_network_codes = set(networks_lm.keys())
            if isinstance(explicit_fee_info.get('networks'), dict):
                all_network_codes.update(explicit_fee_info['networks'].keys())

            if not all_network_codes: all_network_codes.add('DEFAULT')  # Placeholder

            for network_code in all_network_codes:
                network_data_lm = networks_lm.get(network_code, {})
                explicit_network_info = explicit_fee_info.get('networks', {}).get(network_code, {})

                withdraw_fee_raw = (
                        explicit_network_info.get('fee') or
                        explicit_fee_info.get('withdraw', {}).get('fee') or
                        network_data_lm.get('fee') or
                        currency_data_lm.get('fee')
                )

                if withdraw_fee_raw is None: continue

                try:
                    withdraw_fee = Decimal(str(withdraw_fee_raw).replace('%', ''))
                    withdraw_fee_float = float(withdraw_fee)
                except Exception:
                    continue  # Skip if fee is not a valid number

                withdraw_fee_usd = float(withdraw_fee * last_price) if last_price else None

                final_network_key = NETWORK_NAME_MAP.get(network_code, network_code)
                processed_networks[final_network_key] = {
                    "depositEnabled": network_data_lm.get('deposit', False),
                    "withdrawEnabled": network_data_lm.get('withdraw', False),
                    "withdrawFee": withdraw_fee_float,
                    "withdrawFeeUSD": withdraw_fee_usd,
                }

            if processed_networks:
                exchange_results[code] = {"networks": processed_networks}

        logging.info(f"[{exchange_id}] Processing complete. Found data for {len(exchange_results)} currencies.")
        return exchange_id, exchange_results

    except Exception as e:
        logging.error(f"[{exchange_id}] An unexpected error occurred: {e}", exc_info=True)
        return exchange_id, {"error": f"Unexpected Error: {str(e)}"}
    finally:
        await exchange.close()


# ==============================================================================
# Main Execution Runner
# ==============================================================================
async def run_update_cycle():
    """Runs one full cycle of fetching data from all exchanges concurrently."""
    global last_known_good_data  # Ensure we are using the global variable
    logging.info("=" * 30 + "\nStarting new update cycle...\n" + "=" * 30)

    new_lkgs_for_this_cycle = {}  # This will become the new last_known_good_data at the end of the cycle

    tasks = []
    for eid in EXCHANGES_TO_PROCESS:
        if eid == 'lbank':
            creds = API_CREDENTIALS.get('lbank', {})
            if creds.get('apiKey') and creds.get('secret'):
                tasks.append(process_lbank_natively_async(creds['apiKey'], creds['secret']))
            else:
                logging.error("[lbank] API key or secret not found. Skipping LBank native processing for this cycle.")
                tasks.append(
                    asyncio.create_task(asyncio.sleep(0, result=('lbank', {"error": "API key or secret not found."}))))
        else:
            tasks.append(process_ccxt_exchange_async(eid))

    raw_results_from_tasks = await asyncio.gather(*tasks, return_exceptions=True)

    current_cycle_fetched_data = {}
    for res in raw_results_from_tasks:
        if isinstance(res, Exception):
            logging.error(f"A task failed with an unhandled exception during gather: {res}", exc_info=True)
            # We don't know which exchange_id this belongs to here, so it will be missing from current_cycle_fetched_data
        elif isinstance(res, tuple) and len(res) == 2:
            exchange_id_from_task, data_from_task = res
            current_cycle_fetched_data[exchange_id_from_task] = data_from_task
        else:
            logging.warning(f"Received an unexpected result format from a worker task: {res}")

    final_data_to_output = {}
    for exchange_id in EXCHANGES_TO_PROCESS:
        current_fetch_result = current_cycle_fetched_data.get(exchange_id)
        # previous_lkgs_for_exchange is structured as: {coin_symbol: {'data': {...}, 'missing_streak': X}}
        previous_lkgs_for_exchange = last_known_good_data.get(exchange_id, {})

        output_for_this_exchange = {}
        lkgs_for_this_exchange_this_cycle = {}

        current_fetch_is_good_and_has_data = False
        if isinstance(current_fetch_result, dict) and not current_fetch_result.get("error"):
            # An empty dict {} from fetch means the exchange is responsive but has no coins for us.
            # It's "good" in terms of API health, but "bad" if we expect coins.
            # We treat it as a "successful fetch" that just happens to be empty.
            # If it has data (len > 0), it's definitely good.
            current_fetch_is_good_and_has_data = True

        if current_fetch_is_good_and_has_data:
            if len(current_fetch_result) > 0:
                logging.info(f"[{exchange_id}] Successfully fetched new data with {len(current_fetch_result)} assets.")
            else:
                logging.info(f"[{exchange_id}] Successfully fetched new data, which is empty (0 assets).")

            # Process currently fetched coins (if any)
            for coin_symbol, coin_data in current_fetch_result.items():
                output_for_this_exchange[coin_symbol] = coin_data
                lkgs_for_this_exchange_this_cycle[coin_symbol] = {'data': coin_data, 'missing_streak': 0}

            # Check for coins that were in previous LKG but not in current successful (possibly empty) fetch
            if previous_lkgs_for_exchange:
                for coin_symbol, prev_coin_meta in previous_lkgs_for_exchange.items():
                    if coin_symbol not in current_fetch_result:  # Coin is missing in current good fetch
                        new_streak = prev_coin_meta.get('missing_streak', 0) + 1
                        if new_streak < 4:
                            output_for_this_exchange[coin_symbol] = prev_coin_meta['data']  # Use stale data
                            lkgs_for_this_exchange_this_cycle[coin_symbol] = {'data': prev_coin_meta['data'],
                                                                              'missing_streak': new_streak}
                            logging.info(
                                f"[{exchange_id}] Coin '{coin_symbol}' missing in current fetch, using stale data (streak {new_streak}).")
                        else:
                            logging.info(
                                f"[{exchange_id}] Coin '{coin_symbol}' dropped after {new_streak} missing cycles following a successful exchange fetch.")
        else:  # Current fetch failed, returned error, or was None (task didn't complete normally)
            if current_fetch_result and current_fetch_result.get("error"):
                logging.error(f"[{exchange_id}] Fetch resulted in an error: {current_fetch_result.get('error')}")
            elif current_fetch_result is None:
                logging.error(
                    f"[{exchange_id}] Fetch failed: No data returned from task (it may have crashed or been cancelled).")
            else:  # Should not happen if current_fetch_is_good_and_has_data is false due to other reasons
                logging.error(
                    f"[{exchange_id}] Fetch was not successful for an unknown reason. Data: {current_fetch_result}")

            if previous_lkgs_for_exchange:
                logging.info(f"[{exchange_id}] Using stale data due to current fetch issue.")
                all_stale_coins_timed_out = True
                for coin_symbol, prev_coin_meta in previous_lkgs_for_exchange.items():
                    new_streak = prev_coin_meta.get('missing_streak',
                                                    0) + 1  # Increment streak due to exchange-level issue
                    if new_streak < 4:
                        output_for_this_exchange[coin_symbol] = prev_coin_meta['data']
                        lkgs_for_this_exchange_this_cycle[coin_symbol] = {'data': prev_coin_meta['data'],
                                                                          'missing_streak': new_streak}
                        all_stale_coins_timed_out = False
                    else:
                        logging.info(
                            f"[{exchange_id}] Coin '{coin_symbol}' from stale data dropped after {new_streak} total missing cycles (due to ongoing exchange issue).")

                if all_stale_coins_timed_out and not output_for_this_exchange:  # if output is still empty
                    logging.error(f"[{exchange_id}] All stale data timed out during current fetch issue.")
                    output_for_this_exchange = current_fetch_result if (
                                current_fetch_result and current_fetch_result.get("error")) else {
                        "error": f"Exchange {exchange_id} fetch failed and all stale coins timed out."}
            else:
                logging.error(f"[{exchange_id}] Fetch failed and no stale data available.")
                output_for_this_exchange = current_fetch_result if current_fetch_result else {
                    "error": f"Data fetch failed for {exchange_id} and no previous data available."}

        if lkgs_for_this_exchange_this_cycle:  # if there's anything to keep for next cycle
            new_lkgs_for_this_cycle[exchange_id] = lkgs_for_this_exchange_this_cycle

        final_data_to_output[exchange_id] = output_for_this_exchange

    last_known_good_data = new_lkgs_for_this_cycle  # Update the global LKG store

    if 'huobi' in final_data_to_output:
        final_data_to_output['htx'] = final_data_to_output.pop('huobi')

    # --- Output to File ---
    try:
        output_json = json.dumps(final_data_to_output, indent=4, default=str)
        with open(OUTPUT_FILENAME, 'w') as f:
            f.write(output_json)
        logging.info(f"Update cycle complete. Combined results saved to {OUTPUT_FILENAME}")
    except Exception as e:
        logging.error(f"Error saving combined results to file: {e}")


async def main():
    """Main entry point: runs the update cycle on a schedule."""
    while True:
        await run_update_cycle()
        logging.info(f"Next update cycle will start in {UPDATE_INTERVAL_SECONDS} seconds...")
        await asyncio.sleep(UPDATE_INTERVAL_SECONDS)


def main_entry_point_CNF():
    """
    Main entry point for CEX network fetcher.
    """
    if sys.platform == 'win32' and sys.version_info >= (3, 8):
        # Necessary for aiohttp on Windows in some cases
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Script manually interrupted. Exiting.")


# ==============================================================================
# Main Execution - Scheduler
# ==============================================================================

if __name__ == "__main__":
    main_entry_point_CNF()