#!/usr/bin/env python
import asyncio
import websockets
import json
import gzip
import io
import os
import time
import uuid
import datetime
import logging
import sys
import traceback
from typing import List, Dict, Optional, Any, Tuple, Literal  # MODIFIED, Literal  # MODIFIED
import ccxt
import aiohttp

# --- Configuration ---
EXCHANGE_ID = 'bingx'
MARKET_TYPE_TO_WATCH = 'spot'
QUOTE_ASSET = 'USDT'
TOPICS_PER_CONNECTION_LIMIT = 25
DEPTH_LEVEL = 50
UPDATE_INTERVAL_MS = 200  # Ignored for spot, used for swap
OUTPUT_DIRECTORY = "CEX/orderbooks/bingx_orderbooks"
# MODIFICATION: Added constants for lost connections
LOST_CONNECTIONS_DIR = "CEX/lost_connections"
LOST_CONNECTIONS_FILE = os.path.join(LOST_CONNECTIONS_DIR, "bingx_lost.json")
RECONNECT_BASE_DELAY_SECONDS = 5
RECONNECT_MAX_DELAY_SECONDS = 300
PING_SERVER_INTERVAL_SECONDS = 25
CLIENT_PONG_TIMEOUT_SECONDS = 10

# --- Telegram Configuration ---
TELEGRAM_BOT_TOKEN = ""
TELEGRAM_CHAT_ID = 1007554570
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
MAX_CONSECUTIVE_CONNECTION_FAILURES_FOR_ALERT = 5  # Changed from 3 to 5 for consistency
MAX_TELEGRAM_MSG_LENGTH = 4096

# --- Logging Setup ---
LOG_FORMAT = '%(asctime)s [%(levelname)s] [%(name)s] %(message)s'
# MODIFICATION: Added MAIN_LOG_FILE
MAIN_LOG_FILE = "CEX/app_log/bingx_app.log"
ERROR_LOG_FILENAME = "CEX/logging_cex/bingx_connector_errors.log"

# Set root logger to lowest level to allow handlers to filter
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
logging.getLogger("websockets").setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
root_logger.addHandler(console_handler)

try:
    # Main log file handler (bingx_app.log) - Captures EVERYTHING
    main_file_handler = logging.FileHandler(MAIN_LOG_FILE, mode='a', encoding='utf-8')
    main_file_handler.setLevel(logging.DEBUG)
    main_file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    root_logger.addHandler(main_file_handler)

    # Error log file handler - Captures only ERRORS
    error_file_handler = logging.FileHandler(ERROR_LOG_FILENAME, mode='a', encoding='utf-8')
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    root_logger.addHandler(error_file_handler)
except Exception as e:
    print(f"CRITICAL: Failed to configure file loggers: {e}")

logger = logging.getLogger(f"{EXCHANGE_ID}_connector")

BINGX_SPOT_WS_URL = "wss://open-api-ws.bingx.com/market"
BINGX_SWAP_WS_URL = "wss://open-api-swap.bingx.com/swap-market"
WEBSOCKET_URL = BINGX_SPOT_WS_URL if MARKET_TYPE_TO_WATCH == 'spot' else BINGX_SWAP_WS_URL


# --- Telegram Sender Function ---
# MODIFICATION: Enhanced function to handle levels and formatting
async def send_telegram_message(message: str, level: str = 'INFO', session: Optional[aiohttp.ClientSession] = None):
    if not TELEGRAM_ENABLED:
        return

    telegram_logger = logger.getChild("telegram")
    level_emoji = {'INFO': '✅', 'WARNING': '🟡', 'CRITICAL': '🔴'}
    if "re-established" in message.lower():
        level_emoji['INFO'] = '🟢'
    if "starting up" in message.lower():
        level_emoji['INFO'] = '🚀'

    formatted_message = f"{level_emoji.get(level, '🔵')} **BingX WebSocket Monitor**\n\n{message}"
    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    if len(formatted_message) > MAX_TELEGRAM_MSG_LENGTH:
        formatted_message = formatted_message[:MAX_TELEGRAM_MSG_LENGTH - 100] + "\n... (message truncated)"

    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': formatted_message, 'parse_mode': 'Markdown'}
    _session_managed_locally = False
    actual_session = session
    if not actual_session:
        actual_session = aiohttp.ClientSession()
        _session_managed_locally = True

    try:
        async with actual_session.post(api_url, json=payload, timeout=10) as response:
            if response.status != 200:
                response_text = await response.text()
                telegram_logger.error(
                    f"Failed to send Telegram message. Status: {response.status}, Response: {response_text}")
    except Exception as e:
        telegram_logger.error(f"Unexpected error sending Telegram message: {e}", exc_info=True)
    finally:
        if _session_managed_locally and actual_session:
            await actual_session.close()


# --- Utility functions (UNCHANGED) ---
def get_bingx_tickers(market_type: str = MARKET_TYPE_TO_WATCH, quote_asset: str = QUOTE_ASSET, max_retries: int = 3,
                      retry_delay_s: int = 10) -> Tuple[List[str], Optional[str]]:
    ccxt_logger = logger.getChild("ccxt_fetch")
    ccxt_logger.info(f"Fetching BingX {market_type} tickers for {quote_asset} quote...")
    exchange_config = {'options': {'defaultType': 'swap'}} if market_type == 'swap' else {}
    exchange = ccxt.bingx(exchange_config)
    try:
        markets = exchange.load_markets(True)
        filtered_symbols = []
        for _, market_info in markets.items():
            if market_info.get('type', '').lower() == market_type.lower() and market_info.get('active',
                                                                                              False) and market_info.get(
                'quote', '').upper() == quote_asset.upper():
                filtered_symbols.append(market_info['id'])
        ccxt_logger.info(f"Found {len(filtered_symbols)} active {market_type} {quote_asset} tickers.")
        return filtered_symbols, None
    except Exception as e:
        err_msg = f"CCXT error loading markets: {type(e).__name__} - {e}"
        ccxt_logger.error(err_msg, exc_info=True)
        return [], err_msg


# --- WebSocket Handler Task (MODIFIED for notifications) ---
async def handle_bingx_connection(symbols_batch: List[str], conn_id: str, shutdown_event: asyncio.Event,
                                  http_session: aiohttp.ClientSession):
    task_logger = logger.getChild(conn_id)
    # MODIFICATION: Extract base tokens for this batch
    base_tokens_in_batch = [extract_base_asset(s) for s in symbols_batch]
    task_logger.info(
        f"Starting for {len(symbols_batch)} symbols (Base tokens: {', '.join(base_tokens_in_batch[:5])}{'...' if len(base_tokens_in_batch) > 5 else ''})...")

    subscription_payloads = []
    for symbol_id_ccxt in symbols_batch:
        data_type_str = f"{symbol_id_ccxt}@depth{DEPTH_LEVEL}" if MARKET_TYPE_TO_WATCH == 'spot' else f"{symbol_id_ccxt}@depth{DEPTH_LEVEL}@{UPDATE_INTERVAL_MS}ms"
        subscription_payloads.append({"id": str(uuid.uuid4()), "reqType": "sub", "dataType": data_type_str})

    current_retry_delay = RECONNECT_BASE_DELAY_SECONDS
    consecutive_failures = 0

    while not shutdown_event.is_set():
        last_exception: Optional[Exception] = None
        last_server_ping_time = time.monotonic()
        try:
            task_logger.info(f"Attempting to connect to {WEBSOCKET_URL}...")
            async with websockets.connect(WEBSOCKET_URL, open_timeout=20, close_timeout=10,
                                          ping_interval=None) as websocket:
                # MODIFICATION: Handle re-establishment notification
                if consecutive_failures > 0:  # This means it's a re-established connection
                    task_logger.info("Connection re-established.")
                    # MODIFICATION: Manage lost connections file - remove tokens for this batch
                    removed_tokens = manage_lost_connections_file_sync(base_tokens_in_batch, conn_id, action='remove')

                    reestablish_msg = f"Connection for worker `{conn_id}` has been re-established."
                    if removed_tokens:  # Add info about restored symbols if any were actually removed from the file
                        reestablish_msg += f" Monitored symbols ({', '.join(removed_tokens)}) are back online."

                    await send_telegram_message(reestablish_msg, level='INFO', session=http_session)

                consecutive_failures = 0
                current_retry_delay = RECONNECT_BASE_DELAY_SECONDS
                task_logger.info("Connection established. Subscribing...")

                for sub_payload in subscription_payloads: await websocket.send(json.dumps(sub_payload))

                while not shutdown_event.is_set():
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=PING_SERVER_INTERVAL_SECONDS + 5)
                        if isinstance(message, bytes):
                            utf8_data_str = gzip.GzipFile(fileobj=io.BytesIO(message), mode='rb').read().decode('utf-8')
                        else:
                            utf8_data_str = message
                        if utf8_data_str.lower() == "ping":
                            await websocket.send("Pong");
                            last_server_ping_time = time.monotonic();
                            continue
                        data = json.loads(utf8_data_str)
                        if data.get("dataType") and isinstance(data.get("data"), dict):
                            symbol_from_msg = data["dataType"].split('@')[0]
                            with open(os.path.join(OUTPUT_DIRECTORY, f"{symbol_from_msg.replace('-', '')}.json"),
                                      'w') as f: json.dump(data["data"], f, indent=2)
                    except asyncio.TimeoutError:
                        if (
                                time.monotonic() - last_server_ping_time) > PING_SERVER_INTERVAL_SECONDS + CLIENT_PONG_TIMEOUT_SECONDS:
                            raise websockets.exceptions.ConnectionClosedError(1001, "Stale connection, no server ping")

        except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError) as e:
            last_exception = e
            task_logger.warning(f"Connection issue: {type(e).__name__} - {e}")
        except Exception as e:
            last_exception = e
            task_logger.error(f"Unhandled exception in connection task", exc_info=True)

        if not shutdown_event.is_set():
            consecutive_failures += 1

            # MODIFICATION: Manage lost connections file - add tokens for this batch
            # This is done regardless of whether it's the first failure or subsequent,
            # to ensure the file is always up-to-date with the latest failure.
            manage_lost_connections_file_sync(base_tokens_in_batch, conn_id, action='add')

            task_logger.info(f"Reconnecting in {current_retry_delay}s (attempt {consecutive_failures})...")

            # MODIFICATION: Enhanced failure notification logic
            if consecutive_failures >= MAX_CONSECUTIVE_CONNECTION_FAILURES_FOR_ALERT:
                error_details = traceback.format_exc() if last_exception else "No specific exception captured."
                # MODIFICATION: Include affected symbols in critical alert
                critical_msg = (
                    f"CRITICAL failure for worker `{conn_id}`. Cannot re-establish after {consecutive_failures} attempts.\\n"
                    f"Symbols managed by this worker: {', '.join(base_tokens_in_batch) if base_tokens_in_batch else 'N/A'}.\\n\\n"
                    f"**Last Error:**\\n```\\n{last_exception}\\n```\\n"
                    f"**Full Traceback:**\\n```\\n{error_details[:1000]}\\n```"
                )
                await send_telegram_message(critical_msg, level='CRITICAL', session=http_session)
                consecutive_failures = 0  # Reset to avoid spamming
            elif consecutive_failures == 1:  # First failure in a sequence for this worker
                # MODIFICATION: Send Telegram notification with affected symbols for this worker
                lost_symbols_message = (
                    f"Connection lost for worker `{conn_id}`. "
                    f"Affected symbols in this batch: {', '.join(base_tokens_in_batch)}. "
                    f"Attempting to reconnect...\\n\\n**Error:** `{last_exception}`"
                )
                await send_telegram_message(lost_symbols_message, level='WARNING', session=http_session)
            # For consecutive_failures > 1 but < MAX_..., no additional specific symbol message,
            # as the initial one (at failure == 1) already listed them. General reconnect attempts continue.

            await asyncio.sleep(current_retry_delay)
            current_retry_delay = min(current_retry_delay * 2, RECONNECT_MAX_DELAY_SECONDS)
        else:
            break
    task_logger.info(f"Task stopped.")


# --- Main Application Logic (MODIFIED for notifications) ---
async def run_main_application_loop(shutdown_event: asyncio.Event, http_session: aiohttp.ClientSession):
    active_ws_tasks: List[asyncio.Task] = []
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
    logger.info(f"Using WebSocket URL: {WEBSOCKET_URL} for market type: {MARKET_TYPE_TO_WATCH.upper()}")

    all_symbols, ccxt_err_msg = get_bingx_tickers()
    if not all_symbols:
        err_report = f"CRITICAL: Failed to fetch initial symbols from BingX. CCXT Error: {ccxt_err_msg}"
        logger.critical(err_report)
        await send_telegram_message(err_report, level='CRITICAL', session=http_session)
        return

    symbol_chunks = [all_symbols[i:i + TOPICS_PER_CONNECTION_LIMIT] for i in
                     range(0, len(all_symbols), TOPICS_PER_CONNECTION_LIMIT)]
    logger.info(f"Divided {len(all_symbols)} symbols into {len(symbol_chunks)} connection chunks.")

    for i, chunk in enumerate(symbol_chunks):
        if shutdown_event.is_set(): break
        conn_id = f"conn-{i + 1}"
        task = asyncio.create_task(handle_bingx_connection(chunk, conn_id, shutdown_event, http_session))
        active_ws_tasks.append(task)
        await asyncio.sleep(0.5)

    if active_ws_tasks:
        logger.info(f"All {len(active_ws_tasks)} WebSocket tasks launched. Monitoring...")
        # MODIFICATION: Send detailed success notification
        success_message = (
            f"BingX Market Monitor Started Successfully!\n\n"
            f"• **Market Type:** `{MARKET_TYPE_TO_WATCH.upper()}`\n"
            f"• **Tracking:** `{len(all_symbols)}` symbols\n"
            f"• **Connections:** `{len(symbol_chunks)}` workers launched\n"
            f"• **Topics/Connection:** `{TOPICS_PER_CONNECTION_LIMIT}`"
        )
        await send_telegram_message(success_message, level='INFO', session=http_session)
        try:
            await shutdown_event.wait()
        except asyncio.CancelledError:
            logger.info("Main application run_loop cancelled.")

    if not shutdown_event.is_set(): shutdown_event.set()
    if active_ws_tasks:
        done, pending = await asyncio.wait(active_ws_tasks, timeout=10)
        for task in pending: task.cancel()
        if pending: await asyncio.gather(*pending, return_exceptions=True)


async def main_with_session():
    shutdown_event = asyncio.Event()
    try:
        async with aiohttp.ClientSession() as http_session:
            # MODIFICATION: Immediate startup notification
            await send_telegram_message(f"BingX Connector ({MARKET_TYPE_TO_WATCH.upper()}) script is starting up...",
                                        level='INFO', session=http_session)
            main_app_task = asyncio.create_task(run_main_application_loop(shutdown_event, http_session))
            await main_app_task
    except asyncio.CancelledError:
        logger.info("main_with_session task was cancelled.")
    except Exception as e:
        error_message = f"FATAL ERROR (BingX Connector): Unhandled Exception in main_with_session: {e}"
        logger.critical(error_message, exc_info=True)
        # Session might be closed, so create a temporary one for a final report
        async with aiohttp.ClientSession() as temp_session:
            await send_telegram_message(error_message, level='CRITICAL', session=temp_session)
    finally:
        if not shutdown_event.is_set(): shutdown_event.set()


# --- Utility functions ---

# MODIFICATION: Added utility function to extract base asset
def extract_base_asset(symbol_id_ccxt: str) -> str:
    """
    Extracts the base asset from a CCXT market ID.
    Handles formats like "BTC/USDT" -> "BTC" or "ETH/USDT:USDT" -> "ETH".
    """
    # Assumes symbol_id_ccxt from CCXT is typically 'BASE/QUOTE' or 'BASE/QUOTE:QUOTE'
    return symbol_id_ccxt.split('/')[0]


# MODIFICATION: Added utility function to manage the lost connections JSON file
def manage_lost_connections_file_sync(
        base_tokens: List[str],
        conn_id: str,  # For logging
        action: Literal['add', 'remove']
) -> List[str]:  # Returns list of tokens whose state actually changed
    """
    Manages the bingx_lost.json file.
    Adds or removes base tokens based on connection status.
    """
    os.makedirs(LOST_CONNECTIONS_DIR, exist_ok=True)

    lost_connections_data: Dict[str, str] = {}
    if os.path.exists(LOST_CONNECTIONS_FILE):
        try:
            with open(LOST_CONNECTIONS_FILE, 'r', encoding='utf-8') as f:
                content = f.read()
                if content.strip():  # Ensure file is not empty
                    lost_connections_data = json.loads(content)
                else:  # File is empty or whitespace only
                    lost_connections_data = {}
        except json.JSONDecodeError:
            # Use the main logger here as this function is global
            logging.getLogger(f"{EXCHANGE_ID}_connector").error(
                f"Worker {conn_id}: Error decoding {LOST_CONNECTIONS_FILE}. Re-initializing file content.")
            lost_connections_data = {}  # Reset if file is corrupt
        except Exception as e:
            logging.getLogger(f"{EXCHANGE_ID}_connector").error(
                f"Worker {conn_id}: Error reading {LOST_CONNECTIONS_FILE}: {e}. Assuming empty content.")
            lost_connections_data = {}  # Reset on other read errors

    changed_tokens: List[str] = []

    if action == 'add':
        for token in base_tokens:
            if lost_connections_data.get(token) != "lost":
                lost_connections_data[token] = "lost"
                changed_tokens.append(token)
        if changed_tokens:
            logging.getLogger(f"{EXCHANGE_ID}_connector").info(
                f"Worker {conn_id}: Added {changed_tokens} to {LOST_CONNECTIONS_FILE}.")

    elif action == 'remove':
        for token in base_tokens:
            if token in lost_connections_data:
                del lost_connections_data[token]
                changed_tokens.append(token)
        if changed_tokens:
            logging.getLogger(f"{EXCHANGE_ID}_connector").info(
                f"Worker {conn_id}: Removed {changed_tokens} from {LOST_CONNECTIONS_FILE}.")

    try:
        with open(LOST_CONNECTIONS_FILE, 'w', encoding='utf-8') as f:
            json.dump(lost_connections_data, f, indent=2)
    except Exception as e:
        logging.getLogger(f"{EXCHANGE_ID}_connector").error(
            f"Worker {conn_id}: Error writing to {LOST_CONNECTIONS_FILE}: {e}")
    return changed_tokens


# --- Main Entry Point ---
def main_entry_point_bingx():
    """
    Main entry point for BingX Connector.
    """
    # MODIFICATION: Ensure lost_connections directory exists at startup
    os.makedirs(LOST_CONNECTIONS_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(ERROR_LOG_FILENAME), exist_ok=True)
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
    try:
        asyncio.run(main_with_session())
    except KeyboardInterrupt:
        logger.info("\nScript interrupted by user. Shutting down.")
    except Exception as e:
        logger.critical(f"FATAL CRASH (Top Level): {e}", exc_info=True)
    finally:
        logger.info("Program finished execution.")
        logging.shutdown()