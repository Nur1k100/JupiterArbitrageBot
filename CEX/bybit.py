#!/usr/bin/env python
import asyncio
import websockets
import json
import time
import os
import ccxt.async_support as ccxt_async
import csv
import aiohttp  # MODIFICATION: Using aiohttp instead of urllib
import logging
import sys
import traceback
from typing import List, Dict, Optional
import threading
import datetime

# --- Logging Configuration ---
LOG_FORMAT = '%(asctime)s [%(levelname)s] [%(name)s] %(message)s'
# MODIFICATION: Added MAIN_LOG_FILE
MAIN_LOG_FILE = "CEX/app_log/bybit_app.log"
ERROR_LOG_FILEPATH = "CEX/logging_cex/bybit_connector_errors.log"

# --- Configuration ---
MAX_TELEGRAM_MSG_LENGTH = 4096
MAX_CONCURRENT_CONNECTIONS_TO_ATTEMPT = 700  # Max number of symbols to try connecting to at once
CONNECTION_ATTEMPT_DELAY = 0.1  # Seconds to wait between launching connection tasks

# --- Reconnection Configuration ---
MAX_RECONNECTION_ATTEMPTS = 5  # Number of times to attempt reconnection for a single symbol
RECONNECTION_DELAY_SECONDS = 0.1  # Delay in seconds between reconnection attempts
RECONNECT_PERIOD_MINUTES = 360  # 6 hours 50 minutes, to preempt 7-hour disconnects

LOST_CONNECTIONS_DIR = "CEX/lost_connections"  # Directory for lost connection info
BYBIT_LOST_SYMBOLS_FILE = os.path.join(LOST_CONNECTIONS_DIR, "bybit_lost.json")  # File for Bybit lost symbols
_bybit_lost_symbols_lock = asyncio.Lock()  # Lock for managing access to the lost symbols file

OUTPUT_DIRECTORY = "CEX/orderbooks/bybit_orderbooks"
DEFAULT_DUMP_SCALE = 4
SUCCESSFUL_DUMPSCALES_CSV_FILE = "successful_dumpscales.csv"
DUMP_SCALE_RETRY_RANGE = range(16, -1, -1)
BYBIT_DUMPSCALE_ERROR_CODE = -100009
BYBIT_DUMPSCALE_ERROR_MSG_PART = "DumpScale error"

# --- Global Counters & Lists ---
successful_initial_connections = 0
failed_final_connections = 0
active_websocket_tasks = []
current_run_successful_details = []
established_successful_dumpscales_map = {}
total_symbols_identified_as_candidates = 0
counter_lock = asyncio.Lock()
success_map_lock = asyncio.Lock()

# --- Telegram Configuration ---
TELEGRAM_BOT_TOKEN = ""
TELEGRAM_CHAT_ID = 1
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID and "YOUR" not in TELEGRAM_BOT_TOKEN)


# --- Logger Setup ---
def setup_logging():
    """Configures logging to console and multiple files."""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # Set root to lowest level
    formatter = logging.Formatter(LOG_FORMAT)
    logging.getLogger("websockets").setLevel(logging.INFO)
    # Console Handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    root_logger.addHandler(ch)

    try:
        # Main log file handler (bybit_app.log) - Captures EVERYTHING
        main_handler = logging.FileHandler(MAIN_LOG_FILE, mode='a', encoding='utf-8')
        main_handler.setLevel(logging.DEBUG)
        main_handler.setFormatter(formatter)
        root_logger.addHandler(main_handler)

        # File handler for errors
        error_handler = logging.FileHandler(ERROR_LOG_FILEPATH, mode='a', encoding='utf-8')
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        root_logger.addHandler(error_handler)
    except Exception as e:
        print(f"CRITICAL: Failed to configure file loggers: {e}")

    logger = logging.getLogger("bybit_connector")
    if not TELEGRAM_ENABLED:
        logger.warning("Telegram notifications are DISABLED.")
    else:
        logger.info("Telegram notifications are ENABLED.")
    return logger


logger = setup_logging()


# --- Telegram Notification Function ---
# MODIFICATION: Replaced urllib with modern aiohttp and added levels
async def send_telegram_notification(message: str, level: str = 'INFO'):
    if not TELEGRAM_ENABLED: return

    level_emoji = {'INFO': '✅', 'WARNING': '🟡', 'CRITICAL': '🔴'}
    if "starting up" in message.lower(): level_emoji['INFO'] = '🚀'

    hostname = os.getenv("HOSTNAME", os.uname().nodename if hasattr(os, 'uname') else "UnknownHost")
    formatted_message = f"{level_emoji.get(level, '🔵')} **Bybit WebSocket Monitor** ({hostname})\n\n{message}"

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    if len(formatted_message) > 4096: formatted_message = formatted_message[:4093] + "..."
    data = {'chat_id': TELEGRAM_CHAT_ID, 'text': formatted_message, 'parse_mode': 'Markdown'}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=data, timeout=10) as response:
                if response.status != 200:
                    logger.error(f"Telegram API error: {await response.text()}")
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")


# --- Utility Functions (Unchanged) ---
def get_decimal_places(s):
    try:
        return len(str(s).split('.')[1]) if '.' in str(s) else 0
    except:
        return None


def load_established_dump_scales(filename=SUCCESSFUL_DUMPSCALES_CSV_FILE):
    loaded_scales = {}
    if not os.path.exists(filename): return loaded_scales
    try:
        with open(filename, 'r', newline='') as f:
            reader = csv.reader(f);
            next(reader, None)
            for row in reader:
                if len(row) == 2: loaded_scales[row[0].strip().upper()] = int(row[1].strip())
        logger.info(f"Loaded {len(loaded_scales)} established dump scales from {SUCCESSFUL_DUMPSCALES_CSV_FILE}.")
    except Exception as e:
        logger.error(f"Error loading established dump scales: {e}")
    return loaded_scales


def get_bybit_base_symbol(symbol: str) -> str:
    """Extracts the base symbol (e.g., BTC from BTCUSDT)."""
    if isinstance(symbol, str):
        # Common quote currencies used by Bybit for spot
        common_quotes = ["USDT", "USDC", "BTC", "ETH", "DAI", "EUR", "GBP"]
        for quote in common_quotes:
            if symbol.endswith(quote) and len(symbol) > len(quote):
                return symbol[:-len(quote)]
        # Fallback if no common quote is matched (e.g. for pairs like MNTETH)
        # This might need adjustment based on all possible symbol structures
        if len(symbol) > 3 and symbol[-3:].isupper() and symbol[:-3].isupper():  # Heuristic for 3-char quote
            return symbol[:-3]
        if len(symbol) > 4 and symbol[-4:].isupper() and symbol[:-4].isupper():  # Heuristic for 4-char quote
            return symbol[:-4]
    return symbol  # Fallback to original symbol if parsing fails


async def update_lost_symbols_file_bybit(
        base_symbols_to_update: List[str],  # List of base symbols (usually one for Bybit)
        action: str,  # 'add' or 'remove'
        file_path: str,
        logger_instance: logging.Logger,
        triggering_symbol: Optional[str] = None  # The original symbol like BTCUSDT
):
    async with _bybit_lost_symbols_lock:
        log_prefix = f"[{triggering_symbol}] " if triggering_symbol else ""
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            lost_data: Dict[str, str] = {}
            if os.path.exists(file_path):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        if content.strip():
                            lost_data = json.loads(content)
                        else:
                            lost_data = {}
                except json.JSONDecodeError:
                    logger_instance.error(f"{log_prefix}Error decoding JSON from {file_path}. Re-initializing file.")
                    lost_data = {}
                except FileNotFoundError:  # Should be caught by os.path.exists, but good practice
                    logger_instance.info(f"{log_prefix}{file_path} not found. Will create a new one.")
                    lost_data = {}

            updated_in_action = []
            if action == 'add':
                for sym_base in base_symbols_to_update:
                    if sym_base not in lost_data:
                        lost_data[sym_base] = "lost"
                        updated_in_action.append(sym_base)
                if updated_in_action:
                    logger_instance.info(
                        f"{log_prefix}Added to {os.path.basename(file_path)}: {updated_in_action}. Current lost: {list(lost_data.keys())}")
            elif action == 'remove':
                for sym_base in base_symbols_to_update:
                    if sym_base in lost_data:
                        del lost_data[sym_base]
                        updated_in_action.append(sym_base)
                if updated_in_action:
                    logger_instance.info(
                        f"{log_prefix}Removed from {os.path.basename(file_path)}: {updated_in_action}. Current lost: {list(lost_data.keys())}")
                elif base_symbols_to_update:  # Log if symbols were provided but none were found/removed
                    logger_instance.info(
                        f"{log_prefix}Symbols {base_symbols_to_update} not found in {os.path.basename(file_path)} for removal, or already removed.")

            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(lost_data, f, indent=4)

            return updated_in_action  # Return symbols actually added/removed

        except Exception as e:
            logger_instance.error(f"{log_prefix}Error updating lost symbols file {file_path}: {e}", exc_info=True)
            return []  # Return empty list on error


async def get_all_bybit_usdt_symbols(logger_instance):
    """Fetches all active Bybit SPOT USDT symbols and derives dump scales."""
    logger_instance.info("Attempting to fetch all active Bybit SPOT USDT symbols via CCXT...")
    candidate_ws_symbols = []
    derived_dump_scale_map = {}
    exchange = None
    try:
        exchange = ccxt_async.bybit({'enableRateLimit': True, 'timeout': 20000})
        await exchange.load_markets(True)
        raw_ccxt_symbols = exchange.symbols
        if raw_ccxt_symbols:
            for symbol_str in raw_ccxt_symbols:
                market = exchange.market(symbol_str)
                if market and market.get('spot') and market.get('quoteId', '').upper() == 'USDT' and market.get(
                        'active'):
                    candidate_ws_symbols.append(symbol_str.replace('/', ''))

        logger_instance.info(f"Identified {len(candidate_ws_symbols)} active SPOT USDT symbols.")

        # This part is for symbols without a pre-saved dump scale.
        established_successful_dumpscales_map = load_established_dump_scales()
        symbols_needing_derivation = [s for s in candidate_ws_symbols if
                                      s not in established_successful_dumpscales_map]
        if symbols_needing_derivation:
            logger_instance.info(f"Found {len(symbols_needing_derivation)} symbols needing dump scale derivation.")
            all_tickers = await exchange.fetch_tickers()
            for key, data in all_tickers.items():
                symbol = key.split(':')[0].replace('/', '')
                if symbol in symbols_needing_derivation:
                    derived_dump_scale_map[symbol] = get_decimal_places(data.get('last'))
            logger_instance.info(f"Derived dump scales for {len(derived_dump_scale_map)} symbols.")

    except Exception as e:
        logger_instance.critical(f"FATAL: CCXT setup/symbol fetch failed: {e}", exc_info=True)
        await send_telegram_notification(f"CRITICAL: CCXT setup/symbol fetch failed: {e}", level='CRITICAL')
        return [], {}  # Return empty on failure
    finally:
        if exchange:
            await exchange.close()

    return candidate_ws_symbols, derived_dump_scale_map


# --- WebSocket Communication and Data Handling (MODIFIED) ---
async def send_ping(websocket, symbol):
    while True:
        try:
            await websocket.send(json.dumps({"ping": int(time.time() * 1000), "params": {"binary": False}}))
            await asyncio.sleep(20)
        except:
            break


async def try_subscribe_with_dump_scale(websocket, symbol, current_ds_to_try):
    await websocket.send(json.dumps({"topic": "mergedDepth", "event": "sub", "symbol": symbol, "limit": 40,
                                     "params": {"binary": False, "dumpScale": current_ds_to_try}}))
    try:
        response = json.loads(await asyncio.wait_for(websocket.recv(), timeout=10))
        if response.get("code") == 0: return True, response, "Explicit ack"
        if response.get("topic") == "mergedDepth" and response.get("data"): return True, response, "Initial snapshot"
        is_ds_error = BYBIT_DUMPSCALE_ERROR_MSG_PART.lower() in response.get('desc', '').lower()
        if is_ds_error: return False, response, f"DumpScale error"
        return False, response, f"Bybit error (Code: {response.get('code')})"
    except Exception as e:
        return False, None, f"Exception: {type(e).__name__}"


# MODIFICATION: Worker function now accepts notifier and sends failure notifications
async def bybit_spot_orderbook_stream_for_symbol(symbol, initial_dump_scale_to_try):
    global successful_initial_connections, failed_final_connections, current_run_successful_details, established_successful_dumpscales_map
    ws_url = f"wss://ws2.bybit.com/spot/ws/quote/v2?_platform=2&tamp={int(time.time() * 1000)}"
    reconnection_attempts = 0
    last_exception = None  # Store the last exception for notification purposes

    # --- Status: mark connection as starting ---
    with bybit_status_lock:
        bybit_status_data.setdefault(symbol, {})
        bybit_status_data[symbol]['connection_alive'] = False
        bybit_status_data[symbol]['receiving_updates'] = False
        bybit_status_data[symbol]['last_update_time'] = 0.0

    while True:  # Loop indefinitely to always try to reconnect
        ping_task = None
        websocket_connection = None
        try:
            logger.info(f"[{symbol}] Attempting connection (Consecutive attempt {reconnection_attempts + 1})...")
            websocket_connection = await websockets.connect(ws_url, open_timeout=10, close_timeout=5,
                                                            ping_interval=None)
            current_ds_used = initial_dump_scale_to_try
            # --- Status: mark connection as alive ---
            with bybit_status_lock:
                bybit_status_data.setdefault(symbol, {})
                bybit_status_data[symbol]['connection_alive'] = True
                bybit_status_data[symbol]['receiving_updates'] = False
                bybit_status_data[symbol]['last_update_time'] = 0.0

            success, response_data, sub_details = await try_subscribe_with_dump_scale(websocket_connection, symbol,
                                                                                      current_ds_used)

            if not success and "DumpScale error" in sub_details:
                for retry_ds in DUMP_SCALE_RETRY_RANGE:
                    if retry_ds == current_ds_used: continue
                    logger.info(f"[{symbol}] Retrying subscription with dumpScale={retry_ds}...")
                    success, response_data, sub_details = await try_subscribe_with_dump_scale(websocket_connection,
                                                                                              symbol,
                                                                                              retry_ds)
                    current_ds_used = retry_ds
                    if success:
                        logger.info(f"[{symbol}] Successfully subscribed with ds={retry_ds} after dumpScale retry.")
                        break

            if success:
                logger.info(f"[{symbol}] SUCCESS (ds={current_ds_used}). Connection established.")
                # Only count as new successful_initial_connection if it's the first time or after a permanent failure state for this task instance (which we are trying to avoid)
                # This logic might need refinement based on how "successful_initial_connections" is truly defined.
                # For now, let's assume it's about the first ever success for this symbol in this run.
                # if symbol not in [s_tuple[0] for s_tuple in current_run_successful_details]: # A bit inefficient, better to use a set if many symbols
                #    async with counter_lock: successful_initial_connections += 1

                async with success_map_lock:  # Ensure this part is thread-safe if needed
                    # Update current run details and established scales
                    current_run_successful_details = [(s, d) for s, d in current_run_successful_details if
                                                      s != symbol]  # Remove old entry if any
                    current_run_successful_details.append((symbol, current_ds_used))
                    established_successful_dumpscales_map[symbol] = current_ds_used

                # If connection was previously lost, remove from lost symbols file
                base_symbol = get_bybit_base_symbol(symbol)
                await update_lost_symbols_file_bybit([base_symbol], 'remove', BYBIT_LOST_SYMBOLS_FILE, logger,
                                                     triggering_symbol=symbol)

                reconnection_attempts = 0  # Reset consecutive failures count on successful connection
                ping_task = asyncio.create_task(send_ping(websocket_connection, symbol))

                if sub_details == "Initial snapshot" and response_data:  # Process initial snapshot if received
                    snapshot = response_data["data"][0]
                    data_to_write = {"symbol": symbol, "timestamp_ms": snapshot.get("t"),
                                     "update_id": snapshot.get("v"),
                                     "bids": snapshot.get("b", []), "asks": snapshot.get("a", [])}
                    with open(os.path.join(OUTPUT_DIRECTORY, f"{symbol}.json"), 'w') as f: json.dump(data_to_write, f,
                                                                                                     indent=2)
                    # --- Status: update on initial snapshot ---
                    with bybit_status_lock:
                        bybit_status_data.setdefault(symbol, {})
                        bybit_status_data[symbol]['last_update_time'] = time.time()
                        bybit_status_data[symbol]['receiving_updates'] = True

                # Main message processing loop
                try:
                    async for message_str in websocket_connection:
                        message = json.loads(message_str)
                        if "topic" in message and message["topic"] == "mergedDepth" and "data" in message:
                            snapshot = message["data"][0]
                            if snapshot.get("s") == symbol:
                                data_to_write = {"symbol": symbol, "timestamp_ms": snapshot.get("t"),
                                                 "update_id": snapshot.get("v"), "bids": snapshot.get("b", []),
                                                 "asks": snapshot.get("a", [])}
                                with open(os.path.join(OUTPUT_DIRECTORY, f"{symbol}.json"), 'w') as f: json.dump(
                                    data_to_write, f, indent=2)
                                # --- Status: update on every orderbook update ---
                                with bybit_status_lock:
                                    bybit_status_data.setdefault(symbol, {})
                                    bybit_status_data[symbol]['last_update_time'] = time.time()
                                    bybit_status_data[symbol]['receiving_updates'] = True
                except websockets.exceptions.ConnectionClosedError as e_closed_err:
                    last_exception = e_closed_err
                    logger.warning(
                        f"[{symbol}] Connection closed during streaming (Error): {e_closed_err}. Will attempt to reconnect.")
                    raise  # Re-raise to be caught by the main retry handler below
                except websockets.exceptions.ConnectionClosedOK as e_closed_ok:
                    last_exception = e_closed_ok
                    logger.info(
                        f"[{symbol}] Connection closed gracefully by server (OK): {e_closed_ok}. Will attempt to reconnect.")
                    raise  # Re-raise to be caught by the main retry handler below
                # If the message loop exits without a websockets exception (e.g. server just stops sending, or other logic breaks out)
                logger.info(f"[{symbol}] WebSocket stream ended or was interrupted. Will attempt to reconnect.")
                # No return here, will loop to reconnect

            else:  # Initial subscription failed (not a dumpScale error, or dumpScale retries also failed)
                # This case should ideally be covered by try_subscribe_with_dump_scale raising an error or returning specific details
                # For safety, ensure it's treated as a connection failure to trigger retry logic.
                raise ConnectionError(f"Subscription failed after attempts: {sub_details}")

        except (websockets.exceptions.WebSocketException, ConnectionRefusedError, asyncio.TimeoutError, ConnectionError,
                OSError) as e:  # Catch common retryable errors
            last_exception = e  # Store the exception for potential notification
            # --- Status: mark connection as dead ---
            with bybit_status_lock:
                bybit_status_data.setdefault(symbol, {})
                bybit_status_data[symbol]['connection_alive'] = False
                bybit_status_data[symbol]['receiving_updates'] = False
                bybit_status_data[symbol]['last_update_time'] = bybit_status_data[symbol].get('last_update_time', 0.0)
            logger.warning(
                f"[{symbol}] Connection attempt {reconnection_attempts + 1} failed: {type(e).__name__} - {str(e)[:150]}")
            reconnection_attempts += 1

            base_symbol = get_bybit_base_symbol(symbol)
            await update_lost_symbols_file_bybit([base_symbol], 'add', BYBIT_LOST_SYMBOLS_FILE, logger,
                                                 triggering_symbol=symbol)

            if reconnection_attempts == MAX_RECONNECTION_ATTEMPTS:  # Send notification once when this threshold is hit
                logger.error(
                    f"[{symbol}] Reached {MAX_RECONNECTION_ATTEMPTS} consecutive failed attempts. Sending critical notification, but WILL CONTINUE RETRYING.")
                await send_telegram_notification(
                    f"CRITICAL: Still unable to connect to `{symbol}` after {MAX_RECONNECTION_ATTEMPTS} consecutive attempts. Will continue retrying indefinitely.\n\n"
                    f"**Last Error:** `{type(last_exception).__name__}: {str(last_exception)[:200]}`",
                    level='CRITICAL'
                )

            logger.info(
                f"[{symbol}] Retrying in {RECONNECTION_DELAY_SECONDS} seconds... (Next attempt: {reconnection_attempts + 1}) ")
            await asyncio.sleep(RECONNECTION_DELAY_SECONDS)
            # Loop continues for another attempt

        except asyncio.CancelledError:
            logger.warning(f"[{symbol}] Task cancelled, likely for scheduled reconnect. Exiting worker.")
            # --- Status: mark connection as dead ---
            with bybit_status_lock:
                bybit_status_data.setdefault(symbol, {})
                bybit_status_data[symbol]['connection_alive'] = False
                bybit_status_data[symbol]['receiving_updates'] = False
                bybit_status_data[symbol]['last_update_time'] = bybit_status_data[symbol].get('last_update_time', 0.0)
            break  # Exit the while True loop, terminating the task.

        except Exception as e:  # Catch all other non-specific exceptions (potentially non-retryable)
            last_exception = e
            async with counter_lock:
                failed_final_connections += 1  # This connection is considered finally failed for this task instance
            logger.critical(
                f"[{symbol}] CRITICAL UNHANDLED FAIL (terminating for this symbol): {type(e).__name__} - {str(e)[:150]}. Total final failed: {failed_final_connections}",
                exc_info=True)
            base_symbol = get_bybit_base_symbol(symbol)
            await update_lost_symbols_file_bybit([base_symbol], 'add', BYBIT_LOST_SYMBOLS_FILE, logger,
                                                 triggering_symbol=symbol)
            await send_telegram_notification(
                f"CRITICAL: Unhandled exception for symbol `{symbol}`. Connection attempts for this symbol will stop.\n\n"
                f"**Error:** `{type(last_exception).__name__}: {str(last_exception)[:200]}`",
                level='CRITICAL'
            )
            # --- Status: mark connection as dead ---
            with bybit_status_lock:
                bybit_status_data.setdefault(symbol, {})
                bybit_status_data[symbol]['connection_alive'] = False
                bybit_status_data[symbol]['receiving_updates'] = False
                bybit_status_data[symbol]['last_update_time'] = bybit_status_data[symbol].get('last_update_time', 0.0)
            break  # Exit the while True loop for this symbol, as it's a non-retryable or unhandled critical error

        finally:
            if ping_task and not ping_task.done():
                ping_task.cancel()
                try:
                    await ping_task  # Allow cancellation to complete
                except asyncio.CancelledError:
                    logger.debug(f"[{symbol}] Ping task successfully cancelled.")
            if websocket_connection and not websocket_connection.closed:
                await websocket_connection.close()
            logger.debug(f"[{symbol}] Cleaned up resources for connection attempt.")

    # This part is reached only if the 'break' from the unhandled exception block is hit.
    logger.error(
        f"[{symbol}] Permanently failed to connect and stopped retrying for this symbol due to a critical unhandled error: {type(last_exception).__name__} - {str(last_exception)[:150]}.")
    # --- Status: mark connection as dead ---
    with bybit_status_lock:
        bybit_status_data.setdefault(symbol, {})
        bybit_status_data[symbol]['connection_alive'] = False
        bybit_status_data[symbol]['receiving_updates'] = False
        bybit_status_data[symbol]['last_update_time'] = bybit_status_data[symbol].get('last_update_time', 0.0)


# MODIFICATION: main() is now async and has a reconnect loop
async def main():
    global active_websocket_tasks, total_symbols_identified_as_candidates, established_successful_dumpscales_map
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
    os.makedirs(LOST_CONNECTIONS_DIR, exist_ok=True)

    await send_telegram_notification("Bybit Market Monitor script is starting up...", level='INFO')

    # Start the status updater background task
    status_task = asyncio.create_task(bybit_status_updater())

    # Load established dump scales once at the start
    established_successful_dumpscales_map = load_established_dump_scales()

    # Fetch all symbols and their derived scales once at the start
    candidate_ws_symbols, derived_dump_scale_map = await get_all_bybit_usdt_symbols(logger)
    total_symbols_identified_as_candidates = len(candidate_ws_symbols)

    if not candidate_ws_symbols:
        logger.critical("No candidate symbols found. Shutting down.")
        return  # Exit if we can't get any symbols

    # --- Start Telegram Command Listener ---
    telegram_listener = TelegramCommandListener(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, logger)
    telegram_task = asyncio.create_task(telegram_listener.poll())

    # The main reconnection loop
    while True:
        active_websocket_tasks = []  # Reset tasks for this cycle

        # Determine which symbols to connect to for this cycle
        symbols_for_tasks = candidate_ws_symbols[:min(len(candidate_ws_symbols), MAX_CONCURRENT_CONNECTIONS_TO_ATTEMPT)]

        # Launch all connection tasks
        for symbol in symbols_for_tasks:
            initial_ds = established_successful_dumpscales_map.get(symbol,
                                                                   derived_dump_scale_map.get(symbol,
                                                                                              DEFAULT_DUMP_SCALE))
            task = asyncio.create_task(bybit_spot_orderbook_stream_for_symbol(symbol, initial_ds))
            active_websocket_tasks.append(task)
            await asyncio.sleep(CONNECTION_ATTEMPT_DELAY)

        logger.info(
            f"All {len(symbols_for_tasks)} connection tasks launched. Next reconnect in {RECONNECT_PERIOD_MINUTES} minutes.")
        await send_telegram_notification(
            f"Bybit Market Monitor Started/Reconnected.\n\n"
            f"• **Attempting:** `{len(symbols_for_tasks)}` connections\n"
            f"• **Total Symbols:** `{total_symbols_identified_as_candidates}`\n"
            f"• **Next Reconnect:** `{RECONNECT_PERIOD_MINUTES}` minutes",
            level='INFO'
        )

        # Wait for the reconnect period to elapse or a /restart command
        logger.info(f"Main loop is now sleeping for {RECONNECT_PERIOD_MINUTES} minutes or until /restart command.")
        try:
            done, pending = await asyncio.wait([
                asyncio.create_task(asyncio.sleep(RECONNECT_PERIOD_MINUTES * 60)),
                asyncio.create_task(telegram_listener.restart_event.wait())
            ], return_when=asyncio.FIRST_COMPLETED)
        except Exception as e:
            logger.error(f"Error in main wait: {e}")
            done, pending = [], []

        # If /restart was triggered
        if telegram_listener.restart_event.is_set():
            delay = telegram_listener.restart_delay or 0
            logger.warning(f"/restart triggered. Cancelling all tasks and waiting {delay} seconds before reconnect.")
            await send_telegram_notification(f"/restart: Cancelling all tasks and waiting {delay} seconds before reconnect.", level='WARNING')
            telegram_listener.restart_event.clear()
        else:
            delay = 0
            logger.warning(
                f"RECONNECT: Scheduled period of {RECONNECT_PERIOD_MINUTES} minutes ended. Initiating reconnect.")
            await send_telegram_notification(
                f"Initiating scheduled reconnection for all {len(symbols_for_tasks)} Bybit connections.",
                level='WARNING'
            )

        # Mark all symbols as 'lost' before disconnecting to indicate a planned outage
        all_base_symbols = [get_bybit_base_symbol(s) for s in symbols_for_tasks]
        if all_base_symbols:
            await update_lost_symbols_file_bybit(all_base_symbols, 'add', BYBIT_LOST_SYMBOLS_FILE, logger,
                                                 triggering_symbol="SCHEDULED_RECONNECT")

        # Cancel all running websocket tasks
        logger.info(f"RECONNECT: Cancelling {len(active_websocket_tasks)} active tasks.")
        for task in active_websocket_tasks:
            task.cancel()

        # Wait for all tasks to acknowledge cancellation and finish cleanup
        await asyncio.gather(*active_websocket_tasks, return_exceptions=True)

        save_current_run_successful_to_csv()

        if delay > 0:
            logger.info(f"Waiting {delay} seconds before reconnecting as per /restart command.")
            await asyncio.sleep(delay)

        logger.info("RECONNECT: All tasks terminated. Restarting connection cycle in 5 seconds...")
        await asyncio.sleep(5)  # Brief pause before restarting the loop

    # On exit, cancel the status updater
    status_task.cancel()
    await status_task


# --- UNCHANGED utility functions for saving and summary ---
def save_current_run_successful_to_csv():
    if current_run_successful_details:
        for symbol, ds in current_run_successful_details:
            established_successful_dumpscales_map[symbol] = ds
    if not established_successful_dumpscales_map: return
    unique_sorted = sorted(list(established_successful_dumpscales_map.items()))
    try:
        with open(SUCCESSFUL_DUMPSCALES_CSV_FILE, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Symbol', 'SuccessfulDumpScale'])
            writer.writerows(unique_sorted)
        logger.info(f"Saved {len(unique_sorted)} dumpScale pairs to {SUCCESSFUL_DUMPSCALES_CSV_FILE}.")
    except IOError as e:
        logger.error(f"Error saving dump scales to CSV: {e}")


def print_summary():
    logger.info("\n" + "=" * 30 + " SUMMARY " + "=" * 30)
    logger.info(f"Total symbols identified: {total_symbols_identified_as_candidates}")
    logger.info(f"Attempted to connect: {len(active_websocket_tasks)}")
    logger.info(f"Successful connections: {successful_initial_connections}")
    logger.info(f"Failed connections: {failed_final_connections}")
    logger.info(f"Total established dumpScales: {len(established_successful_dumpscales_map)}")
    logger.info("=" * 70)


def main_entry_point_bybit():
    """
    Main entry-point function
    :return:
    """
    try:
        # MODIFICATION: main() is now async
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script interrupted by user.")
    except Exception as e:
        logger.critical(f"FATAL SCRIPT ERROR: {e}", exc_info=True)
        # Final attempt to notify
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(
            send_telegram_notification(f"FATAL SCRIPT CRASH: {type(e).__name__} - {str(e)[:200]}", level='CRITICAL'))
    finally:
        print_summary()
        save_current_run_successful_to_csv()
        logger.info("Script execution finished.")


# --- Telegram Command Listener ---
class TelegramCommandListener:
    def __init__(self, token, chat_id, logger):
        self.token = token
        self.chat_id = chat_id
        self.logger = logger
        self.restart_event = asyncio.Event()
        self.restart_delay = None
        self.last_update_id = None

    async def poll(self):
        url = f"https://api.telegram.org/bot{self.token}/getUpdates"
        self.logger.info("Telegram command listener started.")
        while True:
            try:
                params = {'timeout': 30, 'allowed_updates': ['message']}
                if self.last_update_id:
                    params['offset'] = self.last_update_id + 1
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=35) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            for update in data.get('result', []):
                                self.last_update_id = update['update_id']
                                message = update.get('message', {})
                                text = message.get('text', '')
                                chat = message.get('chat', {})
                                if str(chat.get('id')) == str(self.chat_id):
                                    await self.handle_command(text)
            except Exception as e:
                self.logger.error(f"Telegram polling error: {e}")
            await asyncio.sleep(2)

    async def handle_command(self, text):
        if text.startswith('/restart'):
            parts = text.strip().split()
            delay = 0
            if len(parts) > 1:
                try:
                    delay = int(parts[1])
                except Exception:
                    delay = 0
            self.logger.warning(f"/restart command received from Telegram. Will restart in {delay} seconds.")
            await send_telegram_notification(f"/restart command received. Restarting all connections in {delay} seconds.", level='WARNING')
            self.restart_delay = delay
            self.restart_event.set()


# --- Status Tracking ---
BYBIT_STATUS_FILE = os.path.join('statuses', 'bybit_status.json')
bybit_status_data = {}
bybit_status_lock = threading.Lock()

# --- Status Updater Task ---
async def bybit_status_updater():
    while True:
        try:
            with bybit_status_lock:
                os.makedirs(os.path.dirname(BYBIT_STATUS_FILE), exist_ok=True)
                # Convert to the required structure
                symbols_status = {}
                for symbol, data in bybit_status_data.items():
                    symbols_status[symbol.lower()] = {
                        'last_update_time': data.get('last_update_time', 0.0),
                        'receiving_updates': data.get('receiving_updates', False),
                        'connection_alive': data.get('connection_alive', False)
                    }
                # Write as {"symbols": {...}}
                with open(BYBIT_STATUS_FILE, 'w') as f:
                    json.dump({'symbols': symbols_status}, f, indent=2)
        except Exception as e:
            logger.error(f"Error writing bybit status file: {e}")
        await asyncio.sleep(1)


if __name__ == "__main__":
    # Initialize logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('BybitMarketMonitor')

    # Ensure output directories exist
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
    os.makedirs(LOST_CONNECTIONS_DIR, exist_ok=True)

    # Start the main entry point for Bybit monitoring
    main_entry_point_bybit()