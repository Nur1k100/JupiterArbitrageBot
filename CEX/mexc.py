#!/usr/bin/env python
import asyncio
import websockets
import json
import time
import os
import logging
import sys
import datetime
from typing import List, Dict, Optional, Any, Tuple
import ccxt
import aiohttp
import traceback

# --- Early Path Add for Protobuf ---
_current_script_directory = os.path.dirname(os.path.abspath(__file__))
_pb_dir = os.path.join(_current_script_directory, 'generated_python_pb')
if os.path.isdir(_pb_dir):
    if _pb_dir not in sys.path:
        sys.path.insert(0, _pb_dir)

PROTOBUF_AVAILABLE = False
try:
    import PushDataV3ApiWrapper_pb2
    import PublicLimitDepthsV3Api_pb2

    PROTOBUF_AVAILABLE = True
except ImportError as e:
    _initial_protobuf_error = e

# --- Configuration ---
MEXC_WEBSOCKET_URI = "wss://wbs-api.mexc.com/ws"
PROXIES_FILE = "proxies.txt"
OUTPUT_DIRECTORY = "CEX/orderbooks/mexc_orderbooks"
LOST_CONNECTIONS_DIR = "CEX/lost_connections"  # New directory for lost connection info
MEXC_LOST_SYMBOLS_FILE = os.path.join(LOST_CONNECTIONS_DIR, "mexc_lost.json")  # New file
PING_INTERVAL_SECONDS = 25
MEXC_PING_METHOD = {"method": "PING"}
MAX_SYMBOLS_PER_CONNECTION = 30
DEPTH_LEVEL = 20
RECONNECT_DELAY_SECONDS = 5

# --- Telegram Configuration ---
TELEGRAM_BOT_TOKEN = ""
TELEGRAM_CHAT_ID = 1
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
MAX_CONSECUTIVE_CONNECTION_FAILURES_FOR_ALERT = 5
MAX_TELEGRAM_MSG_LENGTH = 4096

# Ensure lost_connections directory exists
# os.makedirs(LOST_CONNECTIONS_DIR, exist_ok=True) # This was moved to be defined after LOST_CONNECTIONS_DIR

# --- Logging Setup ---
# MODIFICATION: Added MAIN_LOG_FILE for comprehensive logging
MAIN_LOG_FILE = "CEX/app_log/mexc_appX.log"
ERROR_LOG_FILE = "CEX/logging_cex/mexc_connector_errors.log"

LOG_FORMAT = '%(asctime)s [%(levelname)s] [%(name)s] %(message)s'

logging.getLogger("websockets").setLevel(logging.INFO)

# Set root logger to lowest level to allow handlers to filter
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)

# Console Handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
root_logger.addHandler(console_handler)

try:
    # Main log file handler (mexc_app.log) - Captures EVERYTHING
    main_file_handler = logging.FileHandler(MAIN_LOG_FILE, mode='a', encoding='utf-8')
    main_file_handler.setLevel(logging.DEBUG)
    main_file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    root_logger.addHandler(main_file_handler)

    # Error log file handler - Captures only ERRORS
    error_file_handler = logging.FileHandler(ERROR_LOG_FILE, mode='a', encoding='utf-8')
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    root_logger.addHandler(error_file_handler)
except Exception as e:
    print(f"CRITICAL: Failed to configure file loggers: {e}")

logger = logging.getLogger(__name__)  # Get logger for the current module

if not TELEGRAM_ENABLED:
    logger.warning("Telegram notifications are DISABLED.")
else:
    logger.info("Telegram notifications are ENABLED.")

# --- Lost Symbols Management ---
_lost_symbols_lock = asyncio.Lock()
# LOST_CONNECTIONS_DIR is already defined in a previous step
# MEXC_LOST_SYMBOLS_FILE is already defined in a previous step

# Ensure lost_connections directory exists at module load time (if not already created by previous step)
if not os.path.exists(LOST_CONNECTIONS_DIR):
    os.makedirs(LOST_CONNECTIONS_DIR, exist_ok=True)


def get_base_symbols_from_batch(symbols_batch: List[str]) -> List[str]:
    """Extracts base symbols (e.g., BTC from BTCUSDT)."""
    base_symbols = set()
    for s in symbols_batch:
        if isinstance(s, str) and s.endswith("USDT"):
            base_symbols.add(s[:-4])
        elif isinstance(s, str) and s.count('_') == 1 and s.endswith("USDT"):  # Handles symbols like BTC_USDT
            base_symbols.add(s.split('_')[0])
        elif isinstance(s, str):  # Fallback for other formats or non-USDT pairs
            # Attempt to remove common quote currencies if symbol seems like a pair
            common_quotes = ["USDT", "USDC", "BTC", "ETH", "USD"]
            original_symbol = s
            for quote in common_quotes:
                if s.endswith(quote) and len(s) > len(quote):
                    s = s[:-len(quote)]
                    break
            base_symbols.add(s)
    return list(base_symbols)


async def update_lost_symbols_file(
        base_symbols: List[str],
        action: str,  # 'add' or 'remove'
        file_path: str,
        logger_instance: logging.Logger,
        connection_id: Optional[str] = None
):
    async with _lost_symbols_lock:
        log_prefix = f"[{connection_id}] " if connection_id else ""
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)  # Ensure directory exists just in case
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
                except FileNotFoundError:
                    logger_instance.info(f"{log_prefix}{file_path} not found. Will create a new one.")
                    lost_data = {}

            if action == 'add':
                for sym in base_symbols:
                    lost_data[sym] = "lost"
                if base_symbols:  # Only log if there are symbols to add
                    logger_instance.info(
                        f"{log_prefix}Added to {os.path.basename(file_path)}: {base_symbols}. Current lost: {list(lost_data.keys())}")
            elif action == 'remove':
                removed_symbols_this_action = []
                for sym in base_symbols:
                    if sym in lost_data:
                        del lost_data[sym]
                        removed_symbols_this_action.append(sym)
                if removed_symbols_this_action:
                    logger_instance.info(
                        f"{log_prefix}Removed from {os.path.basename(file_path)}: {removed_symbols_this_action}. Current lost: {list(lost_data.keys())}")
                elif base_symbols:  # Log if symbols were provided but none were found/removed
                    logger_instance.info(
                        f"{log_prefix}Symbols {base_symbols} not found in {os.path.basename(file_path)} for removal, or already removed.")

            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(lost_data, f, indent=4)

        except Exception as e:
            logger_instance.error(f"{log_prefix}Error updating lost symbols file {file_path}: {e}", exc_info=True)


# --- Telegram Sender Function ---
# MODIFICATION: Enhanced function to handle levels and formatting
async def send_telegram_message(message: str, level: str = 'INFO', session: Optional[aiohttp.ClientSession] = None):
    if not TELEGRAM_ENABLED:
        return

    telegram_logger = logger.getChild("telegram")  # Use a child logger for Telegram messages

    level_emoji = {'INFO': '✅', 'WARNING': '🟡', 'CRITICAL': '🔴'}
    if "re-established" in message.lower():
        level_emoji['INFO'] = '🟢'
    if "starting up" in message.lower():
        level_emoji['INFO'] = '🚀'

    formatted_message = f"{level_emoji.get(level, '🔵')} **MEXC WebSocket Monitor**\n\n{message}"
    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    if len(formatted_message) > MAX_TELEGRAM_MSG_LENGTH:
        formatted_message = formatted_message[:MAX_TELEGRAM_MSG_LENGTH - 100] + "\n... (message truncated)"

    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': formatted_message, 'parse_mode': 'Markdown'}

    _session: Optional[aiohttp.ClientSession] = None
    try:
        _session = session if session else aiohttp.ClientSession()
        async with _session.post(api_url, json=payload, timeout=10) as response:
            if response.status != 200:
                response_text = await response.text()
                telegram_logger.error(  # Changed to telegram_logger
                    f"Failed to send Telegram message. Status: {response.status}, Response: {response_text}")
    except Exception as e:
        telegram_logger.error(f"Unexpected error sending Telegram message: {e}",
                              exc_info=True)  # Changed to telegram_logger
    finally:
        if not session and _session:
            await _session.close()


# --- CCXT and other utility functions (UNCHANGED) ---
def get_mexc_usdt_tickers(reload_markets: bool = False, timeout_ms=30000, max_retries=3, retry_delay_s=10) -> Tuple[
    List[str], Optional[str]]:
    # This function remains unchanged.
    ccxt_logger = logger.getChild("ccxt_fetch")
    ccxt_logger.info(f"Attempting to fetch MEXC USDT tickers (Reload: {reload_markets})...")
    exchange = ccxt.mexc({'timeout': timeout_ms, 'enableRateLimit': True})
    telegram_error_msg: Optional[str] = None
    try:
        markets_data = exchange.load_markets(reload_markets)
        filtered_symbols = []
        for symbol_ccxt, market_info in markets_data.items():
            if market_info.get('spot', False) and market_info.get('quote', '').upper() == 'USDT' and market_info.get(
                    'active', True):
                filtered_symbols.append(market_info.get('id', ''))
        return filtered_symbols, None
    except Exception as e:
        err_msg = f"Error fetching MEXC markets: {e}"
        ccxt_logger.error(err_msg, exc_info=True)
        return [], err_msg


def load_proxies(filename: str) -> List[Optional[str]]:
    # This function remains unchanged.
    try:
        with open(filename, 'r') as f:
            proxies = [line.strip() for line in f if line.strip()]
        if proxies: return proxies
    except FileNotFoundError:
        pass
    return [None]


async def send_mexc_pings(websocket: websockets.WebSocketClientProtocol, connection_id: str,
                          shutdown_event: asyncio.Event):
    # This function remains unchanged.
    ping_logger = logger.getChild(f"{connection_id}.ping")
    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=PING_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            try:
                if not websocket.closed: await websocket.send(json.dumps(MEXC_PING_METHOD))
            except:
                break
        except asyncio.CancelledError:
            break


# --- Connection Worker (MODIFIED FOR NOTIFICATIONS) ---
async def subscribe_to_mexc_depth_batch(
        symbols_batch: List[str], depth: int, proxy: Optional[str],
        connection_id: str, shutdown_event: asyncio.Event,
        http_session: aiohttp.ClientSession
):
    task_logger = logger.getChild(connection_id)
    current_base_symbols = get_base_symbols_from_batch(symbols_batch)

    all_target_channels = [f"spot@public.limit.depth.v3.api.pb@{s}@{depth}" for s in symbols_batch]
    channel_to_symbol_map = {ch: sym for sym, ch in zip(symbols_batch, all_target_channels)}
    SUBSCRIPTION_BATCH_SIZE = 10
    ping_task: Optional[asyncio.Task] = None
    connect_timeout, close_timeout = 30, 10

    consecutive_connection_failures = 0

    while not shutdown_event.is_set():
        websocket_connection: Optional[websockets.WebSocketClientProtocol] = None
        last_exception: Optional[Exception] = None
        connection_established_this_iteration = False
        try:
            task_logger.info(
                f"Attempting connect (Proxy: {proxy if proxy else 'Direct'}) for {len(symbols_batch)} symbols ({', '.join(current_base_symbols[:3])}{'...' if len(current_base_symbols) > 3 else ''})."
            )

            websocket_connection = await websockets.connect(
                MEXC_WEBSOCKET_URI, open_timeout=connect_timeout, close_timeout=close_timeout,
                ping_interval=None, compression=None
            )
            connection_established_this_iteration = True

            if consecutive_connection_failures > 0:  # This means it's a re-establishment
                task_logger.info(
                    f"Connection re-established for {connection_id}. Recovered symbols: {', '.join(current_base_symbols)}")
                if current_base_symbols:  # Only update/notify if there are symbols
                    await update_lost_symbols_file(current_base_symbols, 'remove', MEXC_LOST_SYMBOLS_FILE, task_logger,
                                                   connection_id)
                    await send_telegram_message(
                        f"Worker `{connection_id}` re-established. Recovered symbols: {', '.join(current_base_symbols)}",
                        level='INFO', session=http_session
                    )

            consecutive_connection_failures = 0  # Reset on successful connection

            task_logger.info(f"Connected. Subscribing to {len(all_target_channels)} channels for {connection_id}...")

            for i in range(0, len(all_target_channels), SUBSCRIPTION_BATCH_SIZE):
                if shutdown_event.is_set(): break
                await websocket_connection.send(json.dumps(
                    {"method": "SUBSCRIPTION", "params": all_target_channels[i:i + SUBSCRIPTION_BATCH_SIZE]}))
                await asyncio.sleep(0.2)  # Keep small delay

            ping_task = asyncio.create_task(send_mexc_pings(websocket_connection, connection_id, shutdown_event))

            async for message_raw in websocket_connection:
                if shutdown_event.is_set(): break
                if isinstance(message_raw, bytes):
                    wrapper = PushDataV3ApiWrapper_pb2.PushDataV3ApiWrapper()
                    wrapper.ParseFromString(message_raw)
                    if wrapper.channel in channel_to_symbol_map:
                        sym_for_file = channel_to_symbol_map[wrapper.channel]
                        if wrapper.HasField("publicLimitDepths"):
                            depth_pb = wrapper.publicLimitDepths
                            data = {"symbol": sym_for_file, "timestamp_ms": wrapper.sendTime,
                                    "update_id": depth_pb.version,
                                    "bids": [[b.price, b.quantity] for b in depth_pb.bids],
                                    "asks": [[a.price, a.quantity] for a in depth_pb.asks]}
                            out_file = os.path.join(OUTPUT_DIRECTORY, f"{sym_for_file}.json")
                            with open(out_file, 'w') as f: json.dump(data, f, indent=2)

        except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError, OSError) as e:
            last_exception = e
            if not shutdown_event.is_set(): task_logger.warning(f"Connection issue: {type(e).__name__} - {e}")
        except Exception as e:
            last_exception = e
            if not shutdown_event.is_set(): task_logger.error(
                f"Unexpected error in connection phase for {connection_id}.", exc_info=True)

        finally:
            if ping_task and not ping_task.done(): ping_task.cancel()
            if websocket_connection and websocket_connection.open:
                try:
                    await websocket_connection.close()
                except Exception as e_close:
                    if not shutdown_event.is_set(): task_logger.warning(
                        f"Error closing websocket for {connection_id}: {e_close}")

            if shutdown_event.is_set():
                task_logger.info(f"Task {connection_id} shutting down as per event.");
                break

            is_currently_disconnected = not (
                        connection_established_this_iteration and websocket_connection and websocket_connection.open)

            if is_currently_disconnected:
                consecutive_connection_failures += 1
                log_msg = (
                    f"Worker `{connection_id}` disconnected. Error: {last_exception}. "
                    f"Failures: {consecutive_connection_failures}/{MAX_CONSECUTIVE_CONNECTION_FAILURES_FOR_ALERT}."
                )
                if current_base_symbols:
                    log_msg += f" Marking symbols as lost: {', '.join(current_base_symbols)}"
                task_logger.warning(log_msg)

                if current_base_symbols:
                    await update_lost_symbols_file(current_base_symbols, 'add', MEXC_LOST_SYMBOLS_FILE, task_logger,
                                                   connection_id)

                    await send_telegram_message(
                        f"Worker `{connection_id}` lost connection. Symbols: {', '.join(current_base_symbols)}. "
                        f"Error: `{last_exception}`. Attempting reconnect ({consecutive_connection_failures}).",
                        level='WARNING', session=http_session
                    )

                if consecutive_connection_failures >= MAX_CONSECUTIVE_CONNECTION_FAILURES_FOR_ALERT:
                    error_details = traceback.format_exc() if last_exception and isinstance(last_exception,
                                                                                            Exception) else str(
                        last_exception)
                    critical_msg_parts = [
                        f"CRITICAL failure for worker `{connection_id}`. Cannot re-establish after {consecutive_connection_failures} attempts."
                    ]
                    if current_base_symbols:
                        critical_msg_parts.append(f"**Lost Symbols:** {', '.join(current_base_symbols)}")
                    critical_msg_parts.append(f"**Last Error:**\n```\n{last_exception}\n```")
                    critical_msg_parts.append(f"**Traceback Snippet:**\n```\n{error_details[:1000]}\n```")

                    await send_telegram_message("\n\n".join(critical_msg_parts), level='CRITICAL', session=http_session)
                    consecutive_connection_failures = 0  # Reset to avoid spamming critical alerts for this continuous failure sequence

            await asyncio.sleep(RECONNECT_DELAY_SECONDS)


# --- Main Application Logic (MODIFIED FOR NOTIFICATIONS) ---
async def run_main_application_loop(http_session: aiohttp.ClientSession):
    active_ws_tasks: List[asyncio.Task] = []
    shutdown_event_global = asyncio.Event()

    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
    logger.info(f"Order book data will be saved in: {os.path.abspath(OUTPUT_DIRECTORY)}")

    if not PROTOBUF_AVAILABLE:
        # ... (unchanged)
        return

    proxies = load_proxies(PROXIES_FILE)
    startup_notification_sent = False

    try:
        while not shutdown_event_global.is_set():
            logger.info("Fetching/Refreshing ticker list from MEXC...")
            all_tickers, ccxt_error_msg = get_mexc_usdt_tickers(reload_markets=True)

            if ccxt_error_msg:
                await send_telegram_message(f"CRITICAL: Unable to fetch tickers from MEXC. Details: {ccxt_error_msg}",
                                            level='CRITICAL', session=http_session)

            if not all_tickers:
                logger.error("No tickers fetched. Retrying in 60s.")
                await asyncio.sleep(60);
                continue

            # Shutdown existing tasks before starting new ones
            if active_ws_tasks:
                logger.info(f"Signaling {len(active_ws_tasks)} old tasks to shut down for refresh...")
                shutdown_event_global.set()
                await asyncio.gather(*active_ws_tasks, return_exceptions=True)

            shutdown_event_global.clear()
            active_ws_tasks = []

            ticker_chunks = [all_tickers[i:i + MAX_SYMBOLS_PER_CONNECTION] for i in
                             range(0, len(all_tickers), MAX_SYMBOLS_PER_CONNECTION)]
            logger.info(f"Processing {len(all_tickers)} tickers in {len(ticker_chunks)} connection chunks.")

            for i, chunk in enumerate(ticker_chunks):
                if shutdown_event_global.is_set(): break
                proxy = proxies[i % len(proxies)]
                connection_id = f"conn-{i + 1}"
                task = asyncio.create_task(
                    subscribe_to_mexc_depth_batch(chunk, DEPTH_LEVEL, proxy, connection_id, shutdown_event_global,
                                                  http_session))
                active_ws_tasks.append(task)
                await asyncio.sleep(0.2)

            logger.info(f"Launched all {len(active_ws_tasks)} WebSocket tasks for this cycle.")

            # MODIFICATION: Send detailed success notification after first launch
            if not startup_notification_sent:
                success_message = (
                    f"MEXC Market Monitor Started Successfully!\n\n"
                    f"• **Tracking:** `{len(all_tickers)}` symbols\n"
                    f"• **Connections:** `{len(ticker_chunks)}` workers launched\n"
                    f"• **Topics/Connection:** `{MAX_SYMBOLS_PER_CONNECTION}`"
                )
                await send_telegram_message(success_message, level='INFO', session=http_session)
                startup_notification_sent = True

            # Wait for the next refresh cycle
            await asyncio.sleep(3600)  # Simple hourly refresh

    finally:
        logger.info("Main application loop ending. Shutting down tasks.")
        shutdown_event_global.set()
        if active_ws_tasks:
            await asyncio.gather(*active_ws_tasks, return_exceptions=True)


# --- main_with_session (MODIFIED FOR NOTIFICATIONS) ---
async def main_with_session():
    if not PROTOBUF_AVAILABLE:
        msg = f"FATAL (Startup - MEXC Connector):\nRequired Protobuf modules could not be imported.\nError: {_initial_protobuf_error}"
        logger.critical(msg.replace("\n", " - "), exc_info=_initial_protobuf_error)
        if TELEGRAM_ENABLED: await send_telegram_message(msg, level='CRITICAL')
        sys.exit(1)

    async with aiohttp.ClientSession() as http_session:
        # MODIFICATION: Immediate startup notification
        await send_telegram_message("MEXC Market Monitor script is starting up...", level='INFO', session=http_session)

        try:
            await run_main_application_loop(http_session)
        except Exception as e:
            error_message = f"FATAL ERROR (MEXC Connector - Unhandled Exception):\nType: {type(e).__name__}\nError: {e}"
            logger.critical(error_message.replace("\n", " - "), exc_info=True)
            await send_telegram_message(error_message, level='CRITICAL', session=http_session)


def main_entry_point_mexc():
    """
    Main entry point for MEXC Market Monitor.
    :return:
    """
    os.makedirs(os.path.dirname(ERROR_LOG_FILE), exist_ok=True)
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
    try:
        asyncio.run(main_with_session())
    except KeyboardInterrupt:
        logger.info("\nCtrl+C detected. Shutting down.")
    finally:
        logger.info("Program finished execution.")