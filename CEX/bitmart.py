#!/usr/bin/env python
import asyncio
import websockets
from websockets.client import WebSocketClientProtocol
import json
import time
import os
import logging
import logging.handlers
import sys
import zlib
from typing import List, Dict, Optional, Any, Tuple, Set, Literal  # MODIFICATION: Added Literal
import ccxt
import itertools
import aiohttp
from decimal import Decimal
import traceback

# --- Configuration ---
EXCHANGE_ID = 'bitmart'
MARKET_TYPE_TO_WATCH = 'spot'
QUOTE_ASSET = 'USDT'
BITMART_SPOT_WS_URL = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"
OUTPUT_DIRECTORY = "CEX/orderbooks/bitmart_orderbooks"

# MODIFICATION: Defined paths for both log files
MAIN_LOG_FILE = "CEX/app_log/bitmart_app.log"
ERROR_LOG_FILENAME = "CEX/logging_cex/bitmart_spot_webws_errors.log"

# BitMart specific limits
TOPICS_PER_CONNECTION_LIMIT = 100
MAX_CONNECTIONS_PER_IP = 20
DEPTH_LEVEL = 'spot/depth20'

# Timing configuration
PING_SERVER_INTERVAL_SECONDS = 15
CLIENT_PONG_TIMEOUT_SECONDS = 10
SUBSCRIPTION_DELAY_SECONDS = 0.05
CONNECTION_LAUNCH_DELAY_SECONDS = 0.5

# Reconnection configuration
RECONNECT_BASE_DELAY_SECONDS = 5
RECONNECT_MAX_DELAY_SECONDS = 300
RECONNECT_EXPONENTIAL_BASE = 2
MAX_CONSECUTIVE_CONNECTION_FAILURES_FOR_ALERT = 5

# MODIFICATION: Added constants for lost connections
LOST_CONNECTIONS_DIR = "CEX/lost_connections"  # Shared with other connectors
LOST_CONNECTIONS_FILE = os.path.join(LOST_CONNECTIONS_DIR, "bitmart_lost.json")

# Telegram configuration
TELEGRAM_BOT_TOKEN = ""
TELEGRAM_CHAT_ID = 1
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

# Processing limits
MAX_TOTAL_SYMBOLS_TO_PROCESS = None

# --- Logging Setup ---
# Set root logger to lowest level to allow handlers to filter
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s] %(message)s')
logging.getLogger("websockets").setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(log_formatter)
root_logger.addHandler(console_handler)

try:
    # Main log file handler (bitmart_app.log) - Captures EVERYTHING
    main_file_handler = logging.FileHandler(MAIN_LOG_FILE, mode='a', encoding='utf-8')
    main_file_handler.setLevel(logging.DEBUG)
    main_file_handler.setFormatter(log_formatter)
    root_logger.addHandler(main_file_handler)

    # Error log file handler - Captures only ERRORS
    error_file_handler = logging.FileHandler(ERROR_LOG_FILENAME, mode='a', encoding='utf-8')
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(log_formatter)
    root_logger.addHandler(error_file_handler)
except IOError as e:
    print(f"CRITICAL: Could not open log files: {e}. Logs will only go to console.", file=sys.stderr)

logger = logging.getLogger("BitMart_SPOT_WebWS_Client")  # Get logger for this module


# --- Telegram Notifier ---
class TelegramNotifier:
    def __init__(self):
        self.enabled = TELEGRAM_ENABLED
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.session = None
        self.logger = logger.getChild("telegram")

    async def initialize(self):
        if self.enabled and not self.session: self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session: await self.session.close()

    # MODIFICATION: Enhanced send_message with levels and formatting
    async def send_message(self, message: str, level: str = 'INFO', parse_mode: str = 'Markdown'):
        if not self.enabled: return
        level_emoji = {'INFO': '✅', 'WARNING': '🟡', 'CRITICAL': '🔴'}
        if "re-established" in message.lower(): level_emoji['INFO'] = '🟢'
        if "starting up" in message.lower(): level_emoji['INFO'] = '🚀'
        formatted_message = f"{level_emoji.get(level, '🔵')} **Bitmart WebSocket Monitor**\n\n{message}"
        try:
            if not self.session: await self.initialize()
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            if len(formatted_message) > 4096: formatted_message = formatted_message[:4093] + "..."
            data = {'chat_id': self.chat_id, 'text': formatted_message, 'parse_mode': parse_mode}
            async with self.session.post(url, json=data) as response:
                if response.status != 200: self.logger.error(f"Telegram API error: {await response.text()}")
        except Exception as e:
            self.logger.error(f"Failed to send Telegram message: {e}")


telegram_notifier = TelegramNotifier()


# --- Utility functions ---

# MODIFICATION: Added utility function to extract base asset
def extract_base_asset(bitmart_symbol_id: str) -> str:
    """
    Extracts the base asset from a BitMart market ID (e.g., "BTC_USDT" -> "BTC").
    """
    return bitmart_symbol_id.split('_')[0]


# MODIFICATION: Added utility function to manage the lost connections JSON file for BitMart
def manage_lost_connections_file_bitmart_sync(
        base_tokens: List[str],
        conn_id: str,  # For logging
        action: Literal['add', 'remove'],
        logger_instance: logging.Logger
) -> List[str]:  # Returns list of tokens whose state actually changed
    """
    Manages the bitmart_lost.json file.
    Adds or removes base tokens based on connection status.
    """
    os.makedirs(LOST_CONNECTIONS_DIR, exist_ok=True)

    lost_connections_data: Dict[str, str] = {}
    if os.path.exists(LOST_CONNECTIONS_FILE):
        try:
            with open(LOST_CONNECTIONS_FILE, 'r', encoding='utf-8') as f:
                content = f.read()
                if content.strip():
                    lost_connections_data = json.loads(content)
                else:
                    lost_connections_data = {}
        except json.JSONDecodeError:
            logger_instance.error(f"Worker {conn_id}: Error decoding {LOST_CONNECTIONS_FILE}. Re-initializing.")
            lost_connections_data = {}
        except Exception as e:
            logger_instance.error(f"Worker {conn_id}: Error reading {LOST_CONNECTIONS_FILE}: {e}. Assuming empty.")
            lost_connections_data = {}

    changed_tokens: List[str] = []

    if action == 'add':
        for token in base_tokens:
            if lost_connections_data.get(token) != "lost":
                lost_connections_data[token] = "lost"
                changed_tokens.append(token)
        if changed_tokens:
            logger_instance.info(f"Worker {conn_id}: Added {changed_tokens} to {LOST_CONNECTIONS_FILE}.")

    elif action == 'remove':
        for token in base_tokens:
            if token in lost_connections_data:
                del lost_connections_data[token]
                changed_tokens.append(token)
        if changed_tokens:
            logger_instance.info(f"Worker {conn_id}: Removed {changed_tokens} from {LOST_CONNECTIONS_FILE}.")

    try:
        with open(LOST_CONNECTIONS_FILE, 'w', encoding='utf-8') as f:
            json.dump(lost_connections_data, f, indent=2)
    except Exception as e:
        logger_instance.error(f"Worker {conn_id}: Error writing to {LOST_CONNECTIONS_FILE}: {e}")
    return changed_tokens


def decompress_data(data: bytes) -> str:
    try:
        decompress = zlib.decompressobj(-zlib.MAX_WBITS)
        inflated = decompress.decompress(data)
        inflated += decompress.flush()
        return inflated.decode('UTF-8')
    except Exception as e:
        logger.error(f"Error decompressing data: {e}");
        return None


def get_bitmart_spot_symbols(reload_markets: bool = False, timeout_ms=30000, max_retries=3, retry_delay_s=10) -> List[
    str]:
    ccxt_logger = logger.getChild("ccxt_fetch")
    ccxt_logger.info(f"Attempting to fetch BitMart {QUOTE_ASSET} spot symbols...")
    exchange = ccxt.bitmart({'timeout': timeout_ms, 'enableRateLimit': True, 'options': {'defaultType': 'spot'}})
    try:
        markets_data = exchange.load_markets(reload_markets)
        symbols = []
        for _, market_info in markets_data.items():
            if (market_info.get('spot', False) and market_info.get('active', True) and market_info.get('quoteId',
                                                                                                       '').upper() == QUOTE_ASSET):
                ws_symbol = market_info.get('id')
                if ws_symbol and not ws_symbol.startswith('$') and not ws_symbol.endswith(':USDT'):
                    symbols.append(ws_symbol[:-5] if ws_symbol.endswith('_SPBL') else ws_symbol)
        ccxt_logger.info(f"Found {len(symbols)} active {QUOTE_ASSET} spot pairs.")
        return symbols
    except Exception as e:
        ccxt_logger.error(f"Error loading BitMart markets: {e}", exc_info=True)
        return []


async def send_bitmart_pings(websocket: WebSocketClientProtocol, shutdown_event: asyncio.Event, connection_id: str):
    ping_logger = logger.getChild(f"{connection_id}.ping")
    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=PING_SERVER_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            try:
                if not websocket.closed: await websocket.send("ping")
            except:
                break
        except asyncio.CancelledError:
            break


# --- Manage BitMart WebSocket Connection (MODIFIED FOR NOTIFICATIONS) ---
async def manage_bitmart_spot_connection(symbols: List[str], connection_id: str, shutdown_event: asyncio.Event):
    conn_logger = logger.getChild(f"conn.{connection_id}")
    # MODIFICATION: Extract base tokens for this batch
    base_tokens_in_batch = sorted(list(set([extract_base_asset(s) for s in symbols])))

    consecutive_failures = 0
    reconnect_delay = RECONNECT_BASE_DELAY_SECONDS

    while not shutdown_event.is_set():
        websocket: Optional[WebSocketClientProtocol] = None
        ping_task: Optional[asyncio.Task] = None
        last_exception: Optional[Exception] = None

        try:
            conn_logger.info(f"Attempting to connect for {len(symbols)} symbols...")
            async with websockets.connect(BITMART_SPOT_WS_URL, open_timeout=20, close_timeout=10, ping_interval=None,
                                          compression=None) as websocket:
                # MODIFICATION: Handle re-establishment notification
                if consecutive_failures > 0:
                    conn_logger.info("Connection re-established.")
                    removed_tokens = manage_lost_connections_file_bitmart_sync(base_tokens_in_batch, connection_id,
                                                                               action='remove',
                                                                               logger_instance=conn_logger)
                    reestablish_msg = f"Connection for worker `{connection_id}` has been re-established."
                    if removed_tokens:
                        reestablish_msg += f" Monitored symbols (base): {', '.join(removed_tokens)} are back online."
                    else:
                        reestablish_msg += f" Symbols in this batch (base): {', '.join(base_tokens_in_batch)} were already considered online or not in lost file."
                    await telegram_notifier.send_message(reestablish_msg, level='INFO')

                consecutive_failures = 0
                reconnect_delay = RECONNECT_BASE_DELAY_SECONDS
                conn_logger.info(f"Successfully connected. Subscribing to symbols...")

                ping_task = asyncio.create_task(send_bitmart_pings(websocket, shutdown_event, connection_id))

                for symbol in symbols:
                    if shutdown_event.is_set(): break
                    await websocket.send(json.dumps({"op": "subscribe", "args": [f"{DEPTH_LEVEL}:{symbol}"]}))
                    await asyncio.sleep(SUBSCRIPTION_DELAY_SECONDS)

                async for message in websocket:
                    if shutdown_event.is_set(): break
                    if isinstance(message, bytes):
                        message_str = decompress_data(message)
                    else:
                        message_str = message
                    if not message_str or message_str == "pong": continue
                    try:
                        data = json.loads(message_str)
                        if data.get("table") and "depth" in data.get("table"):
                            for depth_update in data.get("data", []):
                                symbol = depth_update.get("symbol")
                                if symbol:
                                    output_data = {"symbol": symbol, "timestamp_ms": depth_update.get("ms_t"),
                                                   "bids": depth_update.get("bids", []),
                                                   "asks": depth_update.get("asks", [])}
                                    with open(os.path.join(OUTPUT_DIRECTORY, f"{symbol.replace('_', '')}.json"),
                                              'w') as f: json.dump(output_data, f, indent=2)
                    except Exception as e:
                        conn_logger.error(f"Error processing message: {e}", exc_info=True)

        except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError, ConnectionRefusedError) as e:
            last_exception = e
            conn_logger.warning(f"Connection issue for {connection_id}: {type(e).__name__} - {e}")
            # MODIFICATION: Add to lost connections file
            manage_lost_connections_file_bitmart_sync(base_tokens_in_batch, connection_id, action='add',
                                                      logger_instance=conn_logger)
        except Exception as e:
            last_exception = e
            conn_logger.error(f"Unhandled exception in {connection_id}: {e}", exc_info=True)
            # MODIFICATION: Add to lost connections file for unhandled exceptions too
            manage_lost_connections_file_bitmart_sync(base_tokens_in_batch, connection_id, action='add',
                                                      logger_instance=conn_logger)
        finally:
            if ping_task and not ping_task.done(): ping_task.cancel()
            if shutdown_event.is_set():
                conn_logger.info(f"Stopping connection {connection_id} due to global shutdown.")
                # MODIFICATION: Remove from lost connections on graceful shutdown
                manage_lost_connections_file_bitmart_sync(base_tokens_in_batch, connection_id, action='remove',
                                                          logger_instance=conn_logger)
                break

            # MODIFICATION: Enhanced reconnection and notification logic
            consecutive_failures += 1
            # Note: manage_lost_connections_file_bitmart_sync was already called in except blocks
            conn_logger.info(
                f"Will attempt reconnect for {connection_id} in {reconnect_delay}s... (Failure #{consecutive_failures}) Base tokens: {', '.join(base_tokens_in_batch)}")

            if consecutive_failures >= MAX_CONSECUTIVE_CONNECTION_FAILURES_FOR_ALERT:
                conn_logger.critical(
                    f"Max consecutive failures ({consecutive_failures}) reached for {connection_id}. Worker will stop. Base tokens affected: {', '.join(base_tokens_in_batch)}")
                error_details = traceback.format_exc() if last_exception else "No specific exception captured."
                critical_msg = (
                    f"CRITICAL failure for worker `{connection_id}`. Cannot re-establish after {consecutive_failures} attempts.\n"
                    f"Affected symbols (base): {', '.join(base_tokens_in_batch)}.\n\n"
                    f"**Last Error:**\n```\n{last_exception}\n```\n"
                    f"**Full Traceback:**\n```\n{error_details[:1000]}\n```"
                )
                await telegram_notifier.send_message(critical_msg, level='CRITICAL')
                # Entry will remain in the lost_connections_file
                break  # Stop this worker after critical failure

            if consecutive_failures == 1:  # First failure in a sequence for this worker
                lost_symbols_message = (
                    f"Connection lost for worker `{connection_id}`. "
                    f"Affected symbols in this batch (base): {', '.join(base_tokens_in_batch)}. "
                    f"Attempting to reconnect...\n\n**Error:** `{last_exception}`"
                )
                await telegram_notifier.send_message(lost_symbols_message, level='WARNING')

            reconnect_delay = min(
                RECONNECT_BASE_DELAY_SECONDS * (RECONNECT_EXPONENTIAL_BASE ** (consecutive_failures - 1)),
                RECONNECT_MAX_DELAY_SECONDS)
            await asyncio.sleep(reconnect_delay)


# --- Main Application Logic (MODIFIED FOR NOTIFICATIONS) ---
async def main_application():
    global_shutdown_event = asyncio.Event()
    await telegram_notifier.initialize()
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
    # MODIFICATION: Ensure lost_connections directory exists at startup
    os.makedirs(LOST_CONNECTIONS_DIR, exist_ok=True)

    # MODIFICATION: Immediate startup notification
    await telegram_notifier.send_message("Bitmart SPOT WebSocket Monitor is starting up...", level='INFO')

    logger.info(f"Errors logged to: {os.path.abspath(ERROR_LOG_FILENAME)}")
    logger.info(f"All logs (DEBUG+) will be in: {os.path.abspath(MAIN_LOG_FILE)}")

    loop = asyncio.get_running_loop()
    all_symbols = await loop.run_in_executor(None, get_bitmart_spot_symbols, True)

    if not all_symbols:
        logger.critical("No symbols fetched from Bitmart. Cannot proceed.")
        await telegram_notifier.send_message("CRITICAL: Could not fetch symbols from Bitmart. Script cannot start.",
                                             level='CRITICAL')
        return

    symbols_to_process = all_symbols[:MAX_TOTAL_SYMBOLS_TO_PROCESS] if MAX_TOTAL_SYMBOLS_TO_PROCESS else all_symbols
    symbol_chunks = [symbols_to_process[i:i + TOPICS_PER_CONNECTION_LIMIT] for i in
                     range(0, len(symbols_to_process), TOPICS_PER_CONNECTION_LIMIT)]
    if len(symbol_chunks) > MAX_CONNECTIONS_PER_IP:
        symbol_chunks = symbol_chunks[:MAX_CONNECTIONS_PER_IP]

    logger.info(f"Will launch {len(symbol_chunks)} WebSocket connection(s).")
    connection_tasks = []
    for chunk_idx, chunk in enumerate(symbol_chunks):
        if global_shutdown_event.is_set(): break
        connection_id = f"conn_{chunk_idx + 1:02d}"
        task = asyncio.create_task(manage_bitmart_spot_connection(chunk, connection_id, global_shutdown_event))
        connection_tasks.append(task)
        await asyncio.sleep(CONNECTION_LAUNCH_DELAY_SECONDS)

    logger.info(f"All {len(connection_tasks)} connection tasks launched. Monitoring...")

    # MODIFICATION: Detailed success notification
    success_message = (
        f"Bitmart Market Monitor Started Successfully!\n\n"
        f"• **Tracking:** `{len(symbols_to_process)}` symbols\n"
        f"• **Connections:** `{len(symbol_chunks)}` workers launched\n"
        f"• **Topics/Connection:** `{TOPICS_PER_CONNECTION_LIMIT}`"
    )
    await telegram_notifier.send_message(success_message, level='INFO')

    try:
        if connection_tasks: await asyncio.gather(*connection_tasks)
    finally:
        logger.info("Main application shutting down.")
        global_shutdown_event.set()
        if connection_tasks: await asyncio.gather(*connection_tasks, return_exceptions=True)
        await telegram_notifier.send_message("🛑 Bitmart SPOT WebSocket Monitor has been shut down.", level='INFO')
        await telegram_notifier.close()


# --- Entry Point ---
def main_entry_point_bitmart():
    """
    Main entry point for Bitmart.
    :return:
    """
    os.makedirs(os.path.dirname(ERROR_LOG_FILENAME), exist_ok=True)
    # MODIFICATION: Ensure lost_connections directory exists at startup (main_application also does this, but good for safety)
    os.makedirs(LOST_CONNECTIONS_DIR, exist_ok=True)
    try:
        asyncio.run(main_application())
    except KeyboardInterrupt:
        logger.info("\nCtrl+C detected. Shutting down.")
    except Exception as e:
        logger.critical(f"Unhandled exception in __main__: {e}", exc_info=True)
    finally:
        logger.info("Program finished execution.")
        logging.shutdown()