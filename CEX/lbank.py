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
import traceback  # Added traceback
from typing import List, Dict, Optional, Any, Tuple, Literal  # Added Literal
import ccxt
import aiohttp

# --- Configuration ---
EXCHANGE_ID = 'lbank'
MARKET_TYPE_TO_WATCH = 'spot'
QUOTE_ASSET = 'USDT'
LBANK_SPOT_WS_URL = "wss://www.lbkex.net/ws/V2/"
OUTPUT_DIRECTORY = "CEX/orderbooks/lbank_orderbooks"

# LBank specific limits
TOPICS_PER_CONNECTION_LIMIT = 50
MAX_CONNECTIONS_PER_IP = 100
DEPTH_LEVEL = '50'

# Timing configuration
SUBSCRIPTION_DELAY_SECONDS = 0.25
CONNECTION_LAUNCH_DELAY_SECONDS = 2.0

# Reconnection configuration
RECONNECT_BASE_DELAY_SECONDS = 5
RECONNECT_MAX_DELAY_SECONDS = 300
RECONNECT_EXPONENTIAL_BASE = 2

# MODIFICATION: Added constants for lost connections
LOST_CONNECTIONS_DIR = "CEX/lost_connections"  # Shared with other connectors
LOST_CONNECTIONS_FILE = os.path.join(LOST_CONNECTIONS_DIR, "lbank_lost.json")

# Telegram configuration
TELEGRAM_BOT_TOKEN = ""
TELEGRAM_CHAT_ID = 1
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
MAX_CONSECUTIVE_CONNECTION_FAILURES_FOR_ALERT = 10

# Processing limits
MAX_TOTAL_SYMBOLS_TO_PROCESS = None

# Logging configuration
CONSOLE_LOG_LEVEL = logging.INFO
FILE_LOG_LEVEL = logging.ERROR


# --- Telegram Notifier ---
class TelegramNotifier:
    def __init__(self):
        self.enabled = TELEGRAM_ENABLED
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.session = None
        self.logger = None

    async def initialize(self, logger_instance: logging.Logger):
        self.logger = logger_instance.getChild("telegram")
        if self.enabled and not self.session: self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session: await self.session.close()

    async def send_message(self, message: str, parse_mode: str = 'Markdown'):
        if not self.enabled: return
        try:
            if not self.session:
                print("Warning: Telegram Notifier session not initialized.", file=sys.stderr)
                return
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            if len(message) > 4096: message = message[:4093] + "..."
            data = {'chat_id': self.chat_id, 'text': message, 'parse_mode': parse_mode}
            async with self.session.post(url, json=data) as response:
                if response.status != 200: self.logger.error(f"Telegram API error: {await response.text()}")
        except Exception as e:
            self.logger.error(f"Failed to send Telegram message: {e}")


telegram_notifier = TelegramNotifier()


# --- CCXT Function ---
def get_lbank_spot_symbols(logger: logging.Logger, reload_markets: bool = False) -> List[Tuple[str, str]]:
    ccxt_logger = logger.getChild("ccxt_fetch")
    ccxt_logger.info(f"Attempting to fetch LBank {QUOTE_ASSET} spot symbols...")
    exchange = ccxt.lbank({'timeout': 30000, 'enableRateLimit': True, 'options': {'defaultType': 'spot'}})
    symbols: List[Tuple[str, str]] = []
    for attempt in range(3):
        try:
            markets_data = exchange.load_markets(reload_markets)
            ccxt_logger.info(f"Found {len(markets_data)} total markets.")
            for symbol_ccxt, market_info in markets_data.items():
                if (market_info.get('spot', False) and market_info.get('active', True) and
                        market_info.get('quote', '').upper() == QUOTE_ASSET and market_info.get('type') == 'spot'):
                    ws_symbol = market_info.get('id')
                    if ws_symbol and not ws_symbol.startswith('$') and ':' not in ws_symbol:
                        if ws_symbol.endswith('_SPBL'): ws_symbol = ws_symbol[:-5]
                        symbols.append((symbol_ccxt, ws_symbol))
            ccxt_logger.info(f"Found {len(symbols)} active {QUOTE_ASSET} spot pairs.")
            return symbols
        except Exception as e:
            ccxt_logger.warning(f"Error loading LBank markets on attempt {attempt + 1}: {e}")
            if attempt < 2:
                time.sleep(10)
            else:
                ccxt_logger.error("Max retries reached. Failed to load LBank markets.", exc_info=True)
                return []
    return []


# --- Utility functions ---

# MODIFICATION: Added utility function to extract base asset (similar to bingx.py)
def extract_base_asset(symbol_id_ccxt: str) -> str:
    """
    Extracts the base asset from a CCXT market ID.
    Handles formats like "BTC/USDT" -> "BTC".
    """
    return symbol_id_ccxt.split('/')[0]


# MODIFICATION: Added utility function to manage the lost connections JSON file for LBank
def manage_lost_connections_file_lbank_sync(
        base_tokens: List[str],
        conn_id: str,  # For logging
        action: Literal['add', 'remove'],
        logger_instance: logging.Logger  # Pass logger instance
) -> List[str]:  # Returns list of tokens whose state actually changed
    """
    Manages the lbank_lost.json file.
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
            logger_instance.error(
                f"Worker {conn_id}: Error decoding {LOST_CONNECTIONS_FILE}. Re-initializing file content.")
            lost_connections_data = {}  # Reset if file is corrupt
        except Exception as e:
            logger_instance.error(
                f"Worker {conn_id}: Error reading {LOST_CONNECTIONS_FILE}: {e}. Assuming empty content.")
            lost_connections_data = {}  # Reset on other read errors

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


# --- Manage WebSocket Connection ---
async def manage_lbank_spot_connection(logger: logging.Logger, symbols: List[Tuple[str, str]], connection_id: str,
                                       shutdown_event: asyncio.Event):
    conn_logger = logger.getChild(f"conn.{connection_id}")
    # MODIFICATION: Extract base tokens for this batch
    base_tokens_in_batch = sorted(list(set([extract_base_asset(s[0]) for s in symbols])))  # s[0] is ccxt_symbol

    subscribed_symbols: Dict[str, str] = {}
    consecutive_failures = 0
    reconnect_delay = RECONNECT_BASE_DELAY_SECONDS
    last_exception_details = "N/A"

    while not shutdown_event.is_set():
        try:
            conn_logger.info(
                f"Attempting to connect for {len(symbols)} symbols (Base tokens: {', '.join(base_tokens_in_batch[:3])}{'...' if len(base_tokens_in_batch) > 3 else ''})...")
            async with websockets.connect(LBANK_SPOT_WS_URL, open_timeout=20, close_timeout=10,
                                          ping_interval=None) as websocket:  # LBank uses PING/PONG messages, not auto ping_interval

                if consecutive_failures > 0:  # This means it's a re-established connection
                    conn_logger.info("Connection re-established.")
                    removed_tokens = manage_lost_connections_file_lbank_sync(base_tokens_in_batch, connection_id,
                                                                             action='remove',
                                                                             logger_instance=conn_logger)

                    reestablish_msg = f"✅ *LBank Connection Re-established*\\n\\nID: `{connection_id}`"
                    if removed_tokens:
                        reestablish_msg += f"\\nRecovered symbols (base): {', '.join(removed_tokens)}"
                    else:
                        reestablish_msg += f"\\nSymbols in this batch (base): {', '.join(base_tokens_in_batch)}"
                    reestablish_msg += f"\\nStatus: Connected after `{consecutive_failures}` failed attempts."
                    await telegram_notifier.send_message(reestablish_msg)

                ws_symbol_list = [ws_sym for _, ws_sym in symbols]
                conn_logger.info(
                    f"Successfully connected. Subscribing to: {ws_symbol_list[:5]}{'...' if len(ws_symbol_list) > 5 else ''}")

                consecutive_failures = 0  # Reset on successful connection
                reconnect_delay = RECONNECT_BASE_DELAY_SECONDS  # Reset reconnect delay
                subscribed_symbols.clear()

                for ccxt_symbol, ws_symbol in symbols:
                    if shutdown_event.is_set(): break
                    await websocket.send(json.dumps(
                        {"action": "subscribe", "subscribe": "depth", "depth": DEPTH_LEVEL, "pair": ws_symbol}))
                    subscribed_symbols[ws_symbol] = ccxt_symbol
                    await asyncio.sleep(SUBSCRIPTION_DELAY_SECONDS)  # Keep original delay

                if shutdown_event.is_set(): break

                last_ping_ack_time = time.monotonic()  # For LBank's custom ping/pong

                async for message_str in websocket:
                    if shutdown_event.is_set(): break
                    try:
                        data = json.loads(message_str)
                        if data.get("action") == "ping":
                            await websocket.send(json.dumps({"action": "pong", "pong": data.get("ping")}))
                            last_ping_ack_time = time.monotonic()
                            conn_logger.debug(f"Responded to PING with PONG: {data.get('ping')}")
                        elif "depth" in data:
                            last_ping_ack_time = time.monotonic()  # Activity means connection is alive
                            pair = data.get("pair")
                            if pair in subscribed_symbols:
                                output_file = os.path.join(OUTPUT_DIRECTORY,
                                                           f"{subscribed_symbols[pair].replace('/', '')}.json")
                                with open(output_file, 'w') as f: json.dump(data['depth'], f, indent=2)
                        # LBank might send other status messages, log them if needed
                        # else:
                        #     conn_logger.debug(f"Received other message: {data}")

                        # Check for stale connection based on LBank's PING/PONG
                        # LBank typically sends PING every 5 seconds. If no PING or data for ~15-20s, consider stale.
                        if time.monotonic() - last_ping_ack_time > 20:  # 20 seconds without PING/PONG or data
                            raise websockets.exceptions.ConnectionClosedError(1001,
                                                                              "Stale connection, no PING/PONG or data from LBank server")

                    except json.JSONDecodeError as e:
                        conn_logger.error(f"JSON decode error: {e} on message: {message_str[:200]}")
                    except Exception as e:  # Catch errors within message processing loop
                        conn_logger.error(f"Error processing message: {e}", exc_info=True)
                        # Potentially trigger reconnect if a critical message processing error occurs
                        # For now, just log and continue, rely on outer exception handlers for connection issues

        except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK,
                websockets.exceptions.InvalidURI, ConnectionRefusedError, asyncio.TimeoutError) as e:
            last_exception_details = f"{type(e).__name__}: {e}"
            conn_logger.warning(f"Connection issue: {last_exception_details}")
            consecutive_failures += 1
            manage_lost_connections_file_lbank_sync(base_tokens_in_batch, connection_id, action='add',
                                                    logger_instance=conn_logger)

            # Send detailed warning on first failure in a sequence
            if consecutive_failures == 1:
                lost_symbols_message = (
                    f"⚠️ *LBank Connection Warning*\\n\\n"
                    f"ID: `{connection_id}`\\n"
                    f"Error: `{type(e).__name__}`\\n"
                    f"Details: `{e}`\\n"
                    f"Affected symbols (base): {', '.join(base_tokens_in_batch)}\\n"
                    f"Attempting reconnect..."
                )
                await telegram_notifier.send_message(lost_symbols_message)
            # For subsequent non-critical failures, a simpler log message is fine, Telegram already notified.

            if isinstance(e, websockets.exceptions.InvalidURI):
                await telegram_notifier.send_message(
                    f"🚨 *LBank CRITICAL Failure*\\nConn `{connection_id}`: Invalid URI. Task stopping.")
                break  # Stop this task

        except Exception as e:  # Catch-all for other unexpected errors
            last_exception_details = f"{type(e).__name__}: {e}\\nTraceback: {traceback.format_exc(limit=2)}"
            conn_logger.error(f"Unhandled exception in connection manager: {e}", exc_info=True)
            consecutive_failures += 1
            manage_lost_connections_file_lbank_sync(base_tokens_in_batch, connection_id, action='add',
                                                    logger_instance=conn_logger)

            # Send detailed warning on first such failure in a sequence
            if consecutive_failures == 1:
                unhandled_error_message = (
                    f"🔥 *LBank Unhandled Exception*\\n\\n"
                    f"ID: `{connection_id}`\\n"
                    f"Error: `{type(e).__name__}`\\n"
                    f"Details: `{e}`\\n"
                    f"Affected symbols (base): {', '.join(base_tokens_in_batch)}\\n"
                    f"Attempting reconnect..."
                )
                await telegram_notifier.send_message(unhandled_error_message)

        finally:  # This block runs whether an exception occurred or not (if loop breaks)
            if shutdown_event.is_set():
                conn_logger.info(f"Shutdown signal received, exiting connection loop for {connection_id}.")
                # Clean up any "lost" state for this worker if it's shutting down cleanly
                manage_lost_connections_file_lbank_sync(base_tokens_in_batch, connection_id, action='remove',
                                                        logger_instance=conn_logger)
                break

            if consecutive_failures > 0:  # Only if there was a failure leading to this finally block (and not shutdown)
                if consecutive_failures >= MAX_CONSECUTIVE_CONNECTION_FAILURES_FOR_ALERT:
                    critical_msg = (
                        f"🚨 *LBank CRITICAL Failure*\\n\\nID: `{connection_id}`\\n"
                        f"Reason: Max reconnects reached ({MAX_CONSECUTIVE_CONNECTION_FAILURES_FOR_ALERT}).\\n"
                        f"Last Error: `{last_exception_details}`\\n"
                        f"Affected symbols (base): {', '.join(base_tokens_in_batch)}\\n"
                        f"Status: Task **GIVING UP**."
                    )
                    await telegram_notifier.send_message(critical_msg)
                    conn_logger.error(
                        f"Max consecutive failures reached for {connection_id}. Stopping task. Last error: {last_exception_details}")
                    # The file entry for these tokens will remain as "lost"
                    break  # Stop this task permanently

                reconnect_delay = min(
                    RECONNECT_BASE_DELAY_SECONDS * (RECONNECT_EXPONENTIAL_BASE ** (consecutive_failures - 1)),
                    RECONNECT_MAX_DELAY_SECONDS)
                conn_logger.info(
                    f"Will attempt reconnect in {reconnect_delay:.1f}s... (Failure #{consecutive_failures} for {connection_id})")
                try:
                    # Wait for the reconnect delay, but also listen for the shutdown event
                    await asyncio.wait_for(shutdown_event.wait(), timeout=reconnect_delay)
                    if shutdown_event.is_set():  # If shutdown_event was set during wait
                        conn_logger.info(f"Shutdown detected during reconnect wait for {connection_id}.")
                        manage_lost_connections_file_lbank_sync(base_tokens_in_batch, connection_id, action='remove',
                                                                logger_instance=conn_logger)
                        break
                except asyncio.TimeoutError:
                    pass  # Timeout means delay has passed, continue to next iteration to reconnect
                except asyncio.CancelledError:
                    conn_logger.info(f"Reconnect wait cancelled for {connection_id}.")
                    manage_lost_connections_file_lbank_sync(base_tokens_in_batch, connection_id, action='remove',
                                                            logger_instance=conn_logger)
                    raise  # Re-raise CancelledError to be handled by outer task management

    conn_logger.info(f"Exited connection management loop for {connection_id}.")
    # Final cleanup if task exits normally or due to unhandled break
    if not shutdown_event.is_set():  # If exited for reasons other than graceful shutdown (e.g. max retries)
        # Ensure 'lost' status is recorded if not already by a graceful shutdown
        # This might be redundant if already added, but manage_lost_connections_file_lbank_sync handles duplicates
        manage_lost_connections_file_lbank_sync(base_tokens_in_batch, connection_id, action='add',
                                                logger_instance=conn_logger)
    else:  # If part of graceful shutdown, ensure removal
        manage_lost_connections_file_lbank_sync(base_tokens_in_batch, connection_id, action='remove',
                                                logger_instance=conn_logger)


# --- Main Application Logic ---
async def main_application():
    # Define log file paths
    error_log_file = "CEX/logging_cex/lbank_spot_webws_errors.log"
    all_logs_file = "CEX/app_log/lbank_all_logs.log"

    # 1. Create directories FIRST
    try:
        os.makedirs(os.path.dirname(all_logs_file), exist_ok=True)
        os.makedirs(os.path.dirname(error_log_file), exist_ok=True)
        os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
        os.makedirs(LOST_CONNECTIONS_DIR, exist_ok=True)  # MODIFICATION: Ensure lost_connections dir exists
    except OSError as e:
        print(f"CRITICAL: Could not create log directories: {e}. Exiting.", file=sys.stderr)
        return

    # 2. NOW, SETUP THE LOGGING SYSTEM
    logger = logging.getLogger("LBank_SPOT_WebWS_Client")
    logger.setLevel(logging.DEBUG)
    log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s] %(message)s')

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(CONSOLE_LOG_LEVEL)
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)

    # Error File Handler
    error_file_handler = logging.FileHandler(error_log_file, mode='a')
    error_file_handler.setLevel(FILE_LOG_LEVEL)
    error_file_handler.setFormatter(log_formatter)
    logger.addHandler(error_file_handler)

    # All Logs Handler
    all_logs_handler = logging.handlers.RotatingFileHandler(
        all_logs_file, maxBytes=10 * 1024 * 1024, backupCount=5, mode='a')
    all_logs_handler.setLevel(logging.DEBUG)
    all_logs_handler.setFormatter(log_formatter)
    logger.addHandler(all_logs_handler)

    # 3. THE REST OF THE APPLICATION CAN NOW RUN
    global_shutdown_event = asyncio.Event()
    await telegram_notifier.initialize(logger)

    logger.info("Logging initialized. Starting main application.")
    logger.info(f"Order book data will be saved in: {os.path.abspath(OUTPUT_DIRECTORY)}")
    logger.info(f"All logs (DEBUG and up) will be saved to: {os.path.abspath(all_logs_file)}")
    logger.info(f"Error logs (ERROR and up) will be saved to: {os.path.abspath(error_log_file)}")

    await telegram_notifier.send_message("🚀 *LBank SPOT WebSocket Monitor Started*")
    loop = asyncio.get_running_loop()
    all_symbols_with_ws = await loop.run_in_executor(None, get_lbank_spot_symbols, logger, True)
    if not all_symbols_with_ws:
        logger.error("No symbols fetched from LBank. Cannot proceed.")
        await telegram_notifier.send_message("❌ *LBank WS Failed to Start*\nCould not fetch symbols.")
        return

    symbols_to_process = all_symbols_with_ws[
                         :MAX_TOTAL_SYMBOLS_TO_PROCESS] if MAX_TOTAL_SYMBOLS_TO_PROCESS else all_symbols_with_ws
    if not symbols_to_process:
        logger.error("Symbol list empty after filtering. Cannot proceed.")
        return

    symbol_chunks = [symbols_to_process[i:i + TOPICS_PER_CONNECTION_LIMIT] for i in
                     range(0, len(symbols_to_process), TOPICS_PER_CONNECTION_LIMIT)]
    if len(symbol_chunks) > MAX_CONNECTIONS_PER_IP:
        symbol_chunks = symbol_chunks[:MAX_CONNECTIONS_PER_IP]

    logger.info(f"Launching {len(symbol_chunks)} WebSocket connection(s).")
    connection_tasks = [
        asyncio.create_task(manage_lbank_spot_connection(logger, chunk, f"conn_{i:04d}", global_shutdown_event)) for
        i, chunk in enumerate(symbol_chunks)]
    logger.info(f"All {len(connection_tasks)} tasks launched. Monitoring...")
    try:
        if connection_tasks: await asyncio.gather(*connection_tasks, return_exceptions=True)
    except asyncio.CancelledError:
        logger.info("Main application task cancelled.")
    finally:
        logger.info("Main application shutting down.")
        global_shutdown_event.set()
        if connection_tasks: await asyncio.gather(*connection_tasks, return_exceptions=True)
        await telegram_notifier.send_message("🛑 *LBank SPOT WebSocket Monitor Stopped*")
        await telegram_notifier.close()
        logger.info("Main application cleanup complete.")


# --- Entry Point ---
def main_entry_point_lbank():
    """
    main entry point for the lbank program.
    """
    # MODIFICATION: Ensure lost_connections directory exists at startup (redundant if main_application creates it, but safe)
    try:
        os.makedirs(LOST_CONNECTIONS_DIR, exist_ok=True)
    except OSError as e:
        print(f"Warning: Could not create lost_connections directory at startup: {e}", file=sys.stderr)

    try:
        asyncio.run(main_application())
    except KeyboardInterrupt:
        print("\nCtrl+C detected. Shutting down gracefully.")
    finally:
        logging.shutdown()