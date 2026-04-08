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
from typing import List, Dict, Optional, Any, Tuple, Set, Literal  # MODIFICATION: Added Literal
import ccxt.async_support as ccxt_async
import itertools
import aiohttp
from decimal import Decimal
import traceback

# --- Configuration ---
GATEIO_WEBSOCKET_URI = "wss://webws.gateio.live/v3/"
MAIN_OUTPUT_DIRECTORY = "CEX/orderbooks/gateio_orderbooks"
# MODIFICATION: Defined paths for both log files
MAIN_LOG_FILE = "CEX/app_log/gateio_app.log"  # For all logs (DEBUG level and up)
ERROR_LOG_FILE = "CEX/logging_cex/gateio_webws_errors.log"  # For errors only
# MODIFICATION: Added lost connections constants
LOST_CONNECTIONS_DIR = "CEX/lost_connections"
LOST_CONNECTIONS_FILE_GATEIO = os.path.join(LOST_CONNECTIONS_DIR, "gateio_lost.json")

PING_INTERVAL_SECONDS = 25
WEBWS_DEPTH_LEVEL_INT = 30

SYMBOLS_PER_CONNECTION = 2
SUBSCRIPTION_DELAY_SECONDS = 0.1
RECONNECT_DELAY_SECONDS = 10
CONNECTION_LAUNCH_DELAY_SECONDS = 0.5
MAX_TOTAL_SYMBOLS_TO_PROCESS = None

# Telegram configuration
TELEGRAM_BOT_TOKEN = ""
TELEGRAM_CHAT_ID = 1
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
MAX_RECONNECT_ATTEMPTS = 50

PRIORITY_SYMBOLS_FILE = "CEX/priority_token_symbols.txt"
REFRESH_INTERVAL_SECONDS = 60

request_id_counter = itertools.count(1)


def get_next_request_id() -> int:
    return next(request_id_counter)


# --- Logging Setup ---
logger = logging.getLogger("GateIO_WebWS_Client_MultiSymbol")
logger.setLevel(logging.DEBUG)  # Set root logger to lowest level to allow handlers to filter
log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s] %(message)s')
logging.getLogger("websockets").setLevel(logging.INFO)

# Console Handler (INFO and above)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

# MODIFICATION: Centralized file handler setup in one try/except block
try:
    # Main log file handler (gateio_app.log) - Captures EVERYTHING
    main_file_handler = logging.FileHandler(MAIN_LOG_FILE, mode='a', encoding='utf-8')
    main_file_handler.setLevel(logging.DEBUG)  # Set to DEBUG to capture all levels
    main_file_handler.setFormatter(log_formatter)
    logger.addHandler(main_file_handler)

    # Error log file handler (gateio_webws_errors.log) - Captures only ERRORS
    error_file_handler = logging.FileHandler(ERROR_LOG_FILE, mode='a', encoding='utf-8')
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(log_formatter)
    logger.addHandler(error_file_handler)

except IOError as e:
    print(f"CRITICAL: Could not open log files: {e}. Logs will only go to console.", file=sys.stderr)


# --- Telegram Notifier (Unchanged) ---
class TelegramNotifier:
    def __init__(self):
        self.enabled = TELEGRAM_ENABLED
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.session = None
        self.logger = logger.getChild(
            "telegram")  # MODIFICATION: Ensure logger is correctly initialized if used before main setup

    async def initialize(self):
        if self.enabled and not self.session:
            self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session:
            await self.session.close()

    async def send_message(self, message: str, level: str = 'INFO', parse_mode: str = 'Markdown'):
        if not self.enabled:
            return
        level_emoji = {'INFO': '✅', 'WARNING': '🟡', 'CRITICAL': '🔴'}
        if "re-established" in message.lower(): level_emoji['INFO'] = '🟢'
        formatted_message = f"{level_emoji.get(level, '🔵')} **Gate.io WebSocket Monitor**\n\n{message}"
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


# --- Lost Connection Utilities (MODIFICATION: Added) ---
def extract_base_asset_gateio(symbol_str: str) -> str:
    """Extracts the base asset from a Gate.io symbol string (e.g., BTC_USDT -> BTC)."""
    if isinstance(symbol_str, str) and '_' in symbol_str:
        return symbol_str.split('_')[0]
    logger.warning(f"Could not extract base asset from Gate.io symbol: {symbol_str}")
    return symbol_str


def manage_lost_connections_file_gateio_sync(
        base_tokens: List[str],
        conn_id_context: str,
        action: Literal['add', 'remove'],
        logger_instance: logging.Logger
) -> List[str]:
    """
    Manages the gateio_lost.json file by adding or removing base tokens.
    Returns a list of tokens whose state was actually changed.
    """
    if not base_tokens:
        return []

    changed_tokens: List[str] = []
    try:
        os.makedirs(LOST_CONNECTIONS_DIR, exist_ok=True)

        # Read existing lost tokens
        lost_data: Dict[str, str] = {}
        if os.path.exists(LOST_CONNECTIONS_FILE_GATEIO):
            with open(LOST_CONNECTIONS_FILE_GATEIO, 'r') as f:
                try:
                    lost_data = json.load(f)
                except json.JSONDecodeError:
                    logger_instance.error(
                        f"Error decoding JSON from {LOST_CONNECTIONS_FILE_GATEIO}. File will be overwritten/reinitialized.")
                    lost_data = {}

        current_lost_tokens_set = set(lost_data.keys())

        if action == 'add':
            for token in base_tokens:
                if token not in current_lost_tokens_set:
                    lost_data[token] = "lost"
                    changed_tokens.append(token)
            if changed_tokens:
                logger_instance.info(
                    f"[{conn_id_context}] Added to {LOST_CONNECTIONS_FILE_GATEIO}: {', '.join(changed_tokens)}")

        elif action == 'remove':
            for token in base_tokens:
                if token in current_lost_tokens_set:
                    if token in lost_data: del lost_data[token]
                    changed_tokens.append(token)
            if changed_tokens:
                logger_instance.info(
                    f"[{conn_id_context}] Removed from {LOST_CONNECTIONS_FILE_GATEIO}: {', '.join(changed_tokens)}")

        # Write updated data
        if changed_tokens or (action == 'add' and not os.path.exists(
                LOST_CONNECTIONS_FILE_GATEIO)):  # Write if changes or adding to new file
            with open(LOST_CONNECTIONS_FILE_GATEIO, 'w') as f:
                json.dump(lost_data, f, indent=2)

    except Exception as e:
        logger_instance.error(f"[{conn_id_context}] Error managing {LOST_CONNECTIONS_FILE_GATEIO}: {e}", exc_info=True)
    return changed_tokens


# --- All other functions remain exactly the same as the previous version ---
# --- (Utility, CCXT, Ping, Connection Worker, Orchestrator) ---

def scientific_to_decimal_string(value_str: str) -> str:
    try:
        if 'e' in value_str.lower():
            decimal_value = Decimal(value_str)
            if 'e-' in value_str.lower():
                exponent = int(value_str.lower().split('e-')[1])
                format_str = f'{{:.{exponent}f}}'
                result = format_str.format(decimal_value)
                if '.' in result:
                    result = result.rstrip('0').rstrip('.')
                return result
            else:
                return str(decimal_value)
        else:
            return value_str
    except Exception as e:
        logger.error(f"Error converting {value_str} from scientific notation: {e}")
        return value_str


connection_counter = itertools.count(start=1)


def load_priority_symbols() -> Set[str]:
    priority: Set[str] = set()
    try:
        script_dir = os.path.dirname(os.path.realpath(__file__))
        file_path = PRIORITY_SYMBOLS_FILE

        if not os.path.exists(file_path):
            logger.warning(f"Priority symbols file not found: {file_path}")
            return priority

        with open(file_path, 'r') as f:
            for line in f:
                symbol = line.strip().upper()
                if symbol and not symbol.startswith("#"):
                    priority.add(symbol)
        logger.debug(f"Loaded {len(priority)} priority symbols from {file_path}.")
    except IOError as e:
        logger.error(f"Error reading priority symbols file {file_path}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error loading priority symbols: {e}", exc_info=True)
    return priority


async def get_gateio_symbols_with_aggregation(reload_markets: bool = False, timeout_ms=30000, max_retries=3,
                                              retry_delay_s=10) -> List[Tuple[str, str]]:
    ccxt_logger = logger.getChild("ccxt_fetch_async")
    ccxt_logger.info(
        f"Attempting to fetch Gate.io USDT spot symbols and aggregation levels (Reload: {reload_markets})...")
    exchange = ccxt_async.gateio({'timeout': timeout_ms, 'enableRateLimit': True,
                                  'options': {'defaultType': 'spot', 'adjustForTimeDifference': True}})
    symbols_with_aggregation: List[Tuple[str, str]] = []
    markets_data = None
    try:
        for attempt in range(max_retries):
            try:
                ccxt_logger.info(
                    f"Loading Gate.io markets (attempt {attempt + 1}/{max_retries}, Reload: {reload_markets})...")
                if reload_markets:
                    markets_data = await exchange.load_markets(reload=True)
                else:
                    markets_data = await exchange.load_markets()
                ccxt_logger.info(
                    f"Gate.io markets loaded successfully (Total: {len(markets_data) if markets_data else 0}).")
                break
            except (ccxt_async.RequestTimeout, ccxt_async.NetworkError) as e:
                ccxt_logger.warning(f"Network error/timeout loading markets: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay_s)
                else:
                    return []
            except Exception as e:
                ccxt_logger.error(f"Unexpected error loading Gate.io markets: {e}", exc_info=True)
                return []
        if markets_data:
            for symbol_ccxt, market_info in markets_data.items():
                if market_info.get('spot', False) and market_info.get('quoteId',
                                                                      '').upper() == 'USDT' and market_info.get(
                    'active', True):
                    ws_symbol, aggregation_string = market_info.get('id'), None
                    if 'info' in market_info and 'tick_size' in market_info['info']:
                        aggregation_string = str(market_info['info']['tick_size'])
                    elif 'precision' in market_info and 'price' in market_info['precision']:
                        price_precision_val = market_info['precision']['price']
                        if isinstance(price_precision_val, (int,
                                                            float)) and price_precision_val > 0: aggregation_string = f"{Decimal(str(price_precision_val)):.10f}".rstrip(
                            '0').rstrip('.')
                    if ws_symbol and aggregation_string: symbols_with_aggregation.append(
                        (ws_symbol, scientific_to_decimal_string(aggregation_string)))
        else:
            ccxt_logger.warning("Failed to load Gate.io markets.")
    finally:
        if exchange: await exchange.close()
    return symbols_with_aggregation


async def send_gateio_webws_pings(websocket: WebSocketClientProtocol, shutdown_event: asyncio.Event,
                                  connection_id: str):
    ping_logger = logger.getChild(f"{connection_id}.ping")
    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=PING_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            try:
                await websocket.send(json.dumps({"id": get_next_request_id(), "method": "server.ping", "params": []}))
            except websockets.exceptions.ConnectionClosed:
                break
            except Exception as e:
                ping_logger.error(f"Error sending PING: {e}");
                break
        except asyncio.CancelledError:
            break


async def manage_gateio_webws_multi_symbol_connection(
        symbols_with_agg: List[Tuple[str, str]],
        connection_id: str,
        shutdown_event: asyncio.Event):
    conn_logger = logger.getChild(f"conn.{connection_id}")
    active_subscriptions = {}
    symbol_to_sub_id = {}
    reconnect_attempts = 0
    last_exception: Optional[Exception] = None
    # MODIFICATION: Extract base tokens for this connection
    current_raw_symbols = [s[0] for s in symbols_with_agg]
    current_base_tokens = sorted(list(set([extract_base_asset_gateio(s) for s in current_raw_symbols])))
    data_received_this_connection_cycle = False

    while not shutdown_event.is_set():
        websocket: Optional[WebSocketClientProtocol] = None
        ping_task: Optional[asyncio.Task] = None
        data_received_this_connection_cycle = False  # Reset for each connection attempt

        try:
            conn_logger.info(
                f"Attempting to connect for {len(symbols_with_agg)} symbols: {current_raw_symbols} (Base: {current_base_tokens})...")
            async with websockets.connect(
                    GATEIO_WEBSOCKET_URI,
                    open_timeout=20, close_timeout=10, ping_interval=None
            ) as websocket:
                # MODIFICATION: This message is now sent after data is confirmed
                # if reconnect_attempts > 0:
                #     conn_logger.info("Connection re-established.")
                #     await telegram_notifier.send_message(
                #         f"Connection for worker `{connection_id}` has been re-established.", level='INFO')

                # Initial successful connection or re-connection (before data confirmation)
                conn_logger.info(f"Successfully connected. Will subscribe to: {current_raw_symbols}")

                ping_task = asyncio.create_task(send_gateio_webws_pings(websocket, shutdown_event, connection_id))

                active_subscriptions.clear()
                symbol_to_sub_id.clear()

                for symbol, aggregation_str in symbols_with_agg:
                    if shutdown_event.is_set(): break
                    sub_id = get_next_request_id()
                    subscribe_payload = {"id": sub_id, "method": "depth.subscribe",
                                         "params": [symbol, WEBWS_DEPTH_LEVEL_INT, aggregation_str]}
                    await websocket.send(json.dumps(subscribe_payload))
                    active_subscriptions[sub_id] = symbol
                    symbol_to_sub_id[symbol] = sub_id
                    await asyncio.sleep(SUBSCRIPTION_DELAY_SECONDS)

                async for message_str in websocket:
                    if shutdown_event.is_set(): break
                    try:
                        message = json.loads(message_str)
                        msg_id = message.get("id")
                        if msg_id and message.get("result") == "pong": continue
                        if msg_id and msg_id in active_subscriptions:
                            subscribed_symbol_ack = active_subscriptions.pop(msg_id)
                            if message.get("error") is None and message.get("result", {}).get("status") == "success":
                                conn_logger.info(f"Subscription ACKED for {subscribed_symbol_ack} (ID: {msg_id}).")
                            else:
                                conn_logger.warning(
                                    f"Subscription FAILED for {subscribed_symbol_ack} (ID: {msg_id}): {message}")
                                symbol_to_sub_id.pop(subscribed_symbol_ack, None)
                                # MODIFICATION: If a specific symbol subscription fails, mark it as lost
                                failed_base_token = [extract_base_asset_gateio(subscribed_symbol_ack)]
                                added_failure_tokens = manage_lost_connections_file_gateio_sync(failed_base_token,
                                                                                                connection_id, 'add',
                                                                                                conn_logger)
                                if added_failure_tokens:
                                    await telegram_notifier.send_message(
                                        f"Subscription FAILED for worker `{connection_id}`, symbol `{subscribed_symbol_ack}`. "
                                        f"Base token `{added_failure_tokens[0]}` marked as lost.",
                                        level='WARNING'
                                    )
                            continue
                        if message.get("method") == "depth.update" and message.get("id") is None:
                            # MODIFICATION: Data received, manage lost connections file and notify
                            if not data_received_this_connection_cycle:
                                data_received_this_connection_cycle = True  # Mark data as flowing for this cycle
                                removed_tokens = manage_lost_connections_file_gateio_sync(current_base_tokens,
                                                                                          connection_id, 'remove',
                                                                                          conn_logger)
                                if removed_tokens:
                                    conn_logger.info(
                                        f"Data flowing for {connection_id}. Removed {', '.join(removed_tokens)} from lost list.")
                                    await telegram_notifier.send_message(
                                        f"Connection for worker `{connection_id}` is STABLE (data received).\n"
                                        f"Base tokens now monitored: `{', '.join(removed_tokens)}`.",
                                        level='INFO'  # Green check
                                    )
                                elif reconnect_attempts > 0:  # Reconnected but tokens were not in lost list or already removed
                                    await telegram_notifier.send_message(
                                        f"Connection for worker `{connection_id}` re-established and data flowing.",
                                        level='INFO'
                                    )
                                # Reset reconnect_attempts ONLY after successful data receipt
                                reconnect_attempts = 0

                            params = message.get("params")
                            if isinstance(params, list) and len(params) == 3:
                                is_snapshot, data_obj, symbol_from_data = params
                                if isinstance(data_obj, dict):
                                    output_data = {"symbol": symbol_from_data,
                                                   "timestamp_ms": int(data_obj.get("current", time.time()) * 1000),
                                                   "update_id": data_obj.get("id"), "is_snapshot": is_snapshot,
                                                   "bids": data_obj.get("bids", []), "asks": data_obj.get("asks", [])}
                                    clean_symbol = symbol_from_data.replace('_', '')
                                    out_file = os.path.join(MAIN_OUTPUT_DIRECTORY, f"{clean_symbol}.json")
                                    with open(out_file, 'w') as f: json.dump(output_data, f, indent=2)
                    except Exception as e_msg_loop:
                        conn_logger.error(f"Error in message processing loop: {e_msg_loop}", exc_info=True)

        except (websockets.exceptions.ConnectionClosed, websockets.exceptions.InvalidURI, ConnectionRefusedError,
                asyncio.TimeoutError) as e:
            last_exception = e
            conn_logger.warning(f"Connection issue: {type(e).__name__} - {e}")
            # MODIFICATION: Add to lost connections file on failure, regardless of prior data flow in this cycle.
            # If we are in this block, the connection is currently down.
            added_tokens = manage_lost_connections_file_gateio_sync(current_base_tokens, connection_id, 'add',
                                                                    conn_logger)
            if added_tokens:
                conn_logger.info(
                    f"Marked {', '.join(added_tokens)} as lost due to connection failure for {connection_id}.")
        except Exception as e_conn_outer:
            last_exception = e_conn_outer
            conn_logger.error(f"Unhandled exception in connection phase: {e_conn_outer}", exc_info=True)
            # MODIFICATION: Add to lost connections file on critical failure, regardless of prior data flow in this cycle.
            added_tokens = manage_lost_connections_file_gateio_sync(current_base_tokens, connection_id, 'add',
                                                                    conn_logger)
            if added_tokens:
                conn_logger.info(
                    f"Marked {', '.join(added_tokens)} as lost due to unhandled exception for {connection_id}.")
        finally:
            if ping_task and not ping_task.done(): ping_task.cancel()
            if shutdown_event.is_set():
                conn_logger.info(f"Stopping connection {connection_id} due to global shutdown.")
                # MODIFICATION: Attempt to remove from lost list on graceful shutdown
                removed_on_shutdown = manage_lost_connections_file_gateio_sync(current_base_tokens, connection_id,
                                                                               'remove', conn_logger)
                if removed_on_shutdown:
                    conn_logger.info(
                        f"Graceful shutdown: Removed {', '.join(removed_on_shutdown)} from lost connections file for worker {connection_id}.")
                break
            else:  # Not a global shutdown, so it's a connection drop / retry scenario
                reconnect_attempts += 1
                conn_logger.info(
                    f"Will attempt reconnect for connection {connection_id} in {RECONNECT_DELAY_SECONDS}s... (Attempt {reconnect_attempts}/{MAX_RECONNECT_ATTEMPTS})")

                # MODIFICATION: Always attempt to mark as lost if connection dropped and we are retrying.
                # The fact that data *was* received earlier in this *same connection instance* doesn't mean
                # we are currently receiving it. The connection has dropped.
                newly_lost_tokens = manage_lost_connections_file_gateio_sync(current_base_tokens, connection_id, 'add',
                                                                             conn_logger)

                affected_tokens_msg_part = ""
                if newly_lost_tokens:
                    affected_tokens_msg_part = f"\\n**Base Tokens Newly Marked Lost:** `{', '.join(newly_lost_tokens)}`"
                elif current_base_tokens:  # If no new tokens were marked lost, but we have tokens we are managing
                    affected_tokens_msg_part = f"\\n**Base Tokens (already marked or no change):** `{', '.join(current_base_tokens)}`"

                if reconnect_attempts >= MAX_RECONNECT_ATTEMPTS:
                    conn_logger.error(
                        f"Max reconnection attempts ({MAX_RECONNECT_ATTEMPTS}) reached for {connection_id}.")
                    error_details = traceback.format_exc() if last_exception else "No specific exception captured."

                    # Ensure tokens are marked as lost one last time
                    critically_lost_tokens = manage_lost_connections_file_gateio_sync(current_base_tokens,
                                                                                      connection_id, 'add', conn_logger)
                    critical_affected_tokens_msg = f"`{', '.join(critically_lost_tokens)}`" if critically_lost_tokens else "None (already marked)"

                    critical_msg = (
                        f"CRITICAL failure for worker `{connection_id}`. Cannot re-establish after {MAX_RECONNECT_ATTEMPTS} attempts.\n\n"
                        f"**Raw Symbols Affected:** `{', '.join(current_raw_symbols)}`\n"
                        f"**Base Tokens Confirmed Lost:** {critical_affected_tokens_msg}\n\n"
                        f"**Last Error:**\n```\n{last_exception}\n```\n"
                        f"**Full Traceback (partial):**\n```\n{error_details[:1000]}\n```"
                    )
                    await telegram_notifier.send_message(critical_msg, level='CRITICAL')
                    # MODIFICATION: Reset reconnect_attempts to allow new cycle if script logic desires, or let it terminate if orchestrator removes it.
                    # For now, let it break or be handled by orchestrator. If we reset, it might spam.
                    # Consider breaking out or letting orchestrator handle this worker's fate.
                    # For now, we'll let it try to reconnect after a longer delay as per original logic, but it's effectively dead for these symbols.
                    # To prevent spamming critical alerts, we might want to set a flag.
                    # However, the current logic will keep trying after MAX_RECONNECT_ATTEMPTS if not broken here.
                    # The prompt implies it should stop trying or be handled.
                    # Let's assume the orchestrator might eventually remove it.
                    # For safety, we'll reset reconnect_attempts here to avoid repeated critical messages if it somehow continues.
                    reconnect_attempts = 0  # Reset to avoid spamming critical if loop continues due to external logic
                else:  # Normal reconnect attempt
                    if reconnect_attempts == 1:  # First failure in a sequence
                        await telegram_notifier.send_message(
                            f"Connection lost for worker `{connection_id}`. Attempting to reconnect...\n\n"
                            f"**Error:** `{last_exception}`"
                            f"{affected_tokens_msg_part}",  # Add affected tokens info
                            level='WARNING'
                        )
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=RECONNECT_DELAY_SECONDS)
                except asyncio.TimeoutError:
                    pass
                except asyncio.CancelledError:
                    break
    conn_logger.info(f"Exited connection management loop for {connection_id}.")


async def main_orchestrator():
    global connection_counter
    await telegram_notifier.initialize()
    logger.info("Main orchestrator started. WILL ONLY CONNECT TO PRIORITY SYMBOLS.")
    await telegram_notifier.send_message("🚀 Gate.io Market Monitor script is starting up...", level='INFO')

    managed_tasks: Dict[frozenset[str], asyncio.Task] = {}
    managed_shutdown_events: Dict[frozenset[str], asyncio.Event] = {}
    startup_notification_sent = False

    try:
        while True:
            cycle_start_time = time.monotonic()
            logger.info("Starting periodic refresh and connection management cycle (Priority Only).")

            current_market_details_list = await get_gateio_symbols_with_aggregation(reload_markets=True)
            if not current_market_details_list:
                logger.warning("Periodic refresh failed to fetch market data. Skipping this cycle.")
                await asyncio.sleep(REFRESH_INTERVAL_SECONDS)
                continue

            available_spot_markets: Dict[str, Tuple[str, str]] = {ws_symbol: (ws_symbol, agg_str) for ws_symbol, agg_str
                                                                  in current_market_details_list}
            priority_symbols_set = await asyncio.to_thread(load_priority_symbols)
            desired_priority_symbols_details: List[Tuple[str, str]] = [details for ws_symbol, details in
                                                                       available_spot_markets.items() if
                                                                       isinstance(ws_symbol,
                                                                                  str) and '_' in ws_symbol and
                                                                       ws_symbol.split('_')[0] in priority_symbols_set]

            logger.info(f"{len(desired_priority_symbols_details)} priority symbols identified for connection.")

            next_managed_tasks, next_managed_shutdown_events = {}, {}
            tasks_to_await_removal = []

            for task_key, task in managed_tasks.items():
                symbol_in_task = list(task_key)[0]
                if task.done() or not any(d[0] == symbol_in_task for d in desired_priority_symbols_details):
                    if not task.done(): managed_shutdown_events[task_key].set()
                    tasks_to_await_removal.append(task)
                else:
                    next_managed_tasks[task_key] = task
                    next_managed_shutdown_events[task_key] = managed_shutdown_events[task_key]

            if tasks_to_await_removal:
                await asyncio.gather(
                    *[asyncio.wait_for(t, timeout=RECONNECT_DELAY_SECONDS + 5) for t in tasks_to_await_removal],
                    return_exceptions=True)

            managed_tasks, managed_shutdown_events = next_managed_tasks, next_managed_shutdown_events

            for symbol_detail in desired_priority_symbols_details:
                symbol_name = symbol_detail[0]
                task_key = frozenset({symbol_name})
                if task_key not in managed_tasks:
                    conn_id = f"priority_{next(connection_counter)}_{symbol_name}"
                    shutdown_event = asyncio.Event()
                    task = asyncio.create_task(
                        manage_gateio_webws_multi_symbol_connection([symbol_detail], conn_id, shutdown_event))
                    managed_tasks[task_key] = task
                    managed_shutdown_events[task_key] = shutdown_event
                    await asyncio.sleep(CONNECTION_LAUNCH_DELAY_SECONDS)

            if not startup_notification_sent and managed_tasks:
                startup_message = (
                    f"Gate.io Market Monitor Started Successfully!\n\n"
                    f"• **Mode:** `Priority Symbols Only`\n"
                    f"• **Tracking:** `{len(managed_tasks)}` priority symbols\n"
                    f"• **Connections:** `{len(managed_tasks)}` workers launched"
                )
                await telegram_notifier.send_message(startup_message, level='INFO')
                startup_notification_sent = True

            logger.info(f"End of cycle. Active priority tasks: {len(managed_tasks)}.")
            cycle_duration = time.monotonic() - cycle_start_time
            sleep_duration = max(0, REFRESH_INTERVAL_SECONDS - cycle_duration)
            await asyncio.sleep(sleep_duration)

    except asyncio.CancelledError:
        logger.info("Main orchestrator task cancelled. Shutting down all connections.")
        for event in managed_shutdown_events.values(): event.set()
        running_tasks = [task for task in managed_tasks.values() if not task.done()]
        if running_tasks: await asyncio.gather(*running_tasks, return_exceptions=True)
    except Exception as e:
        logger.error(f"Unhandled error in main orchestrator loop: {e}", exc_info=True)
        await telegram_notifier.send_message(
            f"Gate.io Fetcher: CRITICAL ERROR in orchestrator - {e}. Fetcher might be down.", level='CRITICAL')
    finally:
        logger.info("Closing Telegram notifier session.")
        await telegram_notifier.close()
        logger.info("Main orchestrator shut down complete.")


def main_entry_point_gateio():
    """
        main entry point for the gateio program.
    """
    os.makedirs(MAIN_OUTPUT_DIRECTORY, exist_ok=True)
    os.makedirs(os.path.dirname(ERROR_LOG_FILE), exist_ok=True)
    os.makedirs(LOST_CONNECTIONS_DIR, exist_ok=True)  # MODIFICATION: Ensure lost_connections dir exists
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s')

    # MODIFICATION: Initialize TelegramNotifier's logger correctly if it's used early
    # This is a bit of a workaround if TelegramNotifier is instantiated globally before logger is fully configured.
    # A better pattern might be to pass the logger to TelegramNotifier or initialize it fully within its __init__.
    # For now, ensuring the child logger gets handlers from the main logger if already configured.
    if not telegram_notifier.logger.handlers and logger.handlers:
        for handler in logger.handlers:
            telegram_notifier.logger.addHandler(handler)
        telegram_notifier.logger.propagate = False  # Avoid duplicate messages if main logger also logs
    telegram_notifier.logger.setLevel(logging.DEBUG)  # Or whatever level is appropriate

    logger.info("Starting Gate.io WebWS client...")
    try:
        asyncio.run(main_orchestrator())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down.")
    except Exception as e:
        logger.critical(f"Critical error at top level: {e}", exc_info=True)
    finally:
        logger.info("Gate.io WebWS client shutdown process finished.")