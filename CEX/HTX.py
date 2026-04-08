import asyncio
import websockets
import json
import gzip
import signal
import time
import os
import httpx
import traceback
import logging
from typing import List, Dict, Optional, Any, Tuple, Set, Literal  # MODIFICATION: Added Literal

# --- Configuration ---
WEBSOCKET_URL = "wss://www.htx.com/-/s/pro/ws"
DATA_DIRECTORY = "CEX/orderbooks/htx_orderbooks"
TOPICS_PER_CONNECTION = 10
SYMBOL_REFRESH_INTERVAL_SECONDS = 3600  # Refresh symbol list every hour

MAIN_LOG_FILE = 'CEX/app_log/htx_all_logs.log'
ERROR_LOG_FILE = 'CEX/logging_cex/htx_error_logs.log'

# MODIFICATION: Added lost connections constants
LOST_CONNECTIONS_DIR = "CEX/lost_connections"
LOST_CONNECTIONS_FILE_HTX = os.path.join(LOST_CONNECTIONS_DIR, "htx_lost.json")

# --- Telegram Configuration ---
TELEGRAM_BOT_TOKEN = ""  # Placeholder, ensure this is correct
TELEGRAM_CHAT_ID = 1  # Placeholder, ensure this is correct

# --- Reconnection Logic ---
RECONNECT_DELAY_SECONDS = 5
MAX_RECONNECT_ATTEMPTS = 10

# --- Global State ---
shutdown_event = asyncio.Event()
tracked_symbols = set()  # Stores full symbols like 'btcusdt'
running_workers = {}  # Maps worker_id to task

# MODIFICATION: Define known quote assets for extraction, ordered by length (longest first)
# This helps in correctly parsing symbols like 'btchusd' vs 'btcht' if 'ht' and 'husd' are quotes
KNOWN_QUOTE_ASSETS = sorted([
    "usdt", "usdc", "husd", "tusd", "busd", "eurs",  # 4 chars
    "dai", "btc", "eth", "trx", "xrp", "ltc", "eos", "ada", "bnb", "gbp", "try",  # 3 chars
    "ht"  # 2 chars
], key=len, reverse=True)


def setup_logging():
    """Configures logging to file and console."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler = logging.FileHandler(MAIN_LOG_FILE, mode='a')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    error_handler = logging.FileHandler(ERROR_LOG_FILE, mode='a')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logging.getLogger(__name__)


# MODIFICATION: Added Lost Connection Utilities
def extract_base_asset_htx(symbol_str: str, logger_instance: logging.Logger) -> str:
    """Extracts the base asset from an HTX symbol string (e.g., btcusdt -> BTC)."""
    s_lower = symbol_str.lower()
    for quote in KNOWN_QUOTE_ASSETS:
        if s_lower.endswith(quote):
            base = s_lower[:-len(quote)]
            if base:  # Ensure base is not empty
                return base.upper()
    logger_instance.warning(
        f"Could not extract base asset from HTX symbol: {symbol_str} using known quotes. Returning original symbol minus last 3 chars or full if shorter.")
    # Fallback for unknown quote assets, assuming quote is typically 3-4 chars
    if len(s_lower) > 3:
        return s_lower[:-3].upper()
    return s_lower.upper()  # Or return as is if very short


def manage_lost_connections_file_htx_sync(
        base_tokens: List[str],
        conn_id_context: str,
        action: Literal['add', 'remove'],
        logger_instance: logging.Logger
) -> List[str]:
    """
    Manages the htx_lost.json file by adding or removing base tokens.
    Returns a list of tokens whose state was actually changed.
    """
    if not base_tokens:
        return []

    changed_tokens: List[str] = []
    try:
        os.makedirs(LOST_CONNECTIONS_DIR, exist_ok=True)

        lost_data: Dict[str, str] = {}
        if os.path.exists(LOST_CONNECTIONS_FILE_HTX):
            with open(LOST_CONNECTIONS_FILE_HTX, 'r') as f:
                try:
                    lost_data = json.load(f)
                except json.JSONDecodeError:
                    logger_instance.error(
                        f"Error decoding JSON from {LOST_CONNECTIONS_FILE_HTX}. File will be overwritten/reinitialized.")
                    lost_data = {}

        current_lost_tokens_set = set(lost_data.keys())

        if action == 'add':
            for token in base_tokens:
                if token not in current_lost_tokens_set:
                    lost_data[token] = "lost"
                    changed_tokens.append(token)
            if changed_tokens:
                logger_instance.info(
                    f"[{conn_id_context}] Added to {LOST_CONNECTIONS_FILE_HTX}: {', '.join(changed_tokens)}")

        elif action == 'remove':
            for token in base_tokens:
                if token in current_lost_tokens_set:
                    if token in lost_data: del lost_data[token]
                    changed_tokens.append(token)
            if changed_tokens:
                logger_instance.info(
                    f"[{conn_id_context}] Removed from {LOST_CONNECTIONS_FILE_HTX}: {', '.join(changed_tokens)}")

        if changed_tokens or (action == 'add' and not os.path.exists(LOST_CONNECTIONS_FILE_HTX) and base_tokens):
            with open(LOST_CONNECTIONS_FILE_HTX, 'w') as f:
                json.dump(lost_data, f, indent=2)

    except Exception as e:
        logger_instance.error(f"[{conn_id_context}] Error managing {LOST_CONNECTIONS_FILE_HTX}: {e}", exc_info=True)
    return changed_tokens


def shutdown_handler(signum, frame):
    """Handles signals like Ctrl+C to shut down gracefully."""
    logging.getLogger(__name__).info("Shutdown signal received. Initiating graceful shutdown...")
    shutdown_event.set()


async def send_telegram_message(message, level='INFO'):
    """Sends a formatted message to a Telegram chat."""
    logger = logging.getLogger('TelegramNotifier')
    if not TELEGRAM_BOT_TOKEN or "YOUR_BOT_TOKEN" in TELEGRAM_BOT_TOKEN:
        logger.warning(f"Telegram notifications are disabled. Message: {message}")
        return
    level_emoji = {'INFO': '✅', 'WARNING': '🟡', 'CRITICAL': '🔴'}
    formatted_message = f"{level_emoji.get(level, '🔵')} **HTX WebSocket Monitor**\n\n{message}"
    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': formatted_message, 'parse_mode': 'Markdown'}
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(api_url, json=payload)
            if response.status_code != 200:
                logger.error(f"Error sending Telegram message: {response.status_code} - {response.text}")
    except Exception as e:
        logger.error(f"Failed to send Telegram message due to an exception.", exc_info=True)


async def fetch_all_symbols():
    """Connects once to fetch the complete list of market symbols."""
    logger = logging.getLogger('SymbolFetcher')
    logger.info("Attempting to fetch the complete list of market symbols...")
    try:
        async with websockets.connect(WEBSOCKET_URL) as websocket:
            await websocket.send(json.dumps({"req": "market.symbols", "id": "fetch"}))
            async for message in websocket:
                data = json.loads(gzip.decompress(message).decode('utf-8'))
                if 'rep' in data and data['rep'] == 'market.symbols':
                    raw_symbol_data = data.get('data')
                    if isinstance(raw_symbol_data, str):
                        raw_symbol_data = json.loads(raw_symbol_data)
                    if isinstance(raw_symbol_data, dict):
                        symbols = list(raw_symbol_data.keys())
                        logger.info(f"Successfully fetched {len(symbols)} symbols.")
                        return set(symbols)
                    else:
                        logger.error(f"Expected symbol 'data' to be a dict, but got {type(raw_symbol_data)}")
                        return None
    except Exception:
        logger.critical("CRITICAL: Failed to fetch symbol list.", exc_info=True)
        await send_telegram_message("CRITICAL: Could not fetch market symbols list. The script cannot start.",
                                    level='CRITICAL')
        return None


async def connection_worker(worker_id, symbols_chunk):
    """Manages a single WebSocket connection for a chunk of symbols."""
    logger = logging.getLogger(worker_id)
    reconnect_attempts = 0
    # MODIFICATION: Determine base tokens for this worker
    current_base_tokens = sorted(list(set([extract_base_asset_htx(s, logger) for s in symbols_chunk])))
    data_received_this_connection_cycle = False
    last_exception_for_telegram: Optional[Exception] = None

    while not shutdown_event.is_set():
        websocket: Optional[websockets.client.WebSocketClientProtocol] = None
        data_received_this_connection_cycle = False  # Reset for each connection attempt
        last_exception_for_telegram = None

        try:
            logger.info(f"Attempting to connect for symbols: {symbols_chunk} (Base: {current_base_tokens})")
            async with websockets.connect(WEBSOCKET_URL) as websocket:
                # This re-established message is now sent after data confirmation
                # if reconnect_attempts > 0:
                #     logger.info("Connection re-established.")
                #     await send_telegram_message(f"Connection re-established for `{worker_id}`.", level='INFO')

                logger.info(f"Successfully connected. Subscribing to {len(symbols_chunk)} symbols.")

                for symbol in symbols_chunk:
                    if shutdown_event.is_set(): break
                    sub_payload = {"sub": f"market.{symbol}.depth.step0", "id": f"{worker_id}-{symbol}"}
                    await websocket.send(json.dumps(sub_payload))
                logger.info(f"Sent subscription requests for {len(symbols_chunk)} symbols.")

                async for message in websocket:
                    if shutdown_event.is_set(): break

                    try:
                        decompressed_message = gzip.decompress(message)
                        data = json.loads(decompressed_message.decode('utf-8'))
                    except Exception as e_decode:
                        logger.error(f"Error decompressing/decoding message: {e_decode}. Message: {message[:100]}")
                        continue  # Skip this message

                    if 'ping' in data:
                        await websocket.send(json.dumps({'pong': data['ping']}))
                        continue

                    if 'ch' in data and 'tick' in data:  # This is a data message
                        # MODIFICATION: Data received, manage lost connections file and notify
                        if not data_received_this_connection_cycle:
                            data_received_this_connection_cycle = True
                            removed_tokens = manage_lost_connections_file_htx_sync(current_base_tokens, worker_id,
                                                                                   'remove', logger)
                            if removed_tokens:
                                logger.info(
                                    f"Data flowing for {worker_id}. Removed {', '.join(removed_tokens)} from lost list.")
                                await send_telegram_message(
                                    f"Connection for worker `{worker_id}` is STABLE (data received).\n"
                                    f"Base tokens now monitored: `{', '.join(removed_tokens)}`.",
                                    level='INFO'
                                )
                            elif reconnect_attempts > 0:  # Reconnected but tokens were not in lost list or already removed
                                await send_telegram_message(
                                    f"Connection for worker `{worker_id}` re-established and data flowing.",
                                    level='INFO'
                                )
                            reconnect_attempts = 0  # Reset reconnect_attempts ONLY after successful data receipt

                        # Original data processing logic
                        symbol_from_channel = data['ch'].split('.')[1]
                        if symbol_from_channel in symbols_chunk:  # Ensure it's a symbol we care about
                            tick_data = data.get('tick')
                            filename = os.path.join(DATA_DIRECTORY, f"{symbol_from_channel.upper()}.json")
                            with open(filename, 'w', encoding='utf-8') as f:
                                json.dump(tick_data, f, ensure_ascii=False, indent=4)

                    elif 'status' in data and data.get('status') == 'error':
                        logger.error(
                            f"Subscription or request error: {data.get('err-msg', 'Unknown error')}. Full message: {data}")
                        # Consider if specific error codes should trigger marking symbols as lost
                        # For now, general connection loss will handle it.
                        # If a specific symbol subscription fails, we might need more granular handling here.
                        # Example: if data['subbed'] is the topic and it failed.
                        # For now, this is a general error log.

        except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError, ConnectionRefusedError) as e:
            last_exception_for_telegram = e
            reconnect_attempts += 1
            logger.warning(
                f"Connection failed ({reconnect_attempts}/{MAX_RECONNECT_ATTEMPTS}). Error: {type(e).__name__} - {e}")

            # MODIFICATION: Add to lost connections file on failure
            added_tokens = manage_lost_connections_file_htx_sync(current_base_tokens, worker_id, 'add', logger)
            if added_tokens:
                logger.info(f"Marked {', '.join(added_tokens)} as lost due to connection failure for {worker_id}.")

            affected_tokens_msg_part = ""
            if added_tokens:
                affected_tokens_msg_part = f"\\n**Base Tokens Newly Marked Lost:** `{', '.join(added_tokens)}`"
            elif current_base_tokens:
                affected_tokens_msg_part = f"\\n**Base Tokens (already marked or no change):** `{', '.join(current_base_tokens)}`"

            if reconnect_attempts >= MAX_RECONNECT_ATTEMPTS:
                critical_msg = (
                    f"Worker `{worker_id}` failed to reconnect after {MAX_RECONNECT_ATTEMPTS} attempts."
                    f"{affected_tokens_msg_part}"
                )
                logger.critical(critical_msg, exc_info=True)  # Log full exception for critical
                await send_telegram_message(
                    f"{critical_msg}\\n\\n**Last Error:**\\n```\\n{last_exception_for_telegram}\\n```",
                    level='CRITICAL')
                reconnect_attempts = 0  # Reset to allow another cycle of attempts after a longer pause
                sleep_duration = 60
            else:
                await send_telegram_message(
                    f"Connection lost for `{worker_id}`. Attempting to reconnect...\\n"
                    f"**Error:** `{last_exception_for_telegram}`"
                    f"{affected_tokens_msg_part}",
                    level='WARNING'
                )
                sleep_duration = RECONNECT_DELAY_SECONDS

            logger.info(f"Waiting {sleep_duration} seconds before retrying.")
            await asyncio.sleep(sleep_duration)

        except Exception as e:
            last_exception_for_telegram = e
            logger.error(f"An unexpected error occurred in worker {worker_id}.", exc_info=True)

            # MODIFICATION: Add to lost connections file on unexpected error
            added_tokens = manage_lost_connections_file_htx_sync(current_base_tokens, worker_id, 'add', logger)
            if added_tokens:
                logger.info(f"Marked {', '.join(added_tokens)} as lost due to unexpected error for {worker_id}.")

            affected_tokens_msg_part = ""
            if added_tokens:
                affected_tokens_msg_part = f"\\n**Base Tokens Newly Marked Lost:** `{', '.join(added_tokens)}`"
            elif current_base_tokens:
                affected_tokens_msg_part = f"\\n**Base Tokens (already marked or no change):** `{', '.join(current_base_tokens)}`"

            tb_str = traceback.format_exc()
            await send_telegram_message(
                f"Unexpected ERROR in worker `{worker_id}`.\\n"
                f"{affected_tokens_msg_part}\\n"
                f"**Error:**\\n```\\n{last_exception_for_telegram}\\n```\\n"
                f"**Traceback (partial):**\\n```\\n{tb_str[:1000]}\\n```",
                level='CRITICAL'  # Treat unexpected errors as critical for notification
            )
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)  # Standard delay before retry

    logger.info(f"Worker {worker_id} shutting down.")
    # MODIFICATION: On graceful shutdown (worker loop terminates due to shutdown_event), remove its tokens
    if shutdown_event.is_set():
        removed_on_shutdown = manage_lost_connections_file_htx_sync(current_base_tokens, worker_id, 'remove', logger)
        if removed_on_shutdown:
            logger.info(
                f"Graceful shutdown: Removed {', '.join(removed_on_shutdown)} from lost connections file for worker {worker_id}.")


async def symbol_manager():
    """Periodically fetches symbols and launches new workers for new symbols."""
    logger = logging.getLogger('SymbolManager')
    while not shutdown_event.is_set():
        try:
            await asyncio.sleep(SYMBOL_REFRESH_INTERVAL_SECONDS)
            logger.info("Periodically refreshing symbol list...")

            new_symbols = await fetch_all_symbols()
            if not new_symbols:
                logger.warning("Failed to fetch new symbols, skipping this cycle.")
                continue

            added_symbols = list(new_symbols - tracked_symbols)
            delisted_symbols = list(tracked_symbols - new_symbols)

            if delisted_symbols:
                logger.info(f"Detected {len(delisted_symbols)} delisted symbols: {delisted_symbols[:5]}...")

            if not added_symbols:
                logger.info("No new symbols found.")
                continue

            logger.info(f"Found {len(added_symbols)} new symbols to track: {added_symbols[:5]}...")
            await send_telegram_message(f"Found {len(added_symbols)} new market symbols. Launching new workers.",
                                        level='INFO')

            new_chunks = [added_symbols[i:i + TOPICS_PER_CONNECTION] for i in
                          range(0, len(added_symbols), TOPICS_PER_CONNECTION)]
            worker_offset = len(running_workers)

            for i, chunk in enumerate(new_chunks):
                worker_id = f"Worker-{worker_offset + i + 1}"
                task = asyncio.create_task(connection_worker(worker_id, chunk))
                running_workers[worker_id] = task
                for symbol in chunk:
                    tracked_symbols.add(symbol)
            logger.info(f"Launched {len(new_chunks)} new workers for added symbols.")

        except asyncio.CancelledError:
            break
        except Exception:
            logger.error("An error occurred in the symbol manager.", exc_info=True)
    logger.info("Symbol manager shutting down.")


async def main():
    """Main function to orchestrate everything."""
    logger = setup_logging()
    os.makedirs(DATA_DIRECTORY, exist_ok=True)
    os.makedirs(LOST_CONNECTIONS_DIR, exist_ok=True)  # MODIFICATION: Ensure lost_connections dir exists
    logger.info(f"Data will be saved in '{DATA_DIRECTORY}/'. Press Ctrl+C to stop.")

    await send_telegram_message("🚀 HTX Market Monitor script is starting up...", level='INFO')

    initial_symbols = await fetch_all_symbols()
    if not initial_symbols:
        return

    global tracked_symbols
    tracked_symbols = initial_symbols
    symbol_chunks = [list(initial_symbols)[i:i + TOPICS_PER_CONNECTION] for i in
                     range(0, len(initial_symbols), TOPICS_PER_CONNECTION)]
    logger.info(f"Partitioned symbols into {len(symbol_chunks)} chunks.")

    for i, chunk in enumerate(symbol_chunks):
        worker_id = f"Worker-{i + 1}"
        task = asyncio.create_task(connection_worker(worker_id, chunk))
        running_workers[worker_id] = task

    # --- SUCCESS NOTIFICATION ---
    # Send a notification after the initial setup is complete.
    success_message = (
        f"HTX Market Monitor Started Successfully!\n\n"
        f"• **Tracking:** `{len(initial_symbols)}` symbols\n"
        f"• **Connections:** `{len(symbol_chunks)}` workers launched\n"
        f"• **Topics/Connection:** `{TOPICS_PER_CONNECTION}`"
    )
    await send_telegram_message(success_message, level='INFO')

    manager_task = asyncio.create_task(symbol_manager())

    await shutdown_event.wait()

    manager_task.cancel()
    for task in running_workers.values():
        task.cancel()

    await asyncio.gather(manager_task, *running_workers.values(), return_exceptions=True)
    logger.info("All tasks have been cancelled.")


def main_entry_point_htx():
    """
    Main entry point for the HTX CEX
    """
    signal.signal(signal.SIGINT, shutdown_handler)
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        logging.getLogger(__name__).info("Script has been shut down.")