import asyncio
import ccxt.pro
import json
import logging
import os
import sys
import traceback
from datetime import datetime
from ccxt import NetworkError, DDoSProtection, BadSymbol, ExchangeNotAvailable, RequestTimeout
import httpx
import time
from typing import Literal  # MODIFICATION: Added Literal

# --- Global logger instance ---
logger = logging.getLogger(__name__)

# --- Configuration Constants ---
PRIORITY_SYMBOLS_FILE = 'CEX/priority_token_symbols.txt'
REFRESH_INTERVAL_SECONDS = 60
ORDERBOOK_DEPTH = 50
ORDERBOOKS_BASE_DIR = "CEX/orderbooks"
LOG_DIR = "."  # Writing logs to the current directory for simplicity
# MODIFICATION: Changed log file names for clarity
MAIN_LOG_FILE = "CEX/app_log/kucoin_app.log"
ERROR_LOG_FILENAME = "CEX/logging_cex/kucoin_spot_collector_errors.log"

# MODIFICATION: Added constants for lost connections
LOST_CONNECTIONS_DIR = "CEX/lost_connections"
LOST_CONNECTIONS_FILE = os.path.join(LOST_CONNECTIONS_DIR, "kucoin_lost.json")

# --- Telegram Notification Configuration ---
TELEGRAM_BOT_TOKEN = ""
TELEGRAM_CHAT_ID = 1
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID and "YOUR" not in TELEGRAM_BOT_TOKEN)


# MODIFICATION: Enhanced send_telegram_notification_async
async def send_telegram_notification_async(message: str, level: str = 'INFO'):
    """Sends an asynchronous notification message via Telegram with levels."""
    if not TELEGRAM_ENABLED: return

    level_emoji = {'INFO': '✅', 'WARNING': '🟡', 'CRITICAL': '🔴'}
    if "RESTORED" in message: level_emoji['INFO'] = '🟢'
    if "starting up" in message.lower(): level_emoji['INFO'] = '🚀'

    hostname = os.getenv("HOSTNAME", os.uname().nodename if hasattr(os, 'uname') else "UnknownHost")
    full_message = f"{level_emoji.get(level, '🔵')} **KuCoin WebSocket Monitor** ({hostname})\n\n{message}"

    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': full_message, 'parse_mode': 'Markdown'}
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(api_url, json=payload, timeout=10.0)
            if response.status_code >= 400:
                logger.error(f"Telegram notification failed (HTTP {response.status_code}): {response.text}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while sending Telegram notification.", exc_info=True)


# ==============================================================================
# MODIFICATION: Rewritten setup_logging for real-time writing and filtering
# ==============================================================================
def setup_logging(log_level=logging.INFO):
    """
    Sets up logging for real-time file writing and filters noisy libraries.
    Returns the file streams that need to be closed on application exit.
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(LOST_CONNECTIONS_DIR, exist_ok=True)  # MODIFICATION: Ensure lost_connections dir exists
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # Set root to lowest level to catch all app messages
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # --- FIX 1: Silence noisy library loggers ---
    # This prevents raw websocket data and other verbose library messages from being logged.
    logging.getLogger("ccxt").setLevel(logging.INFO)
    logging.getLogger("websockets").setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s')

    # --- FIX 2: Use line-buffered streams for real-time `tail -f` support ---
    # We open the files manually and use StreamHandler instead of FileHandler.
    main_log_path = MAIN_LOG_FILE
    error_log_path = ERROR_LOG_FILENAME

    # Use buffering=1 for line-buffering
    main_log_stream = open(main_log_path, 'a', encoding='utf-8', buffering=1)
    error_log_stream = open(error_log_path, 'a', encoding='utf-8', buffering=1)

    # Console handler (remains the same)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Main log file stream handler
    main_handler = logging.StreamHandler(main_log_stream)
    main_handler.setLevel(logging.DEBUG)  # Captures all levels from your app
    main_handler.setFormatter(formatter)
    root_logger.addHandler(main_handler)

    # Error log file stream handler
    error_handler = logging.StreamHandler(error_log_stream)
    error_handler.setLevel(logging.ERROR)  # Captures only ERROR and CRITICAL
    error_handler.setFormatter(formatter)
    root_logger.addHandler(error_handler)

    logger.info(f"Logging setup complete. All logs in: {main_log_path}. Errors in: {error_log_path}")

    # Return the streams so they can be closed gracefully on shutdown
    return [main_log_stream, error_log_stream]


# --- Utility functions ---

# MODIFICATION: Added utility function to extract base asset for KuCoin
def extract_base_asset(kucoin_symbol_ccxt: str) -> str:
    """Extracts the base asset from a KuCoin CCXT market ID (e.g., "BTC/USDT" -> "BTC")."""
    return kucoin_symbol_ccxt.split('/')[0]


# MODIFICATION: Added utility function to manage the lost connections JSON file for KuCoin
def manage_lost_connections_file_kucoin_sync(
        base_tokens: list[str],
        action: Literal['add', 'remove'],
        logger_instance: logging.Logger,
        conn_id: str = "KuCoinApp"  # Generic ID for KuCoin as it's per symbol
) -> list[str]:
    """
    Manages the kucoin_lost.json file.
    Adds or removes base tokens based on connection status.
    """
    os.makedirs(LOST_CONNECTIONS_DIR, exist_ok=True)

    lost_connections_data: dict[str, str] = {}
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

    changed_tokens: list[str] = []

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


# --- KucoinSpotCollector class remains unchanged ---
class KucoinSpotCollector:
    def __init__(self, priority_symbols_file, refresh_interval, orderbook_depth):
        self.exchange_id = 'kucoin'
        self.exchange = getattr(ccxt.pro, self.exchange_id)()
        self.priority_symbols_file = priority_symbols_file
        self.refresh_interval = refresh_interval
        self.orderbook_depth = orderbook_depth
        self.active_ws_connections = {}
        self.current_priority_base_symbols = set()
        self.markets = {}
        self.output_dir = os.path.join(ORDERBOOKS_BASE_DIR, f"{self.exchange_id}_orderbooks")
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(LOST_CONNECTIONS_DIR, exist_ok=True)  # Ensure dir exists on init
        # Health tracking
        self.symbol_last_data_receipt_time = {}
        self.symbol_failure_streak_count = {}
        self.notification_sent_for_symbol = {}
        self.notification_threshold_minutes = int(os.getenv("KUCOIN_NOTIFICATION_THRESHOLD_MINUTES", "5"))
        self.max_failures_for_notification = int(os.getenv("KUCOIN_MAX_FAILURES_NOTIFICATION", "3"))
        logger.info("KucoinSpotCollector initialized.")

    def load_priority_symbols(self):
        symbols = set()
        try:
            with open(self.priority_symbols_file, 'r', encoding='utf-8') as f:
                for line in f:
                    stripped = line.strip().upper()
                    if stripped and not stripped.startswith('#'): symbols.add(stripped)
            if self.current_priority_base_symbols != symbols:
                logger.info(f"Loaded {len(symbols)} priority symbols.")
            self.current_priority_base_symbols = symbols
        except Exception as e:
            logger.error(f"Error loading priority symbols: {e}");
            self.current_priority_base_symbols = set()
        return self.current_priority_base_symbols

    async def _load_exchange_markets(self, reload=False):
        try:
            markets_data = await self.exchange.load_markets(reload=reload)
            if markets_data: self.markets = markets_data; logger.info(f"Loaded {len(self.markets)} markets.")
        except Exception as e:
            logger.error(f"Error loading markets: {e}", exc_info=True)

    async def _get_active_priority_ccxt_symbols(self, reload_markets_now=False):
        priority_base_symbols = self.load_priority_symbols()
        if not priority_base_symbols: return set()
        if reload_markets_now or not self.markets: await self._load_exchange_markets(reload=True)
        active_priority_symbols = set()
        if self.markets:
            for base in priority_base_symbols:
                target = f"{base.upper()}/USDT"
                market = self.markets.get(target)
                if market and market.get('spot') and market.get('quoteId', '').upper() == 'USDT' and market.get(
                        'active'):
                    active_priority_symbols.add(target)
        logger.info(f"Found {len(active_priority_symbols)} active priority symbols.")
        return active_priority_symbols

    def _save_orderbook(self, symbol, orderbook_data):
        try:
            filename = os.path.join(self.output_dir, f"{symbol.replace('/', '')}.json")
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(orderbook_data, f, indent=4)
        except Exception as e:
            logger.error(f"[{symbol}] Error saving order book: {e}")

    async def _watch_single_symbol_orderbook(self, symbol):
        logger.info(f"[{symbol}] Starting order book watcher.")
        output_filename = os.path.join(self.output_dir, f"{symbol.replace('/', '')}.json")
        base_asset = extract_base_asset(symbol)
        initial_connection_made = False  # Tracks if the first successful data has been received
        try:
            while True:
                orderbook = await self.exchange.watchOrderBook(symbol, self.orderbook_depth)

                # MODIFICATION: Handle connection established/re-established
                if not initial_connection_made or self.notification_sent_for_symbol.get(symbol, False):
                    removed_tokens = manage_lost_connections_file_kucoin_sync([base_asset], 'remove', logger,
                                                                              conn_id=f"watcher-{symbol}")

                    if self.notification_sent_for_symbol.get(symbol,
                                                             False):  # Was previously considered lost by this run's notification logic
                        await send_telegram_notification_async(
                            f"Connection for `{symbol}` (Base: {base_asset}) on KuCoin has been RESTORED.",
                            level='INFO')
                    elif initial_connection_made and removed_tokens:  # Reconnected after an issue not severe enough for full notification cycle, but was in file
                        await send_telegram_notification_async(
                            f"Data flow for `{symbol}` (Base: {base_asset}) on KuCoin resumed. Removed from lost file.",
                            level='INFO')
                    # If !initial_connection_made and removed_tokens, it means it was in the file from a previous run, and now it's good.
                    # No specific "RESTORED" message unless self.notification_sent_for_symbol was true for this run.

                    initial_connection_made = True  # Mark that we have received data at least once

                self.symbol_last_data_receipt_time[symbol] = time.time()
                self.symbol_failure_streak_count[symbol] = 0
                self.notification_sent_for_symbol[symbol] = False
                self._save_orderbook(symbol, orderbook)
        except (BadSymbol, NetworkError, ExchangeNotAvailable, RequestTimeout) as e:
            logger.warning(f"[{symbol}] Watcher terminating due to CCXT error: {type(e).__name__} - {e}")
            # MODIFICATION: Add to lost file on CCXT errors that terminate watcher
            manage_lost_connections_file_kucoin_sync([base_asset], 'add', logger, conn_id=f"watcher-{symbol}")
            # The orchestrator will handle the notification logic based on failure streaks
        except asyncio.CancelledError:
            logger.info(f"[{symbol}] Watcher task cancelled.")
            raise
        except Exception as e:
            logger.error(f"[{symbol}] Unexpected error in watcher: {e}", exc_info=True)
            # MODIFICATION: Add to lost file on other errors that terminate watcher
            manage_lost_connections_file_kucoin_sync([base_asset], 'add', logger, conn_id=f"watcher-{symbol}")
            # The orchestrator will handle the notification logic
        finally:
            logger.info(f"[{symbol}] Order book watcher stopped.")

    async def _orchestrate_priority_connections(self):
        startup_notification_sent = False
        while True:
            try:
                logger.info("Orchestrator: Refreshing markets and priority symbols...")
                target_symbols = await self._get_active_priority_ccxt_symbols(reload_markets_now=True)
                current_watching = set(self.active_ws_connections.keys())

                symbols_to_start = target_symbols - current_watching
                for symbol in symbols_to_start:
                    logger.info(f"Orchestrator: Starting watcher for new symbol: {symbol}")
                    self.symbol_failure_streak_count[symbol] = 0;
                    self.notification_sent_for_symbol[symbol] = False
                    task = asyncio.create_task(self._watch_single_symbol_orderbook(symbol))
                    self.active_ws_connections[symbol] = task
                    await asyncio.sleep(0.5)

                symbols_to_stop = current_watching - target_symbols
                for symbol in symbols_to_stop:
                    logger.info(f"Orchestrator: Stopping watcher for stale symbol: {symbol}")
                    task = self.active_ws_connections.pop(symbol, None)
                    if task and not task.done(): task.cancel()
                    # MODIFICATION: Remove from lost file if explicitly stopped being priority
                    base_asset_stopped = extract_base_asset(symbol)
                    manage_lost_connections_file_kucoin_sync([base_asset_stopped], 'remove', logger,
                                                             conn_id="orchestrator-stop")
                    self.symbol_failure_streak_count.pop(symbol, None);
                    self.notification_sent_for_symbol.pop(symbol, None)

                if not startup_notification_sent and self.active_ws_connections:
                    await send_telegram_notification_async(
                        f"KuCoin Market Monitor Started Successfully!\n\n"
                        f"• **Tracking:** `{len(self.active_ws_connections)}` symbols\n"
                        f"• **Mode:** `CCXT.PRO watchOrderBook`",
                        level='INFO'
                    )
                    startup_notification_sent = True

                completed_tasks = {s: t for s, t in self.active_ws_connections.items() if t.done()}
                for symbol, task in completed_tasks.items():
                    self.active_ws_connections.pop(symbol, None)
                    # MODIFICATION: Add to lost file when orchestrator detects a task failed/completed
                    # This is because _watch_single_symbol_orderbook already adds it in its except/finally blocks
                    # if the failure was due to an exception there. This covers cases where the task might exit
                    # for other reasons or if the logic in _watch_single_symbol_orderbook missed it.
                    # It's okay if it's added twice, the manage function handles duplicates.
                    base_asset_failed = extract_base_asset(symbol)
                    manage_lost_connections_file_kucoin_sync([base_asset_failed], 'add', logger,
                                                             conn_id="orchestrator-fail")

                    self.symbol_failure_streak_count[symbol] = self.symbol_failure_streak_count.get(symbol, 0) + 1
                    failure_streak = self.symbol_failure_streak_count[symbol]
                    if not self.notification_sent_for_symbol.get(symbol,
                                                                 False) and failure_streak >= self.max_failures_for_notification:
                        exception_info = task.exception()
                        error_details = f"Last error: {type(exception_info).__name__} - {exception_info}" if exception_info else "Task exited cleanly but unexpectedly."
                        # MODIFICATION: Include base asset in Telegram notification for failed symbol
                        msg = (
                            f"Persistent issue with `{symbol}` (Base: {base_asset_failed}) on KuCoin. Watcher has failed `{failure_streak}` times.\\n\\n"
                            f"**Details:**\\n```{error_details}```")
                        await send_telegram_notification_async(msg, level='WARNING')
                        self.notification_sent_for_symbol[symbol] = True
                        # The symbol remains in the lost_connections_file as it's persistently failing.
                await asyncio.sleep(self.refresh_interval)
            except asyncio.CancelledError:
                logger.info("Orchestrator loop cancelled.")
                await self.close_all_connections();
                break
            except Exception as e:
                logger.critical(f"Orchestrator: Unhandled exception in main loop.", exc_info=True)
                await send_telegram_notification_async(f"CRITICAL error in KuCoin orchestrator main loop: {e}",
                                                       level='CRITICAL')
                await asyncio.sleep(self.refresh_interval)

    async def start(self):
        logger.info(f"Starting {self.exchange_id} Spot Collector...")
        await send_telegram_notification_async("KuCoin Spot Collector script is starting up...", level='INFO')
        await self._get_active_priority_ccxt_symbols(reload_markets_now=True)
        await self._orchestrate_priority_connections()

    async def close_all_connections(self):
        logger.info("Closing all active connections...")
        # MODIFICATION: Remove all actively managed symbols from lost file on graceful shutdown
        symbols_being_watched = list(self.active_ws_connections.keys())
        for symbol_to_clear in symbols_being_watched:
            base_asset_to_clear = extract_base_asset(symbol_to_clear)
            manage_lost_connections_file_kucoin_sync([base_asset_to_clear], 'remove', logger, conn_id="shutdown")

        for task in self.active_ws_connections.values():
            if not task.done(): task.cancel()
        if self.active_ws_connections: await asyncio.gather(*self.active_ws_connections.values(),
                                                            return_exceptions=True)
        self.active_ws_connections.clear()
        if self.exchange: await self.exchange.close()
        await send_telegram_notification_async("KuCoin Spot Collector has been shut down.", level='INFO')


async def run_collector():
    collector = None
    try:
        # MODIFICATION: Ensure lost_connections directory exists before collector instantiation
        os.makedirs(LOST_CONNECTIONS_DIR, exist_ok=True)
        collector = KucoinSpotCollector(PRIORITY_SYMBOLS_FILE, REFRESH_INTERVAL_SECONDS, ORDERBOOK_DEPTH)
        await collector.start()
    except asyncio.CancelledError:
        logger.info("Main collector task cancelled.")
    except Exception as e:
        logger.critical(f"CRITICAL error during collector initialization or start: {e}", exc_info=True)
        await send_telegram_notification_async(f"CRITICAL error during KuCoin collector startup: {e}", level='CRITICAL')
    finally:
        if collector: await collector.close_all_connections()


def main_entry_point_kucoin():
    """
    Main entry point for KuCoin Spot Collector.
    """
    # ==============================================================================
    # MODIFICATION: Main execution block now manages log file streams
    # ==============================================================================
    log_streams = None
    try:
        # MODIFICATION: Ensure lost_connections directory exists at the very start
        os.makedirs(LOST_CONNECTIONS_DIR, exist_ok=True)
        log_streams = setup_logging()
        loop = asyncio.get_event_loop()
        main_task = loop.create_task(run_collector())
        loop.run_until_complete(main_task)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Shutting down...")
    except Exception as e:
        logger.critical(f"FATAL unhandled exception at top level: {e}", exc_info=True)
        # Final emergency notification
        asyncio.run(send_telegram_notification_async(f"FATAL SCRIPT CRASH: {e}", level='CRITICAL'))
    finally:
        # Gracefully close tasks and event loop
        loop = asyncio.get_event_loop()
        if 'main_task' in locals() and main_task and not main_task.done():
            main_task.cancel()
            if not loop.is_closed():
                loop.run_until_complete(main_task)  # Allow cancellation to complete

        if not loop.is_closed():
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
            logger.info("Asyncio event loop closed.")

        # --- FIX 3: Close the log file streams ---
        if log_streams:
            logger.info("Closing log file streams...")
            for stream in log_streams:
                stream.close()

        print("Application finished.")  # Use print as logger might be closed