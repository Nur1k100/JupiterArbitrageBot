#!/usr/bin/env python3
"""
CoinEx SPOT Real-time Order Book Data Collector

This script connects to CoinEx WebSocket API to collect real-time order book data
for spot markets and saves it to local JSON files.
"""

import asyncio
import websockets
import ccxt
import aiohttp
import gzip
import json
import os
import time
import uuid
import logging
import sys
import traceback
from datetime import datetime
from typing import Dict, List, Set, Optional, Any
from decimal import Decimal

# Configuration
EXCHANGE_ID = 'coinex'
MARKET_TYPE_TO_WATCH = 'spot'
QUOTE_ASSET = 'USDT'
COINEX_SPOT_WS_URL = 'wss://socket.coinex.com/v2/spot/'
TOPICS_PER_CONNECTION_LIMIT = 50
DEPTH_LEVEL = 20
UPDATE_INTERVAL_MS = 0
OUTPUT_DIRECTORY = 'CEX/orderbooks/coinex_orderbooks'
DEBUG_MARKETS = False

# Reconnection settings
RECONNECT_BASE_DELAY_SECONDS = 5
RECONNECT_MAX_DELAY_SECONDS = 300
MAX_RECONNECT_ATTEMPTS = 10

# Ping/Pong settings
PING_SERVER_INTERVAL_SECONDS = 25
CLIENT_PONG_TIMEOUT_SECONDS = 10

# Telegram settings
TELEGRAM_BOT_TOKEN = ""
TELEGRAM_CHAT_ID = 1
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

# Logging settings
# MODIFICATION: Added MAIN_LOG_FILENAME
MAIN_LOG_FILENAME = 'CEX/app_log/coinex_appX.log'
ERROR_LOG_FILENAME = 'CEX/logging_cex/coinex_spot_collector_errors.log'


# Setup logging
def setup_logging():
    """Configure logging to console and multiple files"""
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Set root to lowest level

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.getLogger("websockets").setLevel(logging.INFO)
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)  # Console shows INFO and above
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    try:
        # Main log file handler (coinex_app.log) - Captures EVERYTHING
        main_handler = logging.FileHandler(MAIN_LOG_FILENAME, mode='a', encoding='utf-8')
        main_handler.setLevel(logging.DEBUG)  # Set to DEBUG to capture all levels
        main_handler.setFormatter(formatter)
        logger.addHandler(main_handler)

        # File handler for errors
        error_handler = logging.FileHandler(ERROR_LOG_FILENAME, mode='a', encoding='utf-8')
        error_handler.setLevel(logging.ERROR)  # Captures only ERROR and CRITICAL
        error_handler.setFormatter(formatter)
        logger.addHandler(error_handler)
    except Exception as e:
        print(f"CRITICAL: Failed to configure file loggers: {e}")

    return logger


logger = setup_logging()


class TelegramNotifier:
    """Handles Telegram notifications"""

    def __init__(self):
        self.enabled = TELEGRAM_ENABLED
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.session = None

    async def initialize(self):
        if self.enabled and not self.session:
            self.session = aiohttp.ClientSession()

    async def close(self):
        if self.session:
            await self.session.close()

    # MODIFICATION: Enhanced send_message with levels and formatting
    async def send_message(self, message: str, level: str = 'INFO'):
        if not self.enabled:
            return

        level_emoji = {'INFO': '✅', 'WARNING': '🟡', 'CRITICAL': '🔴'}
        if "re-established" in message.lower():
            level_emoji['INFO'] = '🟢'
        if "starting up" in message.lower():
            level_emoji['INFO'] = '🚀'

        formatted_message = f"{level_emoji.get(level, '🔵')} **CoinEx WebSocket Monitor**\n\n{message}"

        try:
            if len(formatted_message) > 4096:
                formatted_message = formatted_message[:4093] + "..."

            data = {'chat_id': self.chat_id, 'text': formatted_message, 'parse_mode': 'Markdown'}

            async with self.session.post(url=f"https://api.telegram.org/bot{self.bot_token}/sendMessage",
                                         json=data) as response:
                if response.status != 200:
                    logger.error(f"Telegram API error: {await response.text()}")
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")


class CoinExSpotCollector:
    """Main collector class for CoinEx spot order books"""

    def __init__(self):
        self.exchange = None
        self.markets = {}
        self.active_symbols = []
        self.order_books = {}
        self.ws_connections = {}
        self.message_id_counter = 1
        self.notifier = TelegramNotifier()
        self.running = True
        os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

        # Added for lost connection tracking
        self.lost_connections_lock = asyncio.Lock()
        self.lost_connections_dir = 'CEX/lost_connections'
        self.lost_connections_filepath = os.path.join(self.lost_connections_dir, f"{EXCHANGE_ID}_lost.json")
        os.makedirs(self.lost_connections_dir, exist_ok=True)
        logger.info(f"Lost connections will be tracked in: {os.path.abspath(self.lost_connections_filepath)}")

    async def initialize(self):
        """Initialize exchange and fetch markets (without sending notifications)"""
        try:
            logger.info("Initializing CoinEx exchange connection...")
            self.exchange = ccxt.coinex(
                {'enableRateLimit': True, 'rateLimit': 1000, 'options': {'defaultType': 'spot'}})

            await self.notifier.initialize()

            logger.info("Fetching market data...")
            markets = await asyncio.to_thread(self.exchange.load_markets, True)

            if DEBUG_MARKETS and markets:
                logger.info("DEBUG MODE - Showing first 5 markets:")
                for i, (symbol, market) in enumerate(list(markets.items())[:5]):
                    logger.info(f"Market {i + 1}: {symbol} - Info: {market.get('info', {})}")

            self.markets = {
                symbol: market for symbol, market in markets.items()
                if (market.get('type') == 'spot' or market.get('spot', False)) and
                   market.get('quote') == QUOTE_ASSET and
                   market.get('active') is not False and
                   market.get('info', {}).get('status') != 'offline'
            }

            self.active_symbols = list(self.markets.keys())
            logger.info(f"Found {len(self.active_symbols)} active {QUOTE_ASSET} spot markets")
            return True
        except Exception as e:
            logger.critical(f"Failed to initialize exchange or load markets.", exc_info=True)
            await self.notifier.send_message(f"CRITICAL: Failed to load markets from CoinEx.\n\n**Error:**\n`{e}`",
                                             level='CRITICAL')
            return False

    def get_next_message_id(self) -> int:
        current_id = self.message_id_counter;
        self.message_id_counter += 1;
        return current_id

    async def save_order_book(self, symbol: str, order_book: Dict):
        try:
            filename = os.path.join(OUTPUT_DIRECTORY, f"{symbol.replace('/', '')}.json")
            asks = sorted(order_book.get('asks', []), key=lambda x: float(x[0]))
            bids = sorted(order_book.get('bids', []), key=lambda x: float(x[0]), reverse=True)
            data = {'symbol': symbol, 'asks': asks, 'bids': bids,
                    'timestamp': order_book.get('timestamp', int(time.time() * 1000)),
                    'datetime': datetime.utcnow().isoformat(), 'last_price': order_book.get('last_price'),
                    'checksum': order_book.get('checksum')}
            await asyncio.to_thread(lambda: json.dump(data, open(filename, 'w'), indent=2))
        except Exception as e:
            logger.error(f"Failed to save order book for {symbol}: {e}")

    async def handle_message(self, ws, message: bytes, connection_id: str):
        try:
            if message.startswith(b'\x1f\x8b'): message = gzip.decompress(message)
            if isinstance(message, bytes): message = message.decode('utf-8')
            data = json.loads(message)
            method = data.get('method')
            if method == 'depth.update':
                await self.handle_depth_update(data)
            elif data.get('id') and data.get('code') != 0:
                logger.error(f"Error response: {data}")
        except Exception as e:
            logger.error(f"Error handling message: {e}", exc_info=True)

    async def handle_depth_update(self, data: Dict):
        try:
            update_data, market_id = data.get('data', {}), data.get('data', {}).get('market')
            if not market_id: return
            symbol = self.get_symbol_from_market_id(market_id)
            if not symbol: return
            depth, is_full = update_data.get('depth', {}), update_data.get('is_full', False)
            if is_full:
                self.order_books[symbol] = {'asks': [[p, s] for p, s in depth.get('asks', [])],
                                            'bids': [[p, s] for p, s in depth.get('bids', [])],
                                            'timestamp': update_data.get('updated_at'), 'last_price': depth.get('last'),
                                            'checksum': depth.get('checksum')}
            elif symbol in self.order_books:
                asks_dict, bids_dict = {a[0]: a[1] for a in self.order_books[symbol]['asks']}, {b[0]: b[1] for b in
                                                                                                self.order_books[
                                                                                                    symbol]['bids']}
                for p, s in depth.get('asks', []):
                    if Decimal(s) == 0:
                        asks_dict.pop(p, None)
                    else:
                        asks_dict[p] = s
                for p, s in depth.get('bids', []):
                    if Decimal(s) == 0:
                        bids_dict.pop(p, None)
                    else:
                        bids_dict[p] = s
                self.order_books[symbol]['asks'], self.order_books[symbol]['bids'], self.order_books[symbol][
                    'timestamp'], self.order_books[symbol]['last_price'], self.order_books[symbol]['checksum'] = [[p, s]
                                                                                                                  for
                                                                                                                  p, s
                                                                                                                  in
                                                                                                                  asks_dict.items()], [
                    [p, s] for p, s in bids_dict.items()], update_data.get('updated_at'), depth.get('last'), depth.get(
                    'checksum')
            await self.save_order_book(symbol, self.order_books[symbol])
        except Exception as e:
            logger.error(f"Error handling depth update: {e}", exc_info=True)

    def get_symbol_from_market_id(self, market_id: str) -> Optional[str]:
        for symbol, market in self.markets.items():
            if market['id'] == market_id: return symbol
        return None

    def get_market_id_from_symbol(self, symbol: str) -> Optional[str]:
        market = self.markets.get(symbol);
        return market['id'] if market else None

    async def ping_task(self, ws, connection_id: str):
        try:
            while self.running and not ws.closed:
                await ws.send(json.dumps({"method": "server.ping", "params": {}, "id": self.get_next_message_id()}))
                await asyncio.sleep(PING_SERVER_INTERVAL_SECONDS)
        except:
            pass

    async def _update_lost_connections_file(self, symbols: List[str], add: bool):
        """Updates the JSON file tracking lost connections."""
        async with self.lost_connections_lock:
            try:
                if os.path.exists(self.lost_connections_filepath):
                    with open(self.lost_connections_filepath, 'r') as f:
                        lost_data = json.load(f)
                else:
                    lost_data = {}

                for symbol in symbols:
                    base_asset = symbol.split('/')[0]  # Extract base asset (e.g., SOL from SOL/USDT)
                    if add:
                        lost_data[base_asset] = "lost"
                    else:
                        if base_asset in lost_data:
                            del lost_data[base_asset]

                with open(self.lost_connections_filepath, 'w') as f:
                    json.dump(lost_data, f, indent=4)

                logger.info(
                    f"Updated lost connections file: {self.lost_connections_filepath} (add={add}, symbols={symbols})")
                if not lost_data and os.path.exists(self.lost_connections_filepath):
                    logger.info(f"Lost connections file is now empty, deleting: {self.lost_connections_filepath}")
                    os.remove(self.lost_connections_filepath)

            except Exception as e:
                logger.error(f"Error updating lost connections file: {e}", exc_info=True)

    async def _notify_lost_connection(self, connection_id: str, symbols: List[str], error_message: str):
        """Handles actions when a connection is lost."""
        logger.warning(f"Connection lost for {connection_id} (Symbols: {', '.join(symbols)}). Error: {error_message}")
        await self._update_lost_connections_file(symbols, add=True)
        base_symbols_str = ", ".join(sorted(list(set(s.split('/')[0] for s in symbols))))
        await self.notifier.send_message(
            f"Connection LOST for worker `{connection_id}`.\n"
            f"Affected symbols (base): `{base_symbols_str}`.\n"
            f"Attempting to reconnect...\n"
            f"**Error:** `{error_message}`",
            level='WARNING'
        )

    async def _notify_reestablished_connection(self, connection_id: str, symbols: List[str]):
        """Handles actions when a connection is re-established."""
        logger.info(f"Connection re-established for {connection_id} (Symbols: {', '.join(symbols)}).")
        await self._update_lost_connections_file(symbols, add=False)
        base_symbols_str = ", ".join(sorted(list(set(s.split('/')[0] for s in symbols))))
        await self.notifier.send_message(
            f"Connection RE-ESTABLISHED for worker `{connection_id}`.\n"
            f"Symbols now active: `{base_symbols_str}`.",
            level='INFO'
        )

    # MODIFICATION: Rewritten connection handler for better notifications
    async def handle_connection(self, symbols: List[str], connection_id: str):
        reconnect_delay = RECONNECT_BASE_DELAY_SECONDS
        reconnect_attempts = 0  # Number of consecutive failed attempts
        connection_ever_established = False  # Has this worker ever successfully connected?

        while self.running:
            last_exception_in_attempt: Optional[Exception] = None
            successfully_processed_messages_this_attempt = False
            ping_task_obj = None  # Initialize ping_task_obj

            try:
                logger.info(f"Connecting WebSocket {connection_id} for {len(symbols)} symbols: {', '.join(symbols)}...")
                async with websockets.connect(COINEX_SPOT_WS_URL, ping_interval=None, close_timeout=10) as ws:
                    self.ws_connections[connection_id] = ws

                    # If it's a reconnect or was previously connected and might have been marked lost.
                    if connection_ever_established or reconnect_attempts > 0:
                        await self._notify_reestablished_connection(connection_id, symbols)

                    connection_ever_established = True  # Mark that at least one connection succeeded
                    reconnect_attempts = 0  # Reset failed attempts counter for this healthy connection period
                    reconnect_delay = RECONNECT_BASE_DELAY_SECONDS  # Reset delay

                    market_list = [[self.get_market_id_from_symbol(s), DEPTH_LEVEL, str(UPDATE_INTERVAL_MS), True] for s
                                   in symbols if self.get_market_id_from_symbol(s)]
                    if market_list:
                        await ws.send(json.dumps({"method": "depth.subscribe", "params": {"market_list": market_list},
                                                  "id": self.get_next_message_id()}))
                        logger.info(f"Subscribed to {len(market_list)} markets on {connection_id}")

                    ping_task_obj = asyncio.create_task(self.ping_task(ws, connection_id))

                    async for message in ws:
                        await self.handle_message(ws, message, connection_id)
                        successfully_processed_messages_this_attempt = True
                        # reconnect_attempts is already 0 if we are in this loop after a successful connect.

                    # If 'async for' loop finishes, server closed connection gracefully.
                    if ping_task_obj and not ping_task_obj.done():
                        ping_task_obj.cancel()
                        try:
                            await ping_task_obj
                        except asyncio.CancelledError:
                            logger.debug(f"Ping task for {connection_id} cancelled due to graceful server closure.")

                    if self.running:  # Only if not shutting down
                        logger.info(f"WebSocket {connection_id} connection closed gracefully by server.")
                        last_exception_in_attempt = Exception("Server closed connection gracefully")

            except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError,
                    websockets.exceptions.InvalidStatusCode) as e:
                last_exception_in_attempt = e
                logger.warning(f"WebSocket {connection_id} connection issue: {e} (Symbols: {', '.join(symbols)})")
            except Exception as e:
                last_exception_in_attempt = e
                logger.error(f"Unexpected error in WebSocket {connection_id} (Symbols: {', '.join(symbols)})",
                             exc_info=True)
            finally:
                if connection_id in self.ws_connections:
                    del self.ws_connections[connection_id]
                if ping_task_obj and not ping_task_obj.done():
                    ping_task_obj.cancel()
                    try:
                        await ping_task_obj  # Ensure it's awaited if cancelled in finally
                    except asyncio.CancelledError:
                        logger.debug(f"Ping task for {connection_id} cancelled in finally block.")

            # --- Post-connection attempt ---
            if not self.running:
                logger.info(f"Shutdown initiated for {connection_id}. Cleaning up.")
                # If it was ever connected, and this last attempt either failed or didn't yield messages before shutdown
                if connection_ever_established and (
                        last_exception_in_attempt or not successfully_processed_messages_this_attempt):
                    await self._update_lost_connections_file(symbols, add=True)
                break  # Exit while loop

            if last_exception_in_attempt:  # An issue occurred (exception or graceful server close)
                # This is the first failure after a successful period OR an initial failure.
                if reconnect_attempts == 0:
                    await self._notify_lost_connection(connection_id, symbols, str(last_exception_in_attempt))

                reconnect_attempts += 1
                logger.info(
                    f"Worker {connection_id} attempt {reconnect_attempts}/{MAX_RECONNECT_ATTEMPTS} after failure/closure for symbols: {', '.join(symbols)}.")

                if reconnect_attempts >= MAX_RECONNECT_ATTEMPTS:
                    logger.critical(
                        f"Max reconnection attempts reached for {connection_id}. Worker will stop. Symbols: {', '.join(symbols)}")
                    base_symbols_str = ', '.join(sorted(list(set(s.split('/')[0] for s in symbols))))
                    # Use traceback.format_exc() if it's a real exception, otherwise just the string form.
                    error_details = traceback.format_exc() if isinstance(last_exception_in_attempt,
                                                                         Exception) and not str(
                        last_exception_in_attempt) == "Server closed connection gracefully" else str(
                        last_exception_in_attempt)

                    critical_msg = (
                        f"CRITICAL: Worker `{connection_id}` giving up after {reconnect_attempts} attempts.\\n"
                        f"Affected symbols (base): `{base_symbols_str}`\\n"
                        f"**Final Error:**\\n```\\n{last_exception_in_attempt}\\n```\n"
                        f"**Traceback (if applicable):**\n```\n{error_details[:1000]}\n```"
                    )
                    await self.notifier.send_message(critical_msg, level='CRITICAL')
                    break  # Stop this worker

                logger.info(f"Reconnecting {connection_id} in {reconnect_delay} seconds...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_DELAY_SECONDS)

            elif self.running:  # last_exception_in_attempt is None, but we are still running
                logger.error(
                    f"Worker {connection_id} reached unexpected state (no error, but loop ended). Retrying after delay. Symbols: {', '.join(symbols)}")
                reconnect_attempts += 1
                await asyncio.sleep(RECONNECT_BASE_DELAY_SECONDS)  # Basic delay before retrying loop

    async def run(self):
        """Main run loop"""
        # MODIFICATION: Separated startup notification from initialize
        await self.notifier.initialize()
        await self.notifier.send_message("CoinEx Spot Collector script is starting up...", level='INFO')

        if not await self.initialize():
            await self.shutdown();
            return

        if not self.active_symbols:
            logger.error("No active symbols found. Please check configuration.")
            await self.notifier.send_message(f"CRITICAL: No active markets found for Quote Asset `{QUOTE_ASSET}`.",
                                             level='CRITICAL')
            await self.shutdown();
            return

        try:
            symbol_groups = [self.active_symbols[i:i + TOPICS_PER_CONNECTION_LIMIT] for i in
                             range(0, len(self.active_symbols), TOPICS_PER_CONNECTION_LIMIT)]
            logger.info(f"Creating {len(symbol_groups)} WebSocket connections...")

            tasks = [asyncio.create_task(self.handle_connection(symbols, f"conn_{i + 1}")) for i, symbols in
                     enumerate(symbol_groups)]

            # MODIFICATION: Send detailed success notification
            success_message = (
                f"CoinEx Market Monitor Started Successfully!\n\n"
                f"• **Tracking:** `{len(self.active_symbols)}` symbols\n"
                f"• **Connections:** `{len(symbol_groups)}` workers launched\n"
                f"• **Topics/Connection:** `{TOPICS_PER_CONNECTION_LIMIT}`"
            )
            await self.notifier.send_message(success_message, level='INFO')

            await asyncio.gather(*tasks)

        except KeyboardInterrupt:
            logger.info("Received shutdown signal...")
        except Exception as e:
            logger.critical(f"Unexpected error in main run loop", exc_info=True)
            await self.notifier.send_message(f"CRITICAL: Unexpected error in main loop.\n\n**Error:** `{e}`",
                                             level='CRITICAL')
        finally:
            await self.shutdown()

    async def shutdown(self):
        logger.info("Shutting down...")
        if not self.running: return
        self.running = False
        for ws in self.ws_connections.values():
            if not ws.closed: await ws.close()
        await self.notifier.send_message("🛑 CoinEx Spot Collector has been shut down.", level='INFO')
        await self.notifier.close()
        if self.exchange and hasattr(self.exchange, 'close'):
            await asyncio.to_thread(self.exchange.close)
        logger.info("Shutdown complete")


async def main():
    collector = CoinExSpotCollector()
    try:
        await collector.run()
    except KeyboardInterrupt:
        pass
    except Exception:
        logger.critical("Fatal error in main execution.", exc_info=True)


def main_entry_point_coinex():
    """
    Main entry point for CoinEx.
    :return:
    """
    os.makedirs(os.path.dirname(ERROR_LOG_FILENAME), exist_ok=True)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script terminated by user.")
    finally:
        logger.info("Program finished.")