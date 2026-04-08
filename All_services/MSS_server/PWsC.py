import asyncio
import websockets
import logging

logging_path = 'logging/PWsC.log'
logger = logging.getLogger('PersistenceWebSocketsConnection')
logger.setLevel(logging.INFO)
handler = logging.FileHandler(logging_path)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.propagate = False


class PersistenceWebSocketsConnection:
    def __init__(self, uri):
        logger.info(f"--- New run( PWsC.py ) ---")
        self.uri = uri
        self.websocket = None
        self._is_connected = False

    async def connect(self):
        if not self._is_connected or not self.websocket or self.websocket.closed:
            try:
                logger.info("Connecting to websocket...")
                self.websocket = await websockets.connect(self.uri)
                self._is_connected = True
                logger.info("Connected to websocket.")
            except Exception as e:
                logger.info(f"Error connecting to websocket: {e}")
                self._is_connected = False
                self.websocket = None
                return None
        return self.websocket

    async def send(self, message):
        if not self._is_connected or self.websocket is None:
            await self.connect()

        if self.websocket:
            try:
                await self.websocket.send(message)
            except websockets.exceptions.ConnectionClosed:
                logger.info("Websocket connection closed.")
                self._is_connected = False
                await self.connect()
                if self._is_connected:
                    await self.send(message)
                else:
                    logger.info("Repeat connection was not successful.")
            except Exception as e:
                logger.info(f"Error sending message to websocket: {e}")

    async def close(self):
        if self.websocket and self._is_connected:
            try:
                logger.info("Closing websocket connection...")
                await self.websocket.close()
                self._is_connected = False
                self.websocket = None
                logger.info("Websocket connection closed.")
            except Exception as e:
                logger.info(f"Error closing websocket connection: {e}")

    # async def __aenter__(self):
    #     await self.connect()
    #     return self
    #
    # async def __aexit__(self, exc_type, exc_val, exc_tb):
    #     await self.close()