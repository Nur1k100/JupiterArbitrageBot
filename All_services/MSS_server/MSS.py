import asyncio
from websockets.server import serve
import json
from All_services.telegramBot import send_signal
import logging

logging_path = 'logging/MSS.log'
logger = logging.getLogger('MessageSenderServer')
logger.setLevel(logging.INFO)
handler = logging.FileHandler(logging_path)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.propagate = False

async def echo(websocket):
    async for message in websocket:
        try:
            message = json.loads(message)
            logger.info(f"Received message: {message}")
            await send_signal(message)
        except json.decoder.JSONDecodeError as e:
            logger.info(f"Error sending message to websocket: {e}")
        except Exception:
            logger.info(f'Exception raised while processing message {message}')

async def main_async():
    async with serve(echo, "localhost", 8765, ping_interval=None, ping_timeout=None, max_queue=10000) as server:
        logger.info(f"--- New run( MSS.py ) ---")
        await server.serve_forever()


def main():
    asyncio.run(main_async())