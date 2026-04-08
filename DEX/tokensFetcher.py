import aiohttp
import logging
import json
import asyncio
import time

logging_path = 'logging/tokensFetcher.log'
logger = logging.getLogger('TokensFetcher')
logger.setLevel(logging.INFO)
handler = logging.FileHandler(logging_path)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.propagate = False

PATH = "DEX/DEX_json/tokens_Jupiter.json"
URL_TOKENS = "https://tokens.jup.ag/tokens?tags=strict,lst,verified"

def __upload_tokens_to_json(tokens):
    with open(PATH, 'w') as f:
        json.dump(tokens, f, indent=4)

async def __fetch_tokens(session) -> dict or None:
    try:
        async with session.get(URL_TOKENS) as response:
            response.raise_for_status()
            data = await response.json()
            return data
    except aiohttp.ClientResponseError as e:
        logger.info("HTTP error occurred:", e)
        return None
    except aiohttp.ClientError as e:
        logger.info("Error during request:", e)
        return None

async def main():
    async with aiohttp.ClientSession() as session:
        tokens = await __fetch_tokens(session)

    if not tokens:
        logger.info("Error: Could not fetch tokens from Jupiter API.")
        return

    jupiter_json = {
        "tokenOutUSDT": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
        "tokenOutUSDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "tokens": []
    }

    for token in tokens:
        if token.get("daily_volume") is None or token.get("daily_volume") < 30_000:
            continue
        jupiter_json["tokens"].append({
            "address": token.get("address"),
            "symbol": token.get("symbol"),
            "decimals": token.get("decimals"),
            "daily_volume": token.get("daily_volume"),
        })

    __upload_tokens_to_json(jupiter_json)
    logger.info("Tokens fetched successfully.")


def main_entry_tokens_fetcher():
    logger.info("--- New run (tokensFetcher.py) ---")
    try:
        while True:
            asyncio.run(main())
            logger.info("Tokens fetched successfully.")
            logger.info("Sleeping for 3 hours...")
            time.sleep(10800)
    except Exception as e:
        logger.info(f"Unexpected error: {type(e).__name__} - {e}")
