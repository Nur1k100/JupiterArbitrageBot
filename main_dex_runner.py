import multiprocessing
from DEX.dex_runner import runner_dex
import logging

logger = logging.getLogger('main_dex_runner')

if __name__ == '__main__':
    multiprocessing.freeze_support()
    logging.basicConfig(level=logging.INFO, filename="logging/runner_dex.log",
                        format="%(asctime)s - %(processName)s (%(process)d) - [%(levelname)s] - %(message)s",
                        force=True)
    logger.info("--- New run (runner_dex.py executed directly by parent) ---")
    runner_dex()