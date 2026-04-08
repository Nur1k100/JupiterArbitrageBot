from All_services.service_runner import runner_service
import multiprocessing
import logging

logger = logging.getLogger("Main Service Runner")

if __name__ == '__main__':
    multiprocessing.freeze_support()
    logging.basicConfig(level=logging.INFO, filename="logging/service_runner.log",
                        format="%(asctime)s - %(processName)s (%(process)d) - [%(levelname)s] - %(message)s",
                        force=True)
    logger.info("--- New run (runner_cex.py executed directly by parent) ---")
    runner_service()