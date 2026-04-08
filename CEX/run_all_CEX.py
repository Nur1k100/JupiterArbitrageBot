import multiprocessing
import logging
import time
import signal
import os

logger = logging.getLogger("MasterRunner")
logging.basicConfig(level=logging.DEBUG, filename="CEX/logging_cex/run_all_CEX.log",)

try:
    from CEX.cex_network_fetcher import main_entry_point_CNF
    from CEX.HTX import main_entry_point_htx
    from CEX.BingX import main_entry_point_bingx
    from CEX.bybit import main_entry_point_bybit
    from CEX.Coinex import main_entry_point_coinex
    from CEX.mexc import main_entry_point_mexc
    from CEX.bitmart import main_entry_point_bitmart
    from CEX.gateio import main_entry_point_gateio
    from CEX.kucoin import main_entry_point_kucoin

except ImportError as e:
    logger.critical(f"Failed to import one or more script entry points: {e}. "
                           f"Ensure paths are correct and scripts/functions exist.")
    exit(1)

scripts_to_run_config = [
    {"name": "CEXNetworkFetcher", "target_function": main_entry_point_CNF},
    {"name": "HTX_Handler", "target_function": main_entry_point_htx},
    {"name": "BingX_Handler", "target_function": main_entry_point_bingx},
    {"name": "Bybit_Handler", "target_function": main_entry_point_bybit},
    {"name": "Coinex_Handler", "target_function": main_entry_point_coinex},
    {"name": "MEXC_Handler", "target_function": main_entry_point_mexc},
    {"name": "Bitmart_Handler", "target_function": main_entry_point_bitmart},
    {"name": "GateIO_Handler", "target_function": main_entry_point_gateio},
    {"name": "KUCOIN_Handler", "target_function": main_entry_point_kucoin},
]

def runner_cex():
    logger.info("Master script starting all processes...")
    active_processes = []

    for script_conf in scripts_to_run_config:
        process_name = script_conf["name"]
        target_func = script_conf["target_function"]

        logger.info(f"Preparing to start process '{process_name}'...")
        try:
            process = multiprocessing.Process(
                target=target_func,
                name=process_name
            )
            active_processes.append(process)
            process.start()
            logger.info(f"Process '{process_name}' started (PID: {process.pid}).")
        except Exception as exc:
            logger.error(f"Failed to start process '{process_name}': {exc}", exc_info=True)


    def shutdown_handler(sig, frame):
        logger.info("Master script received shutdown signal. Terminating child processes...")
        for t_p in active_processes:
            if t_p.is_alive():
                logger.info(f"Terminating process '{t_p.name}' (PID: {t_p.pid})...")
                t_p.terminate()

        time.sleep(2)

        for k_p in active_processes:
            if k_p.is_alive():
                logger.warning(
                    f"Process '{k_p.name}' (PID: {k_p.pid}) did not terminate gracefully. Forcing kill...")
                k_p.kill()
            k_p.join()
        logger.info("All child processes have been handled.")
        os._exit(0)


    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        while True:
            any_alive = False
            for i, p in enumerate(active_processes):
                if p.is_alive():
                    any_alive = True
                else:
                    if not hasattr(p, '_terminated_logged'):
                        exitcode = p.exitcode
                        logger.info(
                            f"Process '{p.name}' (PID: {p.pid}) has terminated with exit code {exitcode}.")
                        p._terminated_logged = True

            if not any_alive and active_processes:
                logger.info("All managed processes have terminated. Master script exiting.")
                break
            elif not active_processes:
                logger.info("No processes were configured or started. Master script exiting.")
                break
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Master script main loop interrupted. Shutdown handler should take over.")
    except Exception as exc:
        logger.critical(f"Unhandled exception in master script main loop: {exc}", exc_info=True)
    finally:
        logger.info(
            "Master script exiting main loop. Ensuring final cleanup (signal handler should have run)...")
        for p in active_processes:
            if p.is_alive():
                p.terminate()
                p.join(timeout=1)
                if p.is_alive():
                    p.kill()
        logger.info("Master script final cleanup finished.")