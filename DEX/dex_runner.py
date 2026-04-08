import logging
import os
import signal
import multiprocessing
import time

logger = logging.getLogger("DEX runner")

try:
    from DEX.SwapMC import main_entry_swapMC
    from DEX.tokensFetcher import main_entry_tokens_fetcher
except Exception as e:
    logger.error(f"Failed to import one or more script entry points: {e}. "
                 f"Ensure paths are correct and scripts/functions exist.")
    exit(1)


scripts_run_config = [
    {'name': 'SwapMC', 'target_function': main_entry_swapMC},
    {'name': 'TokensFetcher', 'target_function': main_entry_tokens_fetcher},
]

def runner_dex():
    logger.info('Master script starting all processes...')
    active_processes = []

    for script in scripts_run_config:
        process_name = script['name']
        target_func = script['target_function']

        try:
            process = multiprocessing.Process(target=target_func, name=process_name)
            active_processes.append(process)
            process.start()
            logger.info(f"Started process: {process_name} ( PID: {process.pid} )")
        except Exception as e:
            logger.error(f"Failed to start process: {process_name}")


    def shutdown_handler(sig, frame):
        logger.info('Master script received shutdown signal. Terminating child processes...')

        for t_p in active_processes:
            if t_p.is_alive():
                logger.info(f"Process '{t_p.name}' terminated (PID: {t_p.pid}).")
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
    except Exception as e:
        logger.error(f"Unhandled exception in main script main loop: {e}", exc_info=True)
    finally:
        logger.info("Master script exiting main loop. Ensuring final cleanup (signal handler should have run)...")

        for p in active_processes:
            if p.is_alive():
                p.terminate()
                p.join(timeout=1)
                if p.is_alive():
                    logger.info(f"Process '{p.name}' terminated (PID: {p.pid}).")
                    p.kill()
        logger.info("Master script final cleanup finished.")
