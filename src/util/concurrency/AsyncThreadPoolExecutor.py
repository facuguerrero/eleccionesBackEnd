from concurrent.futures import as_completed, ThreadPoolExecutor

from src.util.config.ConfigurationManager import ConfigurationManager
from src.util.logging.Logger import Logger


class AsyncThreadPoolExecutor:

    def run(self, executable, args_list):
        """ Run executable concurrently as many times as elements in args list. """
        Logger(self.__class__.__name__).info("Starting asynchronous thread pool.")
        max_workers = ConfigurationManager().get_int("max_pool_workers")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(executable, args) for args in args_list]
        results = [as_completed(future) for future in futures]
        Logger(self.__class__.__name__).info("Finished executing tasks in asynchronous thread pool.")
        return results
