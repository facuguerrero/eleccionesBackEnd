from concurrent.futures import as_completed, ThreadPoolExecutor

from src.util.config.ConfigurationManager import ConfigurationManager
from src.util.logging.Logger import Logger


class AsyncThreadPoolExecutor:

    def run(self, executable, args_list):
        """ Run executable concurrently as many times as elements in args list. """
        return self._run(executable, args_list)

    def run_multiple_args(self, executable, args_list):
        """ Run an executable that receives N parameters concurrently as many times as elements in args list. """
        return self._run(executable, args_list, multiple=True)

    def _run(self, executable, args_list, multiple=False):
        Logger(self.__class__.__name__).info('Starting asynchronous thread pool.')
        max_workers = ConfigurationManager().get_int('max_pool_workers')
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            Logger(self.__class__.__name__).info('Inside ThreadPoolExecutor context.')
            # TODO: This could be avoided but all single-param calls should send a list of one element
            if multiple: futures = self.__create_futures_multiple_args(executor, executable, args_list)
            else: futures = self.__create_futures(executor, executable, args_list)
            Logger(self.__class__.__name__).info('Futures created.')
            results = [future.result() for future in as_completed(futures)]
        Logger(self.__class__.__name__).info('Finished executing tasks in asynchronous thread pool.')
        return results

    @staticmethod
    def __create_futures_multiple_args(executor, executable, args_list):
        return [executor.submit(executable, *args) for args in args_list]

    @staticmethod
    def __create_futures(executor, executable, args_list):
        return [executor.submit(executable, args) for args in args_list]
