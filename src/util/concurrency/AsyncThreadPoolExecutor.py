import asyncio
import concurrent.futures

from src.util.config.ConfigurationManager import ConfigurationManager
from src.util.logging.Logger import Logger


class AsyncThreadPoolExecutor:

    @staticmethod
    async def _main(executable, args):
        max_workers = ConfigurationManager().get_int("max_pool_workers")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            loop = asyncio.get_event_loop()
            futures = [
                loop.run_in_executor(executor, executable, arg)
                for arg in args
            ]
        return await asyncio.gather(*futures)

    def run(self, executable, args):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        Logger(self.__class__.__name__).info("Starting asynchronous thread pool.")
        results = loop.run_until_complete(self._main(executable, args))
        loop.close()
        return results
