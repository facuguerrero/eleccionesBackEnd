import threading


class Singleton(type):
    _instances = {}
    _locks = {}
    _general_lock = threading.Lock()

    def __call__(cls, *args, **kwargs):

        with cls._general_lock:
            if cls not in cls._locks:
                cls._locks[cls] = threading.Lock()

        with cls._locks[cls]:
            if cls not in cls._instances:
                cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)

        return cls._instances[cls]
