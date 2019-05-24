import types
from unittest import TestCase

from src.util.logging.Logger import Logger
from test.meta.MockLogger import MockLogger


class CustomTestCase(TestCase):

    def setUp(self) -> None:
        setattr(Logger,
                MockLogger.build_logger.__name__,
                types.MethodType(MockLogger.build_logger, Logger))
