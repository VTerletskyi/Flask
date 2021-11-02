import sys
from loguru import logger as custom_logger


custom_logger.remove()
custom_logger.add(sys.__stdout__, level="INFO")
custom_logger.add(sys.__stderr__, level="ERROR")

logger = custom_logger
