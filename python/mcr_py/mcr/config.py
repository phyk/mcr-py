from logging import Logger
from typing import Optional

from mcr_py.mcr.path import PathManager
from mcr_py.utils.logger import Timer, rlog


class MCRConfig:
    def __init__(
        self,
        logger: Logger = rlog,
        enable_limit: bool = False,
        disable_paths: bool = False,
    ) -> None:
        self.logger = logger
        self.timer = Timer(self.logger)
        self.path_manager: Optional[PathManager] = None
        self.enable_limit = enable_limit
        self.disable_paths = disable_paths
        if not disable_paths:
            self.path_manager = PathManager()
