import logging

from ..app import BeanQueue
from ..utils import load_module_var

logger = logging.getLogger(__name__)


def load_app(app: str | None) -> BeanQueue:
    if app is None:
        logger.info("No BeanQueue app provided, create default app")
        return BeanQueue()
    logger.info("Load BeanQueue app from %s", app)
    return load_module_var(app)
