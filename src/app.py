import logging

from lib.consumer import Consumer
from lib.models import Types, Event
from settings import DSN, PUBLICATION_NAME, SLOT_NAME

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    level='INFO',
    datefmt='%d/%m/%Y %X')

logger = logging.getLogger(__name__)


def callback(event: Event):
    logger.info(f"[{event.type}] {event.schema_name}.{event.table_name} with values {event.values}")


consumer = Consumer(DSN)
consumer.on(Types.UPDATE, 'public.actors', callback)
consumer.start(
    slot_name=SLOT_NAME,
    publication_name=PUBLICATION_NAME)
