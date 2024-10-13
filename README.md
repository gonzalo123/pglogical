# Domain Events in Legacy Applications using Python and PostgreSQL

Sometimes we need to generate domain events in our application. It can be simple when you start an application from scratch, but it can be a nightmare when you have a legacy application. Today we're going to explain how to generate domain events from PostgreSQL. We can set up triggers within our database tables to generate domain events. In those triggers, we can use pg_notify to emit events and after that, we can use a listener to consume those events. This approach works, but we need to set up those triggers in every table that we want to generate events from. Today we're going to use logical replication to generate domain events. With this approach, we can generate events from all tables in our database without the need to set up triggers in every table.

First of all, we need to create a publication. A publication is a set of tables that we want to replicate. We can create a publication with the following SQL command:

```sql
CREATE PUBLICATION pub1 FOR ALL TABLES;
SELECT pg_create_logical_replication_slot('slot1', 'pgoutput');
```

When we create our replication slot, we can choose between different plugins. In this case, we're going to use pgoutput. We can use pgoutput to get the changes in our tables. After that, we can create a subscription to consume those changes. pgoutput is a plugin that is included in PostgreSQL by default that sends the information in bytea format. We can use pgoutput to get the changes in our tables. After that, we can create a subscription to consume those changes. We're going to create the subscription in Python. We can do a simple subscription with the following code:

```python
import psycopg2.extras

from settings import DSN


conn = psycopg2.connect(DSN, connection_factory=psycopg2.extras.LogicalReplicationConnection)
cur = conn.cursor()

cur.start_replication(
    slot_name='slot1', 
    decode=False,
    options={'proto_version': '1', 'publication_names': 'pub1'})


def consume(msg):
    payload = msg.payload
    print(payload)
    msg.cursor.send_feedback(flush_lsn=msg.data_start)


cur.consume_stream(consume)
```

With this simple script, we're listening to all changes in our database. We can use this script to generate domain events in our application. We need to decode the payload to get the changes in our tables. There is a library to decode the payload called [pypgoutput](https://pypi.org/project/pypgoutput/). I have had problems with this library, so I have used only one part of the library to decode the payload (decoders.py).

The main script is like this:

```python
import logging
from datetime import datetime

from lib.consumer import Consumer
from lib.models import Types, Event
from settings import DSN, PUBLICATION_NAME, SLOT_NAME

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    level='INFO',
    datefmt='%d/%m/%Y %X')

logger = logging.getLogger(__name__)


def callback(ts: datetime, event: Event):
    logger.info(
        f"{ts} id:{event.tx_id} [{event.type}] {event.schema_name}.{event.table_name} with values {event.values}")


consumer = Consumer(DSN)
consumer.on(Types.UPDATE, 'public.*', callback)

consumer.start(
    slot_name=SLOT_NAME,
    publication_name=PUBLICATION_NAME)
```

For my example, I am using a database with the following schema:

```sql
CREATE TABLE actors (
    nconst TEXT PRIMARY KEY,
    primaryname TEXT,
    birthyear INTEGER,
    deathyear INTEGER,
    primaryprofession TEXT,
    knownfortitles TEXT
);
```
It is important to activate the logical replication in the database. We can do it with the following command:

```sql
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;
ALTER SYSTEM SET max_worker_processes = 10;
```

And I am registering an event on every update of the actors table with the callback function. The callback function is called with the event that contains the type of the event, the schema name, the table name, and the values of the row that has been updated. In callback function we can do wathever we want with the event. In this case, I am just logging the event, but maybe you can emit this event to a message broker such as Kafka, RabbitMQ or a MQTT broker.

That is the main notification part of the script. The other part is the conversion of the values of the row. The values are in bytea format, so we need to convert them to Python types. The conversion is done with the following function:

```python
def convert_value(oid, value):
    if value is None:
        return None
    python_type = OID_MAP.get(oid, str)
    try:
        if python_type == bool:
            return value == 't'
        elif python_type == datetime:
            return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        elif python_type == date:
            return datetime.strptime(value, '%Y-%m-%d').date()
        elif python_type == dict:
            import json
            return json.loads(value)
        elif python_type == uuid.UUID:
            return uuid.UUID(value)
        else:
            return python_type(value)
    except Exception as e:
        logger.error(f"Error converting {value} with OID {oid}: {e}")
        return value


def get_event(message_type, rel, tx, payload) -> Event | None:
    current_type = Types(message_type)
    decoder_map = {
        Types.INSERT: decoders.Insert,
        Types.UPDATE: decoders.Update,
        Types.DELETE: decoders.Delete,
        Types.TRUNCATE: decoders.Truncate
    }
    data = decoder_map.get(current_type, lambda x: None)(payload)

    if data:
        if current_type == Types.DELETE:
            fields = get_fields(data, rel, data.old_tuple)
        elif current_type == Types.TRUNCATE:
            fields = []
        else:
            fields = get_fields(data, rel, data.new_tuple)

        event = Event(
            type=current_type,
            tx_id=tx.tx_id,
            schema_name=rel.namespace,
            table_name=rel.relation_name,
            values=fields
        )
        return event
    return None
```

When a client is connected we can see it using a simple query:

```sql
SELECT * FROM pg_stat_replication;
```

And also we can see the replication slots with the following query:

```sql
SELECT
    pg_current_wal_lsn() AS current_lsn,
    slot_name,
    restart_lsn,
    confirmed_flush_lsn
FROM
    pg_replication_slots
WHERE
    slot_type = 'logical';
```

The script is just an experiment. Maybe it can be adapted to a real application, but probably it needs more work, especially in data type conversion.

In conclusion, using logical replication to generate domain events from PostgreSQL offers a powerful and flexible approach, especially for legacy systems or applications where modifying the existing database structure is challenging. This method allows us to capture changes across all tables without the need for individual triggers, potentially simplifying event sourcing and change data capture processes. However, it's important to note that this approach comes with its own set of considerations, such as performance impact on the database, handling of large volumes of events, and ensuring data consistency. As with any experimental technique, thorough testing and careful consideration of your specific use case are crucial before implementing this in a production environment. Despite these challenges, the potential for creating a robust, database-driven event system makes this an exciting area for further exploration and development.