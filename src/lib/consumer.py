import logging
from typing import Callable

import psycopg2.extras
import lib.pypgoutput.decoders as decoders
from lib.models import Transaction, Event, Types, Field, OID_MAP, DomainEvent

import uuid
from datetime import datetime, date

logger = logging.getLogger(__name__)


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


def get_fields(data, rel, tuple_data):
    fields = [
        Field(
            name=c.name,
            value=convert_value(c.type_id, tuple_data.column_data[i].col_data),
            pkey=c.part_of_pkey == 1
        )
        for i, c in enumerate(rel.columns)
    ]
    return fields


class Consumer:
    domain_events: list[DomainEvent] = []
    events_to_notify: list[tuple[Callable, Event]] = []

    def __init__(self, dsn):
        self.tx = None
        self.rel = None
        self.dsn = dsn

    def start(self, slot_name, publication_name):
        conn = psycopg2.connect(
            self.dsn,
            connection_factory=psycopg2.extras.LogicalReplicationConnection)
        cur = conn.cursor()
        cur.start_replication(
            slot_name=slot_name,
            decode=False,
            options={'proto_version': '1', 'publication_names': publication_name}
        )
        cur.consume_stream(self.get_consumer())

    def on(self, db_type: Types, table: str, callback: callable):
        schema_name, table_name = table.split(".")
        self.domain_events.append(
            DomainEvent(
                type=db_type,
                schema_name=schema_name,
                table_name=table_name,
                callback=callback
            ))

    def on_all(self, table: str, callback: callable):
        schema_name, table_name = table.split(".")
        for db_type in Types:
            self.domain_events.append(
                DomainEvent(
                    type=db_type,
                    schema_name=schema_name,
                    table_name=table_name,
                    callback=callback
                ))

    def append_event_if_registered(self, message_type, payload):
        for domain_event in self.domain_events:
            current_type = Types(message_type)
            if (domain_event.type == current_type and
                    (domain_event.schema_name == '*' or self.rel.namespace == domain_event.schema_name) and
                    (domain_event.table_name == '*' or self.rel.relation_name == domain_event.table_name)):
                event = get_event(message_type, self.rel, self.tx, payload)
                if event:
                    self.events_to_notify.append((domain_event.callback, event))

    def emit_events(self, commit_msg):
        ts = commit_msg.commit_ts
        for callback, event in self.events_to_notify:
            event.tx_id = self.tx.tx_id
            callback(ts, event)

    def get_consumer(self):
        def consume(msg):
            message_type = msg.payload[:1].decode("utf-8")
            payload = msg.payload

            if message_type == "R":
                self.rel = decoders.Relation(payload)
            elif message_type == "B":
                self.events_to_notify = []
                begin_msg = decoders.Begin(payload)
                self.tx = Transaction(
                    tx_id=begin_msg.tx_xid,
                    begin_lsn=begin_msg.lsn,
                    commit_ts=begin_msg.commit_ts)
            elif message_type == "C":
                commit_msg = decoders.Commit(payload)
                self.emit_events(commit_msg)
            else:
                self.append_event_if_registered(message_type, payload)

            msg.cursor.send_feedback(flush_lsn=msg.data_start)

        return consume
