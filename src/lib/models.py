import uuid
from datetime import datetime, date
from decimal import Decimal
from enum import Enum
from typing import List, Callable

from pydantic import BaseModel

OID_MAP = {
    23: int,
    25: str,
    1043: str,
    1700: Decimal,
    16: bool,
    700: float,
    701: float,
    1082: date,
    1114: datetime,
    1184: datetime,
    114: dict,
    2950: uuid.UUID,
}

class Types(str, Enum):
    INSERT = "I",
    UPDATE = "U",
    DELETE = "D"
    TRUNCATE = "T",

class Transaction(BaseModel):
    tx_id: int
    begin_lsn: int
    commit_ts: datetime

class Field(BaseModel):
    name: str
    value: str|float|int|bool|datetime|None
    pkey: bool

class Event(BaseModel):
    type: Types
    tx_id: int
    schema_name: str
    table_name: str
    values: List[Field]

class DomainEvent(BaseModel):
    type: Types
    schema_name: str
    table_name: str
    callback: Callable
