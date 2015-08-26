from alembic.migration import MigrationContext
from alembic.autogenerate import compare_metadata
from sqlalchemy.schema import SchemaItem
from sqlalchemy.types import TypeEngine
from sqlalchemy import (create_engine, MetaData, Column,
        Integer, String, Table)
import pprint

engine = create_engine("sqlite://")

engine.execute('''
    create table foo (
        id integer not null primary key,
        old_data varchar,
        x integer
    )''')

engine.execute('''
    create table bar (
        data varchar
    )''')

metadata = MetaData()
Table('foo', metadata,
    Column('id', Integer, primary_key=True),
    Column('data', Integer),
    Column('x', Integer, nullable=False)
)
Table('bat', metadata,
    Column('info', String)
)

mc = MigrationContext.configure(engine.connect())

diff = compare_metadata(mc, metadata)
pprint.pprint(diff, indent=2, width=20)