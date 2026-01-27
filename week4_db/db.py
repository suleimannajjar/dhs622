from config import *
import sqlalchemy as sa
from sqlalchemy.sql.schema import Table as SQLAlchemyTable
from datetime import datetime


def instantiate_channel_metadata_table(my_table_name: str) -> SQLAlchemyTable:
    my_table = sa.Table(
        my_table_name,
        meta,
        sa.Column("channel_id", sa.types.BIGINT, primary_key=True),
        sa.Column("channel_name", sa.types.TEXT, unique=True),
        sa.Column("channel_title", sa.types.TEXT, default=None),
        sa.Column("channel_birthdate", sa.types.DateTime(timezone=True)),
        sa.Column("channel_bio", sa.types.TEXT, default=None),
        sa.Column("num_subscribers", sa.types.INTEGER, default=None),
        sa.Column("data_source", sa.types.TEXT, default="telegram-api"),
        sa.Column(
            "checkup_time", sa.types.DateTime(timezone=True), default=datetime.utcnow
        ),
        sa.Column("api_response", sa.types.JSON, nullable=False),
    )
    return my_table


def instantiate_seed_table(my_table_name: str) -> SQLAlchemyTable:
    my_table = sa.Table(
        my_table_name,
        meta,
        sa.Column("channel_id", sa.types.BIGINT, primary_key=True),
        sa.Column("channel_name", sa.types.TEXT, nullable=False),
        sa.Column("seed_list", sa.types.TEXT, primary_key=True),
    )
    return my_table


def instantiate_channel_messages_table(my_table_name: str) -> SQLAlchemyTable:
    my_table = sa.Table(
        my_table_name,
        meta,
        sa.Column("channel_id", sa.types.BIGINT, primary_key=True),
        sa.Column("message_id", sa.types.INTEGER, primary_key=True),
        sa.Column("message_datetime", sa.types.DateTime(timezone=True)),
        sa.Column("message_views", sa.types.INTEGER, default=None),
        sa.Column("message_forwards", sa.types.INTEGER, default=None),
        sa.Column("message_text", sa.types.TEXT, default=None),
        sa.Column("forwardee_channel_id", sa.types.BIGINT, default=None),
        sa.Column("forwardee_message_id", sa.types.INTEGER, default=None),
        sa.Column("message_is_forward", sa.types.BOOLEAN, default=None),
        sa.Column("data_source", sa.types.TEXT, default="telegram-api"),
        sa.Column(
            "checkup_time", sa.types.DateTime(timezone=True), default=datetime.utcnow
        ),
        sa.Column("api_response", sa.types.JSON, nullable=False),
    )
    return my_table


def insert_data_into_channel_metadata_table(records: list[dict]):
    # TODO: identify channels already in table
    # TODO: remove those from the incoming records
    # TODO: write remainder to table

    stmt = sa.insert(channel_metadata_table).values(records)
    with engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()

def insert_data_into_channel_messages_table(records: list[dict]):
    """
    This function receives a list of records to insert into the channel messages SQL table.
    That table, however, imposes a primary key restriction on unique tuples (channel_id, message_id).
    Incoming messages should therefore be partitioned into new_records and duplicate_records, where
    new records have (channel_id, message_id) tuples that do not match any records in the table, while
    # duplicate records do already have matches in the table. These two partitions should be handled
    differently.
    """

    stmt = sa.select(
        channel_message_table.c.channel_id, channel_message_table.c.message_id
    ).filter(
        channel_message_table.c.channel_id.in_(
            [record["channel_id"] for record in records]
        ),
        channel_message_table.c.message_id.in_(
            [record["message_id"] for record in records]
        ),
    )

    with engine.connect() as conn:
        rp = conn.execute(stmt)
        rows = rp.fetchall()
    if len(rows) > 0:
        # there is some overlap, so there would be an integrity violation if you attempted to write
        # these records to the database. Instead, let's partition the records to those that are ok
        # to write to the table, and those that are not ok
        new_records = [
            record
            for record in records
            if (record["channel_id"], record["message_id"]) not in rows
        ]
        duplicate_records = [record for record in records if record not in new_records]
    else:
        new_records = records
        duplicate_records = []

    if len(new_records) > 0:
        stmt = sa.insert(channel_message_table).values(new_records)
        with engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
    if len(duplicate_records) > 0:
        pass  # Exercise: add code here to update rows, replacing old data with new

    return


def insert_data_into_seed_table(records: list[dict]):
    # TODO: identify channels already in table
    # TODO: remove those from the incoming records
    # TODO: write remainder to table

    stmt = sa.insert(seed_table).values(records)
    with engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()


def fetch_channel_ids(channel_names: list[str]) -> list[int]:
    with engine.connect() as conn:
        rp = conn.execute(
            sa.select(channel_metadata_table.c.channel_id).where(channel_metadata_table.c.channel_name.in_(channel_names))
        )
    records = [dict(elt._mapping) for elt in rp.fetchall()]
    return [record['channel_id'] for record in records]

def fetch_time_series_chart_data(
    seed_channel_names: list[str],
    start_date: str,
    end_date: str,
    time_series_chart_unit: str,
) -> list[dict]:

    # get seed_channel_ids from seed_channel_names:
    seed_channel_ids = fetch_channel_ids(seed_channel_names)

    stmt = (
        sa.select(
            sa.sql.func.date_trunc(
                time_series_chart_unit, channel_message_table.c.message_datetime
            ),
            sa.sql.func.count(),
        )
        .filter(
            channel_message_table.c.channel_id.in_(seed_channel_ids),
            channel_message_table.c.message_datetime
            >= datetime.strptime(start_date, "%Y-%m-%d"),
            channel_message_table.c.message_datetime
            <= datetime.strptime(end_date, "%Y-%m-%d"),
        )
        .group_by(
            sa.sql.func.date_trunc(
                time_series_chart_unit, channel_message_table.c.message_datetime
            )
        )
        .order_by(
            sa.sql.func.date_trunc(
                time_series_chart_unit, channel_message_table.c.message_datetime
            )
        )
    )

    with engine.connect() as conn:
        rp = conn.execute(stmt)
    records = [dict(elt._mapping) for elt in rp.fetchall()]
    return [
        {"message_dt": record["date_trunc_1"], "count": record["count_1"]}
        for record in records
    ]


engine = sa.create_engine(
    f"postgresql://"
    f"{config['telegram-db']['user']}:"
    f"{config['telegram-db']['password']}"
    f"@{config['telegram-db']['host']}:"
    f"{config['telegram-db']['port']}/"
    f"{config['telegram-db']['dbname']}",
    echo=True,
)

channel_message_table_name = "channel_messages"
channel_metadata_table_name = "channel_metadata"
seed_table_name = "seeds"

# Define tables and create them if they don't already exist
meta = sa.MetaData()
channel_metadata_table = instantiate_channel_metadata_table(channel_metadata_table_name)
seed_table = instantiate_seed_table(seed_table_name)
channel_message_table = instantiate_channel_messages_table(channel_message_table_name)
meta.create_all(engine)