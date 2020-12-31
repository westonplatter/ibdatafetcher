from datetime import date
import pandas as pd
from typing import List, Dict, Tuple
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    Numeric,
    String,
    DateTime,
    Index,
    Boolean,
)
from loguru import logger
from psycopg2 import sql
from psycopg2.errors import UniqueViolation
from sqlalchemy.exc import IntegrityError


Base = declarative_base()


NUMERIC_OPTIONS = dict(precision=8, scale=2, decimal_return_scale=None, asdecimal=True)


def gen_engine():
    connection_string: str = "postgresql://localhost:5432/ibdatafetcher"
    engine = create_engine(connection_string, convert_unicode=True)
    return engine


def init_db(engine):
    Base.metadata.create_all(bind=engine)


class Quote(Base):
    __tablename__ = "quote"

    id = Column(Integer, primary_key=True)
    ts = Column(DateTime(timezone=True), index=True)
    symbol = Column(String(10))
    local_symbol = Column(String(20))
    open = Column(Numeric(**NUMERIC_OPTIONS))
    close = Column(Numeric(**NUMERIC_OPTIONS))
    high = Column(Numeric(**NUMERIC_OPTIONS))
    low = Column(Numeric(**NUMERIC_OPTIONS))
    average = Column(Numeric(**NUMERIC_OPTIONS))
    volume = Column(Integer)
    bar_count = Column(Integer)
    rth = Column(Boolean)
    value_type = Column(String(20))

    __table_args__ = (
        Index(
            "ix_quote_local_symbol_ts_value_type",
            local_symbol,
            ts,
            value_type,
            unique=True,
        ),
    )


def db_insert_df_conflict_on_do_nothing(
    engine, df: pd.DataFrame, table_name: str
) -> None:
    cols = __gen_cols(df)
    values = __gen_values(df)

    # TODO(weston) make it work like, https://stackoverflow.com/questions/4038616/get-count-of-records-affected-by-insert-or-update-in-postgresql
    query_template = "INSERT INTO {table_name} ({cols}) VALUES ({values});"

    query = sql.SQL(query_template).format(
        table_name=sql.Identifier(table_name),
        cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
        values=sql.SQL(", ").join(sql.Placeholder() * len(cols)),
    )

    with engine.connect() as con:
        with con.connection.cursor() as cur:
            for v in values:
                try:
                    cur.execute(query, v)
                    con.connection.commit()
                except UniqueViolation as e:
                    cur.execute("rollback")
                    con.connection.commit()
                except Exception as e:
                    cur.execute("rollback")
                    con.connection.commit()


def __gen_values(df: pd.DataFrame) -> List[Tuple[str]]:
    """
    return array of tuples for the df values
    """
    return [tuple([str(xx) for xx in x]) for x in df.to_records(index=False)]


def __gen_cols(df) -> List[str]:
    """
    return column names
    """
    return list(df.columns)


def transform_rename_df_columns(df) -> None:
    df.rename(columns={"date": "ts", "barCount": "bar_count"}, inplace=True)


def clean_query(query: str) -> str:
    return query.replace("\n", "").replace("\t", "")


def data_already_fetched(
    engine, local_symbol: str, value_type: str, __date: date
) -> bool:
    date_str: str = __date.strftime("%Y-%m-%d")

    query = clean_query(
        f"""
        select count(*)
        from {Quote.__tablename__}
        where
            local_symbol = '{local_symbol}'
            and date(ts) = date('{date_str}')
            and value_type = '{value_type}'
        """
    )

    with engine.connect() as con:
        result = con.execute(query)
        counts = [x for x in result]

    count = counts[0][0]
    return count != 0
