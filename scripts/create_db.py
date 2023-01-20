import csv
import logging
from datetime import datetime, date as datetime_date
from typing import Optional

from pydantic import BaseModel, Field, validator
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    DateTime,
)
from sqlalchemy.orm import Session, declarative_base

from app.config import DATABASE_URL


def engine_db():
    return create_engine(
        DATABASE_URL,
        connect_args={},
    )


def connect_db():
    engine = engine_db()
    session = Session(bind=engine.connect())
    return session


class Bars_validator(BaseModel):
    date: datetime_date = Field(alias="Date")
    symbol: Optional[str] = Field(alias="Symbol")
    adj_close: Optional[float] = Field(alias="Adj Close")
    close: Optional[float] = Field(alias="Close")
    high: Optional[float] = Field(alias="High")
    low: Optional[float] = Field(alias="Low")
    open: Optional[float] = Field(alias="Open")
    volume: Optional[float] = Field(alias="Volume")

    @validator('adj_close', 'close', 'high', 'low', 'open', 'volume')
    def empty_str_to_float(cls, v):
        if not v:
            return 0.0
        return v


Base = declarative_base()


class Bars_2(Base):
    id = Column(Integer, primary_key=True)
    date = Column(String, nullable=True)
    symbol = Column(String, nullable=True)
    adj_close = Column(String, nullable=True)
    close = Column(String, nullable=True)
    high = Column(String, nullable=True)
    low = Column(String, nullable=True)
    open = Column(String, nullable=True)
    volume = Column(String, nullable=True)
    __mapper_args__ = {"eager_defaults": True}

    def __init__(
            self,
            date,
            symbol,
            adj_close,
            close,
            high,
            low,
            open,
            volume,
    ) -> None:
        self.metadata.schema = 'public'
        self.date = date
        self.symbol = symbol
        self.adj_close = adj_close
        self.close = close
        self.high = high
        self.low = low
        self.open = open
        self.volume = volume
        self.metadata.create_all(engine_db())
        super().__init__()

    __tablename__ = 'bars_2'


class Bars_1(Base):
    id = Column(Integer, primary_key=True)
    date = Column(String, nullable=True)
    symbol = Column(String, nullable=True)
    adj_close = Column(String, nullable=True)
    close = Column(String, nullable=True)
    high = Column(String, nullable=True)
    low = Column(String, nullable=True)
    open = Column(String, nullable=True)
    volume = Column(String, nullable=True)
    __mapper_args__ = {"eager_defaults": True}

    def __init__(
        self,
        date,
        symbol,
        adj_close,
        close,
        high,
        low,
        open,
        volume,
    ) -> None:
        self.metadata.schema = 'public'
        self.date = date
        self.symbol = symbol
        self.adj_close = adj_close
        self.close = close
        self.high = high
        self.low = low
        self.open = open
        self.volume = volume
        self.metadata.create_all(engine_db())
        super().__init__()

    __tablename__ = 'bars_1'


class Error_logs(Base):
    __tablename__ = 'error_logs'
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, nullable=True)
    launch_timestamp = Column(String)
    symbol = Column(String, nullable=True)
    message = Column(String)
    __mapper_args__ = {"eager_defaults": True, }

    def __init__(
            self,
            date,
            launch_timestamp,
            symbol,
            message,
    ) -> None:
        self.date = date
        self.launch_timestamp = launch_timestamp
        self.symbol = symbol
        self.message = message
        self.metadata.schema = 'public'
        self.metadata.create_all(engine_db())
        super().__init__()


def populate_data(table, data):
    database = connect_db()
    bulk = []
    for row in data:
        bulk.append(
           table(
            date=row['date'].strftime('%Y/%m/%d'),
            symbol=row['symbol'],
            adj_close=row['adj_close'],
            close=row['close'],
            high=row['high'],
            low=row['low'],
            open=row['open'],
            volume=row['volume'],
            ),
        )

    database.bulk_save_objects(bulk)
    database.commit()


def write_errors(data):
    if data:
        database = connect_db()
        for row in data:
            database.add(Error_logs(
                    launch_timestamp=row['launch_timestamp'],
                    date=row['date'],
                    symbol=row['symbol'],
                    message=row['message'],
                )
            )
        database.commit()


def read_csv_data(table_obj, file_fp):
    with open(file_fp) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')
        data = []
        loggs_errors = []
        count_missing_docs = 0
        count_docs = 0

        for rows in csv_reader:
            try:
                data.append(Bars_validator(**rows).dict())
            except Exception as e:
                loggs_errors.append(
                    {
                        'launch_timestamp': datetime.utcnow(),
                        'date': rows.get('date', None),
                        'symbol': rows.get('symbol', None),
                        'message': f'{e}: {rows}',
                    }
                )
            if len(loggs_errors) >= 10:
                count_missing_docs += len(loggs_errors)
                write_errors(loggs_errors)
                loggs_errors = []
            if len(data) >= 100:
                populate_data(table_obj, data)
                count_docs += len(data)
                data = []
            logging.info(f'Inserted {count_missing_docs} errors docs')
            logging.info(f'Inserted {count_docs} docs')

        write_errors(loggs_errors)
        populate_data(table_obj, data)
        count_missing_docs += len(loggs_errors)
        count_docs += len(data)
        logging.info(f'Inserted {count_missing_docs} errors docs')
        logging.info(f'Inserted {count_docs} docs')


def main():
    read_csv_data(Bars_1, './db_data/bars_1.csv')
    read_csv_data(Bars_2, './db_data/bars_2.csv')


if __name__ == '__main__':
    main()
