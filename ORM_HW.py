import sqlalchemy as sq
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
import json
import os
from dotenv import load_dotenv
from prettytable import PrettyTable
load_dotenv()


Base = declarative_base()


class Publisher(Base):
    __tablename__ = "publisher"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), nullable=False)


class Book(Base):
    __tablename__ = "book"

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=40), nullable=False)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey("publisher.id"), nullable=False)

    publisher = relationship('Publisher', backref='book')


class Shop(Base):
    __tablename__ = "shop"

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), nullable=False)

class Stock(Base):
    __tablename__ = "stock"

    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey("book.id"), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey("shop.id"), nullable=False)
    count = sq.Column(sq.Integer)

    book = relationship(Book, backref='stock')
    shop = relationship(Shop, backref='stock')


class Sale(Base):
    __tablename__ = "sale"

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Numeric, nullable=False)
    date_sale = sq.Column(sq.DateTime, nullable=False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey("stock.id"), nullable=False)
    count = sq.Column(sq.Integer)

    stock = relationship(Stock, backref='sale')


def create_tables(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


db_login = os.getenv("DB_LOGIN")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

DSN = f"postgresql://{db_login}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = sq.create_engine(DSN)

create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

with open('fixtures/tests_data.json', 'r', encoding='utf-8') as f:
    tests_data = json.load(f)

for data in tests_data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[data.get('model')]
    session.add(model(id=data.get('pk'), **data.get('fields')))
session.commit()

x = PrettyTable()
x.field_names = ['Book_title','Shop_name','Sale_price','Sale_date']

if __name__ == "__main__":
    author=input("Введите id или имя автора: ")
    query = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale).join(Publisher).join(Stock).join(Shop).join(Sale)
    if author.isdigit():
        query = query.filter(Publisher.id == author)
        for record in query:
            x.add_row([record[0],record[1],f'{record[2]} $',record[3].date()])
    else:
        query = query.filter(Publisher.name.ilike(f'%{author}%'))
        for record in query:
            x.add_row([record[0], record[1], f'{record[2]} $', record[3].date()])
    print(x)