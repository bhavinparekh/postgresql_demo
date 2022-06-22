from sqlalchemy import Column, String, Float
from sqlalchemy import ForeignKey, Integer
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, relationship

from config import config

params = config()
db = create_engine(
    f"postgresql+psycopg2://{params['user']}:{params['password']}@{params['host']}/{params['database']}",
    isolation_level="SERIALIZABLE",
)
base = declarative_base()


class Supplier(base):
    __tablename__ = 'supplier'

    uid = Column(String, nullable=False, primary_key=True)
    Text = Column(String)


class Material(base):
    __tablename__ = 'material'

    uid = Column(String, nullable=False, primary_key=True)
    Text = Column(String)


class Plant(base):
    __tablename__ = 'plant'

    uid = Column(String, nullable=False, primary_key=True)
    Text = Column(String)
    region = Column(String)


class ExternalPurchase(base):
    __tablename__ = 'external_purchase'
    pk = Column(Integer, nullable=False, primary_key=True, autoincrement=True)
    materialId = Column(String, ForeignKey('material.uid'), primary_key=True, nullable=False)
    plantId = Column(String, ForeignKey('plant.uid'), primary_key=True, nullable=False)
    supplierId = Column(String, ForeignKey('supplier.uid'), primary_key=True, nullable=False)
    unitOfMeasure = Column(String, nullable=False)
    quantityInUnitOfMeasure = Column(Float, nullable=False)

    material = relationship('Material')
    plant = relationship('Plant')
    supplier = relationship('Supplier')


if __name__ == "__main__":
    with Session(db) as session:
        session.begin()
        try:
            base.metadata.create_all(db)
        except:
            session.rollback()
            raise
        else:
            session.commit()
