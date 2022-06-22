from sqlalchemy import select

from connect import Material, Session, db, Plant, Supplier, ExternalPurchase
from preprocess import Preprocess


class Insertion:
    def __init__(self, **paths):
        self.preprocess = Preprocess(**paths)

    def insert_materials(self):
        with Session(db) as session:
            session.begin()
            try:
                materials = self.preprocess.get_material()
                for _, row in materials.iterrows():
                    stmt = select(Material).where(Material.uid == str(row['uid']))
                    result = session.execute(stmt)
                    if result.fetchone() is None:
                        mt = Material(uid=row['uid'], Text=row['text'])
                        session.add(mt)
            except:
                session.rollback()
                raise
            else:
                session.commit()

    def insert_plants(self):
        with Session(db) as session:
            session.begin()
            try:
                plants = self.preprocess.get_plant()
                for _, row in plants.iterrows():
                    stmt = select(Plant).where(Plant.uid == str(row['uid']))
                    result = session.execute(stmt)

                    if result.fetchone() is None:
                        pt = Plant(uid=row['uid'], Text=row['text'], region=row['region'])
                        session.add(pt)
            except:
                session.rollback()
                raise
            else:
                session.commit()

    def insert_suppliers(self):
        with Session(db) as session:
            session.begin()
            try:
                suppliers = self.preprocess.get_supplier()
                for _, row in suppliers.iterrows():
                    stmt = select(Supplier).where(Supplier.uid == str(row['uid']))
                    result = session.execute(stmt)

                    if result.fetchone() is None:
                        sp = Supplier(uid=row['uid'], Text=row['text'])
                        session.add(sp)
            except:
                session.rollback()
                raise
            else:
                session.commit()

    def insert_external_purchases(self):
        with Session(db) as session:
            session.begin()
            try:
                ExternalPurchases = self.preprocess.get_external_purchase()
                for _, row in ExternalPurchases.iterrows():
                    ep = ExternalPurchase(materialId=row['materialId'], plantId=row['plantId'],
                                          supplierId=row['supplierId'],
                                          unitOfMeasure=row['unitOfMeasure'],
                                          quantityInUnitOfMeasure=row['quantityInUnitOfMeasure'])
                    session.add(ep)
            except:
                session.rollback()
                raise
            else:
                session.commit()

    def run(self):
        self.insert_materials()
        self.insert_plants()
        self.insert_suppliers()
        self.insert_external_purchases()
        return "Done"


if __name__ == "__main__":
    paths = {"external_purchase": 'data/external_purchase.csv',
             'material': 'data/material.csv',
             'plant': 'data/plant.csv',
             'supplier': 'data/supplier.csv'}
    insert = Insertion(**paths)
    insert.run()
