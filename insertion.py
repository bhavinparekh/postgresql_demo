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
                mts = []
                for _, row in materials.iterrows():
                    stmt = select(Material).where(Material.uid == str(row['uid']))
                    result = session.execute(stmt)
                    if result.fetchone() is None:
                        mts.append(Material(uid=row['uid'], Text=row['text']))
                session.bulk_save_objects(mts)
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
                pts = []
                for _, row in plants.iterrows():
                    stmt = select(Plant).where(Plant.uid == str(row['uid']))
                    result = session.execute(stmt)
                    if result.fetchone() is None:
                        pts.append(Plant(uid=row['uid'], Text=row['text'], region=row['region']))
                session.bulk_save_objects(pts)
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
                sps = []
                for _, row in suppliers.iterrows():
                    stmt = select(Supplier).where(Supplier.uid == str(row['uid']))
                    result = session.execute(stmt)
                    if result.fetchone() is None:
                        sps.append(Supplier(uid=row['uid'], Text=row['text']))
                session.bulk_save_objects(sps)
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
                eps = []
                for _, row in ExternalPurchases.iterrows():
                    eps.append(ExternalPurchase(materialId=row['materialId'], plantId=row['plantId'],
                                                supplierId=row['supplierId'],
                                                unitOfMeasure=row['unitOfMeasure'],
                                                quantityInUnitOfMeasure=row['quantityInUnitOfMeasure']))
                session.bulk_save_objects(eps)
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
