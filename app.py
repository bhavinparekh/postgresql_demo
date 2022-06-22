from sqlalchemy import func, cast, Integer, desc, over

from connect import Session, db, ExternalPurchase, Material



def get_bar_chart(plantId="ABC"):
    with Session(db) as session:
        session.begin()
        try:
            sum_of_materials = session.query(
                Material.Text,
                ExternalPurchase.materialId,
                func.sum(cast(ExternalPurchase.quantityInUnitOfMeasure, Integer)).label("quantityInKg"),
                over(func.sum(cast(ExternalPurchase.quantityInUnitOfMeasure, Integer)) * 100 /
                     func.sum(func.sum(cast(ExternalPurchase.quantityInUnitOfMeasure, Integer)))).label("allPurchaseQuantity")
            ).join(
                Material, ExternalPurchase.materialId == Material.uid
            ).group_by(
                Material.Text,
                ExternalPurchase.materialId
            ).order_by(
                desc("quantityInKg")
            ).filter(
                ExternalPurchase.plantId == plantId
                if plantId is not None
                else ExternalPurchase.plantId != None
            ).limit(5).all()

            for i in sum_of_materials:
                print(i)

        except:
            session.rollback()
            raise
        else:
            session.commit()



get_bar_chart()
