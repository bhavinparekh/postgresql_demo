from sqlalchemy import func, desc, over

from connect import Session, db, ExternalPurchase, Material


def get_bar_chart(plantId=None):
    with Session(db) as session:
        session.begin()
        try:
            sum_of_materials = session.query(
                Material.Text,
                ExternalPurchase.materialId,
                func.sum(ExternalPurchase.quantityInUnitOfMeasure).label("quantityInKg"),
                over(func.sum(ExternalPurchase.quantityInUnitOfMeasure) * 100 /
                     func.sum(func.sum(ExternalPurchase.quantityInUnitOfMeasure))).label(
                    "allPurchaseQuantity"),
                func.sum(
                    func.sum(ExternalPurchase.quantityInUnitOfMeasure)
                ).over().label("allPurchaseQuantity")
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
                else ExternalPurchase.plantId is not None
            ).limit(5).all()
            other_quantityInKg = int(sum_of_materials[1][4]) - sum([int(i[2]) for i in sum_of_materials])
            other_percentage = round(100 - sum([i[3] for i in sum_of_materials]), 2)

            response = {
                "top5materials": [
                    {
                        "materialId": m[1],
                        "materialName": m[0],
                        "quantityInKg": int(m[2]),
                        "percentage": round(m[3], 2)  # a value between 0 and 100
                    } for m in sum_of_materials
                ],
                "others": {
                    "quantityInKg": other_quantityInKg,
                    "percentage": other_percentage  # a value between 0 and 100
                }
            }
        except:
            session.rollback()
            raise
        else:
            session.commit()
    return response


if __name__ == '__main__':
    print(get_bar_chart())
