import pandas as pd


class Preprocess:
    def __init__(self, **paths):
        self.external_purchase = pd.read_csv(paths['external_purchase'])
        self.material = pd.read_csv(paths['material'])
        self.plant = pd.read_csv(paths['plant'])
        self.supplier = pd.read_csv(paths['supplier'])

    def get_external_purchase(self):
        replace_SId_dict = dict(zip(self.external_purchase.materialId, self.external_purchase.supplierId))
        self.external_purchase.supplierId = self.external_purchase.materialId.apply(
            lambda x: str(int(replace_SId_dict[x])))
        self.external_purchase.materialId = self.external_purchase.materialId.astype(object)
        self.external_purchase.plantId = self.external_purchase.plantId.astype(object)
        self.external_purchase.unitOfMeasure = self.external_purchase.unitOfMeasure.astype(object)
        self.external_purchase.quantityInUnitOfMeasure = self.external_purchase.quantityInUnitOfMeasure.apply(
            lambda x: abs(float(x.replace('KG', ''))) if not isinstance(x, float) else abs(x))
        return self.external_purchase

    def get_material(self):
        self.material.uid = self.material.uid.astype(object)
        return self.material

    def get_plant(self):
        return self.plant

    def get_supplier(self):
        self.supplier.uid = self.supplier.uid.astype(object)
        return self.supplier

# paths = {"external_purchase": 'data/external_purchase.csv',
#          'material': 'data/material.csv',
#          'plant': 'data/plant.csv',
#          'supplier': 'data/supplier.csv'}
# d = DataClean(**paths)
# print(d.get_external_purchase())
