from app import get_bar_chart
from insertion import Insertion


def test_all_good():
    paths = {"external_purchase": 'data/external_purchase.csv',
             'material': 'data/material.csv',
             'plant': 'data/plant.csv',
             'supplier': 'data/supplier.csv'}
    insert = Insertion(**paths)
    result = insert.run()
    assert result == "Done"


def test_missing_param():
    result = get_bar_chart()
    print(result)
    expacted_result = {'others': {'quantityInKg': 148464525, 'percentage': 40.02},'top5materials': [
        {'materialId': '10001', 'materialName': 'Material 1', 'quantityInKg': 129164735, 'percentage': 34.82},
        {'materialId': '10013', 'materialName': 'Material 13', 'quantityInKg': 26665750, 'percentage': 7.19},
        {'materialId': '10004', 'materialName': 'Material 4', 'quantityInKg': 22723460, 'percentage': 6.13},
        {'materialId': '10002', 'materialName': 'Material 2', 'quantityInKg': 22117300, 'percentage': 5.96},
        {'materialId': '10003', 'materialName': 'Material 3', 'quantityInKg': 21853300, 'percentage': 5.89}]}


    assert result == expacted_result
