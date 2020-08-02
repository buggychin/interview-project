import os
import pandas as pd
import numpy as np
from flask import Flask, request, jsonify

import re, os
from data_loader import csv_data_loader

file_path = os.path.join(os.getcwd(), "open_data")
csv_loader = csv_data_loader(file_path=file_path)
trans_data = csv_loader.load_data()

app = Flask(__name__)


rename_columns = {
	"building present situation pattern - compartmented": "compartmented",
	"Building present situation pattern - room": "number_of_rooms",
	"building present situation pattern - hall": "number_of_hall",
	"building present situation pattern - health": "number_of_bathrooms",
	"building shifting total area": "building_area",
	"building state": "building_state",
	"shifting level": "shifting_level",
	"Whether there is manages the organization": "has_manage_org",
	"main building materials": "building_materials",
	"main use": "main_use",
	"construction to complete the years": "construction_date",
	"land sector position building sector house number plate": "address",
	"The villages and towns urban district": "village",
	"land shifting total area square meter": "land_area",
	"berth shifting total area square meter": "berth_area",
	"the berth category": "berth_type",
	"the berth total price NTD": "berth_price",
	"the use zoning or compiles and checks": "use_zoning",
	"total floor number": "floor_number",
	"transaction pen number": "transaction_pen_number",
	"transaction sign": "transaction_sign",
	"transaction year month and day": "transaction_date",
	"total price NTD": "total_price",
	"the unit price (NTD / square meter)": "unit_price",
	"the note": "note"
}
trans_data = trans_data.rename(columns=rename_columns)
trans_data = trans_data[list(rename_columns.values())]

trans_data["compartmented"] = np.where(trans_data['compartmented']=="有", True, False)
trans_data["has_manage_org"] = np.where(trans_data['has_manage_org']=="有", True, False)

@app.route('/api/transactions', methods=['GET'])
def transactions():
    if request.method == 'GET':
        city = request.args.get('city', default=None, type=str)
        village = request.args.get('village', default=None, type=str)
        total_floor = request.args.get('total_floor', default=None, type=int)
        building_state = request.args.get('building_state', default=None, type=str)

        limit = request.args.get('limit', default=20, type=int)
        offset = request.args.get('offset', default=0, type=int)

        limit = min(100, limit)

        filter_data = trans_data
        if city is not None:
            filter_data = filter_data[filter_data['address'].str.match("^"+city)==True]
        if village is not None:
            filter_data = filter_data[filter_data['village']==village]
        if total_floor is not None:
            filter_data = filter_data[filter_data['floor_number']==total_floor]
        if building_state is not None:
            filter_data = filter_data[filter_data['building_state'].str.contains(building_state)]

        filter_data = filter_data.loc[offset:offset+limit]
        print(len(filter_data))
        return jsonify(filter_data.to_json(orient="records"))

if __name__ == '__main__':
    app.run(port=5002, debug=True)