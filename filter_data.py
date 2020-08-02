import re, os
from data_loader import csv_data_loader

file_path = os.path.join(os.getcwd(), "open_data")
if __name__ == '__main__':
	csv_loader = csv_data_loader(file_path=file_path)
	trans_data = csv_loader.load_data()

	# filter data
	filter_data =  trans_data[(trans_data['main use']=="住家用") & (trans_data['building state']=="住宅大樓(11層含以上有電梯)") \
	& (trans_data['total floor number']>=13)]

	# save filter result to a csv file
	filter_data.to_csv(os.path.join(file_path, "result.csv"), encoding="utf-8-sig")