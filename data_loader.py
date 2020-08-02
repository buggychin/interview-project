import pandas as pd
import numpy as np
import re, os

class csv_data_loader():
	def __init__(self, file_path=""):
		if file_path=="":
			self._file_path = os.path.join(os.getcwd(), "open_data")
		else:
			self._file_path = file_path
		
	def read_all_csv(self):
		"""
		read all the csv files in the path into a single dataframe
		"""
		li = []
		for path, folder, filenames in os.walk(self._file_path):
			for file in filenames:
				if not file.endswith(".csv") or "_lvr_land_" not in file: continue # continue if file is not csv file
				file_path = os.path.join(path, file)
				temp = pd.read_csv(file_path, encoding="utf-8", skiprows=1)
	
				# add df_name column to dataframe
				yead_and_season = os.path.basename(path)
				city_and_type = file.replace("_lvr_land", "").replace(".csv", "")
				label = yead_and_season+"_"+city_and_type
				temp["df_name"] = label

				li.append(temp)
		return pd.concat(li)

	def chinese_to_number(self, pd_columns):
		"""
		Loop through pd_columns, and turn all the chinese number string to number.
		"""
		chinese_num = {"一":1, "二":2, "三":3, "四":4, "五":5, "六":6, "七":7, "八":8, "九":9, "十":10}
		col_values = []
		reg_query = "^[%s]+層" % "|".join(list(chinese_num.keys())) # reg_query: "^[一|二|三|四|五|六|七|八|九|十]+層""
		for floor_info in pd_columns:
			# if floor_info is nan
			if floor_info is np.nan:
				floor = floor_info
			else:
				floor_info = str(floor_info)
				# if floor_info is in chinese word format, then turn the value to number format
				if re.match(reg_query, floor_info):
					tmp=1
					floor=0
					for c in floor_info:
						if c in chinese_num:
							val = chinese_num[c]
							if val<10:
								tmp = val
							else:
								floor+=tmp*val
								tmp = 0
					else:
						floor+=tmp
				# if floor_info equals to "地下層", then set the floor to -1
				elif floor_info=="地下層":
					floor = -1
				# if the floor is already a number
				else:
					floor = int(floor_info)
			col_values.append(floor)
		return col_values


	def data_cleaning(self, trans_data):
		"""
		Organize and format data
		"""
		# define the value to fill nan according to different column
		category_col = ["The villages and towns urban district", "Whether there is manages the organization", "building present situation pattern - compartmented", "building state", "df_name", "main building materials", "main use", "non-metropolis land use", "shifting level", "the berth category", "the non-metropolis land use district", "the use zoning or compiles and checks", "transaction pen number", "transaction sign"]
		string_col = ["land sector position building sector house number plate", "serial number", "the note"]
		num_col_default_0 = ["Building present situation pattern - room", " building present situation pattern - hall", " building present situation pattern - health"]
		values = dict([(col, "未知") for col in category_col])
		values = {**values, **dict([(col, "") for col in string_col])}
		values = {**values, **dict([(col, 0) for col in num_col_default_0])}

		# turn `total floor nuumber` into numeric format
		trans_data["total floor number"] = self.chinese_to_number(trans_data["total floor number"])
		# fill nan
		return trans_data.fillna(values)

	def load_data(self):
		trans_data = self.read_all_csv()
		return self.data_cleaning(trans_data)

if __name__ == "__main__":
	file_path = os.path.join(os.getcwd(), "open_data")
	csv_loader = csv_data_loader(file_path=file_path)
	trans_data = csv_loader.load_data()
