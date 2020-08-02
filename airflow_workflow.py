from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import numpy as np
import re

import os, sys
sys.path.append(os.getcwd())
from data_crawler import open_data_crawler
from data_loader import csv_data_loader

file_path = os.path.join(os.getcwd(), "open_data")

def crawl_data():
	"""
	An airflow operator task, will download target csv files.
	"""
	opd_crawler = open_data_crawler(max_threads=20, storage_path=file_path)
	start_year, start_season = 103, 1
	end_year, end_season = 108, 2
	targets = [("臺北市", "不動產買賣"), ("新北市", "不動產買賣"), ("高雄市", "不動產買賣"), 
				("桃園市", "預售屋買賣"), ("臺中市", "預售屋買賣")]
	opd_crawler.crawl_target_data(targets, start_year, start_season, end_year, end_season)

def process_data():
	"""
	An airflow operator task, will read in all csv files and output the filter result.
	"""
	# 1. read in all csv files in the path
	csv_loader = csv_data_loader(file_path=file_path)
	trans_data = csv_loader.load_data()

	# 2. filter data
	filter_data =  trans_data[(trans_data['main use']=="住家用") & (trans_data['building state']=="住宅大樓(11層含以上有電梯)") \
	& (trans_data['total floor number']>=13)]

	# 3. save filter result to a csv file
	filter_data.to_csv(os.path.join(file_path, "result.csv"), encoding="utf-8-sig")

# define a DAG
dag = DAG('opendata_crawler', description='crawl and filter open data', schedule_interval='0 12 * * *', start_date=datetime(2020, 8, 1), catchup=False)
crawler_operator = PythonOperator(task_id='crawl_data_task', python_callable=crawl_data, dag=dag)
process_data_operator = PythonOperator(task_id='process_data_task', python_callable=process_data, dag=dag)
crawler_operator>>process_data_operator
