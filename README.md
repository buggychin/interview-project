# 國泰二階段試題—解答

## 執行步驟
1. cd to the repository folder
2. Run follwing command to download the csv files
```sh=bash
$python data_crawler.py
```
3. Run follwing command to create the result.csv
```sh=bash
$python filter_data.py
```

### 題目與檔案對照
![](https://i.imgur.com/KFqUVDj.png)

| 題號 | 程式碼檔案          |
| ---- |:------------------- |
| Q1   | data_crawler.py     |
| Q2   | flask_api.py        |
| Q3   | data_loader.py      |
| Q4   | filter_data.py      |
| Q5   | filter_data.py      |
| Q6   | airflow_workflow.py |

<hr>

# 詳細解答
## 1. 爬蟲
### 1. 使用Chrome開發者工具，尋找資料下載使用的API網址。
![](https://i.imgur.com/vznBmI4.png)

在網站點擊下載按鈕後，Javscript分別會送出HEAD和GET兩個請求，之後將全部的檔案壓縮成zip檔案再下載下來，結果如下圖。
![](https://i.imgur.com/0CpeIO9.png)

### 2. 直接使用GET request，搭配參數設定可以直接下載我們指定的CSV檔案，結果如下圖
![](https://i.imgur.com/xQ03mRu.png)

### 3. API參數
API url：
https://plvr.land.moi.gov.tw//DownloadSeason?*season*=**106S2**&*fileName*=**C_lvr_land_A.csv**

|  | 格式 |
| -------- | -------- |
| season     | <year>S<season>  |
| fileName     | <city_code>\_lvr_land_<land_type_code>.csv |

#### City Code
| city | city_code | city | city_code |
| -------- | -------- | -------- | -------- | -------- |
| 基隆市 | C | 雲林縣 | P |
| 臺北市 | A | 嘉義市 | I |
| 新北市 | F | 嘉義縣 | Q |
| 桃園市 | H | 臺南市 | D |
| 新竹市 | O | 高雄市 | E |
| 新竹縣 | J | 屏東縣 | T |
| 苗栗縣 | K | 宜蘭縣 | G |
| 臺中市 | B | 花蓮縣 | U |
| 南投縣 | M | 臺東縣 | V |
| 彰化縣 | N | 澎湖縣 | X |
| 金門縣 | W | 連江縣 | Z |

#### Land Type Code
| land_type | land_type_code |
| -------- | -------- |
| 不動產買賣 | A |
| 預售屋買賣 | B |
| 不動產租賃 | C |

### 4. 爬蟲程式撰寫
#### 定義爬蟲物件 - open_data_crawler
```python
class open_data_crawler():
    def __init__(self, max_threads=10, storage_path=""):
        # dictonary for turning city name to city code
        self._city_code = {
            "基隆市": "C", "雲林縣": "P", "臺北市": "A", "嘉義市": "I", "新北市": "F", "嘉義縣": "Q",
            "桃園市": "H", "臺南市": "D", "新竹市": "O", "高雄市": "E", "新竹縣": "J", "屏東縣": "T",
            "苗栗縣": "K", "宜蘭縣": "G", "臺中市": "B", "花蓮縣": "U", "南投縣": "M", "臺東縣": "V",
            "彰化縣": "N", "澎湖縣": "X", "金門縣": "W", "連江縣": "Z"
        }
        
        # dictonary for turning land type to land type code
        self._land_type_code = {
            "不動產買賣": "A", "預售屋買賣": "B", "不動產租賃": "C"
        }
        
        # specifiy the path to store files
        if not storage_path:
            self._storage_path = os.path.join(os.getcwd(), "open_data")
        else:
            self._storage_path = storage_path
        
        self._max_threads = max_threads # maximum number of threads allowed
        self._queue = Queue()
```
**參數：**
***maxthreads:*** 最多同時有幾個thread進行爬蟲，默認為10
***storage_path:*** 檔案儲存路徑，默認為當前目錄下的open_data文件夾

#### 下載csv檔案
```python
def download_csv(self, url, folder, filename):
    """
    Download csv file to the folder with the given filename
    - url: file url
    - folder: file will be saved in this folder, in order to classify files
    - filename: name of the file
    """
    # create folder if the folder not exist
    path = os.path.join(self._storage_path, folder)
    if not os.path.exists(path): 
        os.makedirs(path)

    # request for the file
    while True:
        try:
            response = requests.get(url, timeout=20)
            break
        except:
            print("failed to download file:", filename)
            print("retry in 3 seconds")
            time.sleep(3)

    # save the response as a csv file
    file_path= os.path.join(path, filename)
    with open(file_path, 'wb') as f:
        f.write(response.content)
    print(folder, filename, "done")
```

#### 使用多線程進行爬蟲
由於檔案很多，並且大小都不大，使用單線程的爬蟲速度太慢，因此我們使用python中的threading package來進行多線程的爬蟲
```python
def crawl_target_data(self, targets, start_year, start_season, end_year, end_season):
        """
        Given cities, land_types and time interval, and download all the data between this interval
        - targets： list of tuple, each element is a pair of (city, land_type)
        - start_year: start year of the Republic Era
        - start_season: start season in the start year
        - end_year: end year of the Republic Era
        - end_season: end season in the end year
        """
        for city, land_type in targets:
            city_code = self.get_city_code(city)
            land_type_code = self.get_land_type_code(land_type)
            
            season = 4 # each year ends with season 4
            for y in range(start_year, end_year+1):
                if y==end_year: season=end_season # if y==end_year then season equals to end_season given
                
                # loop throught each year
                for s in range(start_season, season+1):
                    url = self.get_file_url(y, s, city, land_type)
                    folder = "%s_%s"%(y,s) # use year and season to classify files
                    filename = "%s_lvr_land_%s.csv"%(city_code, land_type_code)
                    
                    # if the thread number exceed the limit, then wait until other thread is finished
                    while True:
                        if self._queue.qsize()<=self._max_threads:
                            t = threading.Thread(target=self.download_csv, args=(url, folder, filename, ))
                            t.start()
                            self._queue.put(t) 
                            break
                        else:
#                             print("the queue is full")
                            time.sleep(2)
                start_season = 1 # each year starts with season 1 
        
        # wait until all the downloading tasks is finished
        while not self._queue.empty():
            time.sleep(3)
        print("爬蟲結束！")
```

#### 爬取所有資料
```python
%%time
opd_crawler = open_data_crawler(max_threads=20)
start_year = 103
start_year_season = 1
end_year = 108
end_year_season = 2
targets = [("臺北市", "不動產買賣"), ("新北市", "不動產買賣"), ("高雄市", "不動產買賣"), 
            ("桃園市", "預售屋買賣"), ("臺中市", "預售屋買賣")]

opd_crawler.crawl_target_data(targets, start_year, start_year_season, end_year, end_year_season)
```


## 3. 讀取檔案
### 1. 讀取全部檔案
比較使用 append 和 concat 兩種方法來合併csv檔，結果顯示pd.concat的讀取方式速度快非常多

#### 使用df.append來合併dataframe
```python
%%time
trans_data = pd.DataFrame()
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
        trans_data=trans_data.append(temp)
```
```
Wall time: 2min 7s
```

#### 使用pd.concat來合併dataframe
```python
%%time
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
trans_data = pd.concat(li)
```
```
Wall time: 5.02 s
```

### 2. 資料清理
total floor number欄位中的資料格式不一致，有的是透過阿拉伯數字來記載（如：5, 10等...），有的則透過中文字來記載（如：二十五層，十一層等...）
透過下方的函式，我們可以將中文字轉成數字。
```python
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
```

另外，也針對na欄位進行資料填補，規則如下：


| 欄位類型        | 欄位名稱                                                                                                                                                                                                                                                                                                                                                                                 | 處理方法            |
| --------------- |:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |:------------------- |
| 類別            | The villages and towns urban district、 Whether there is manages the organization、 building present situation pattern – compartmented、 building state、 df_name、main building materials、main use、non-metropolis land use、shifting level、the berth category、the non-metropolis land use district、the use zoning or compiles and checks、transaction pen number、transaction sign | 以 '未知' 填入nan   |
| 字串            | land sector position building sector house number plate、serial number、the note                                                                                                                                                                                                                                                                                                         | 以空字元 '' 填入nan |
| 數值(默認為0)   | Building present situation pattern – room、 building present situation pattern – hall、 building present situation pattern – health                                                                                                                                                                                                                                                      | 以 0 填入nan        |
| 數值(默認不為0) | berth shifting total area square meter、 building shifting total area、land shifting total area square meter、the berth total price NTD、the unit price (NTD / square meter)、total floor number、total price NTD                                                                                                                                                                        | 不做處理，保留nan   |
|                 |                                                                                                                                                                                                                                                                                                                                                                                          |                     |
| 日期            | construction to complete the years、transaction year month and day                                                                                                                                                                                                                                                                                                                       | 不做處理，保留nan   |
|                 |                                                                                                                                                                                                                                                                                                                                                                                          |                     |

實作程式碼如下
```python
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
```

## 4. 資料篩選
篩選條件：
-【main use】為【住家用】
-【building state】為【住宅大樓】
-【total floor number】需【大於等於十三層】
```python
filter_data = trans_data[(trans_data['main use']=="住家用") & \
    (trans_data['building state']=="住宅大樓(11層含以上有電梯)") & \
    (trans_data['total floor number']>=13)]
filter_data
```

## 5. 儲存結果成csv格式
```python
filter_data.to_csv("result.csv", encoding="utf-8-sig")
```

## 6.部署到Airflow
將程式碼稍作整理為data_crawler.py以及data_loader.py兩個物件
接著建立airflow_workflow.py檔案
```python
$ cat airflow_workflow.py

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
	opd_crawler = open_data_crawler(max_threads=100, storage_path=file_path)
	start_year, start_season = 103, 1
	end_year, end_season = 108, 2
	target_data = [("臺北市", "不動產買賣"), ("新北市", "不動產買賣"), ("高雄市", "不動產買賣"), 
				("桃園市", "預售屋買賣"), ("臺中市", "預售屋買賣")]

	for city, land_type in target_data:
		opd_crawler.crawl_target_data(city, land_type, start_year, start_season, end_year, end_season)

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
dag = DAG('opendata_crawler', description='crawler open data and process', \
schedule_interval='0 12 * * *', start_date=datetime(2020, 3, 20), catchup=False)

# define crawler operator
crawler_operator = PythonOperator(task_id='crawl_data_task', \
    python_callable=crawl_data, dag=dag)
# define data process operator
process_data_operator = PythonOperator(task_id='process_data_task', \
python_callable=process_data, dag=dag)

# define the sequence
crawler_operator>>process_data_operator

```

## 2. 設計/建立API
### 設計/建立API
```python
@app.route('/api/transactions', methods=['GET'])
def transactions():
    if request.method == 'GET':
        city = request.args.get('city', default=None, type=str)
        village = request.args.get('village', default=None, type=str)
        total_floor = request.args.get('total_floor', default=None, type=int)
        building_state = request.args.get('building_state', default=None, type=str)

        limit = request.args.get('limit', default=20, type=int)
        offset = request.args.get('offset', default=0, type=int)
```
設計過程：
1. 在restful的風格下，api的URI代表著資源的位置（名詞），並且透過http的method來區分對資源的動作（GET、POST、DELETE...）
2. 這次的情境下，我們需要依照不同條件（參數）去查詢（動作）交易資料（資源）
3. 因此該API的規格如下：
**URI:** /api/transactions
**HTTP METHOD:** GET
**參數:**
    - **village：** 鄉鎮市區名稱（string）
    - **total_floor：** 總樓層數（int）
    - **building_state：** 建物形態 （string）
    - **limit：** 每次回傳多少筆資料，default 20，maximum 100（int）
    - **offset：** 跳過前面幾筆資料，default 20（int）

** 由於我們有60幾萬筆資料，如果沒有輸入任何條件的話，回傳的資料量會過於龐大。因此透過**limit和offset**來限制回傳資料的數量，避免一個request傳輸時間過長造成伺服器阻塞。

### 資料schema
1. 篩選重要欄位，並對欄位進行重新命名
```json
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
	"total floor number": "total_floor_number",
	"transaction pen number": "transaction_pen_number",
	"transaction sign": "transaction_sign",
	"transaction year month and day": "transaction_date",
	"total price NTD": "total_price",
	"the unit price (NTD / square meter)": "unit_price",
	"the note": "note",
    "df_name": "df_name"
}
trans_data = trans_data.rename(columns=rename_columns)
trans_data = trans_data[list(rename_columns.values())]
```
2. 布林欄位使用True、False來表示
```python
trans_data["compartmented"] = np.where(trans_data['compartmented']=="有", True, False)
trans_data["has_manage_org"] = np.where(trans_data['has_manage_org']=="有", True, False)
```

3. 回傳的的資料schema
```json
[
   {
      "compartmented":true,
      "number_of_rooms":3,
      "number_of_hall":2,
      "number_of_bathrooms":2,
      "building_area":109.87,
      "building_state":"住宅大樓(11層含以上有電梯)",
      "shifting_level":"十層",
      "has_manage_org":true,
      "building_materials":"鋼筋混凝土造",
      "main_use":"住家用",
      "construction_date":710824,
      "address":"臺北市中正區羅斯福路三段121~150號",
      "village":"中正區",
      "land_area":9.97,
      "berth_area":0,
      "berth_type":"未知",
      "berth_price":0,
      "use_zoning":"商",
      "total_floor_number":12,
      "transaction_pen_number":"土地2建物1車位0",
      "transaction_sign":"房地(土地+建物)",
      "transaction_date":1021007,
      "total_price":15600000,
      "unit_price":141986,
      "note":"親友、員工或其他特殊關係間之交易。",
      "df_name":"103_1_A_A"
   },
 ...
]
```