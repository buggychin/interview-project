import os
import requests
import threading
from queue import Queue
import time

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
        
    def get_city_code(self, city_name):
        """
        Return the city code of the input city name
        """
        return self._city_code[city_name]

    def get_land_type_code(self, land_type):
        """
        Return the land_type code of the input land_type
        """
        return self._land_type_code[land_type]
    
    def get_file_url(self, year, season, city, land_type):
        """
        Return the file url with given parameters
        - year: year of the Republic Era
        - season: season of the year（1-4）
        - city: city name
        - land_type: land type
        """
        # turn city name and land type to city code and land type code
        city_code = self.get_city_code(city)
        land_type_code = self.get_land_type_code(land_type)
        return "https://plvr.land.moi.gov.tw//DownloadSeason?season={year}S{season}&fileName={city}_lvr_land_{land_type}.csv"\
                .format(year=year, season=season, city=city_code, land_type=land_type_code)

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
        
        
        self._queue.get() # pop an element from task queue
    
    def crawl_target_data(self, targets, start_year, start_season, end_year, end_season):
        """
        Given city, land_type and time interval, and download all the data between this interval
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
                            print("the queue is full")
                            time.sleep(2)
                start_season = 1 # each year starts with season 1 
        
        # wait until all the downloading tasks is finished
        while not self._queue.empty():
            time.sleep(3)
            


if __name__ == "__main__":
    opd_crawler = open_data_crawler(max_threads=20)
    start_year = 103
    start_year_season = 1
    end_year = 108
    end_year_season = 2
    targets = [("臺北市", "不動產買賣"), ("新北市", "不動產買賣"), ("高雄市", "不動產買賣"), 
                ("桃園市", "預售屋買賣"), ("臺中市", "預售屋買賣")]

    opd_crawler.crawl_target_data(targets, start_year, start_year_season, end_year, end_year_season)