from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from time import sleep
from bs4 import BeautifulSoup
from selenium import webdriver
import pandas as pd
import calendar
import pandas as pd
from bs4 import BeautifulSoup
from urllib import request
from selenium.webdriver.common.by import By
import datetime

def init():
    """
    data_url : 流量のデータを取得したいとき使用するurl
               "http://www1.river.go.jp/cgi-bin/DspWaterData.exe?KIND=5&ID={0}&BGNDATE={1}&ENDDATE={2}&KAWABOU=NO"
               
               水位を取得したいときに使用するurl
               "http://www1.river.go.jp/cgi-bin/DspWaterData.exe?KIND=1&ID={0}&BGNDATE={1}&ENDDATE={2}&KAWABOU=NO"
               
               雨量を取得したいときに使用するurl
               "http://www1.river.go.jp/cgi-bin/DspRainData.exe?KIND=1&ID={0}&BGNDATE={1}&ENDDATE={2}&KAWABOU=NO"
               
    data_id : 観測所記号を入れてください(例:常願寺の場合 304071284408010)
    
    start_year : データ取得開始年
    
    end_year :  データ取得終了年
    
    result_filename : スクレイピングしたデータをまとめて，出力するエクセルファイルのパス
    """
    
    data_url = "http://www1.river.go.jp/cgi-bin/DspWaterData.exe?KIND=5&ID={0}&BGNDATE={1}&ENDDATE={2}&KAWABOU=NO"
    data_id = 301061281105040
    start_year = 2010
    end_year = 2021
    result_filename = r"C:\Users\yamamoto\Desktop\program\水門水質データ\output_tomikawa_mukawa_2.xlsx"
    return data_url, data_id, start_year, end_year,result_filename

def MakeYearList(start_year, end_year):
    """スクレイピングするときに必要な年，月，日のリストを作成する
       (例：開始年[20010101], 終了年[20010131])

    Args:
        start_year (int): スクレイピング開始年
        end_year (int): スクレイピング終了年

    Returns:
       start_timelist (list): 
       end_timelist (list): 
    """
    year_list =[]
    while start_year <= end_year:
        year_list.append(start_year)
        start_year += 1
            
    end_daylist = []
    for x in year_list:
        for y in range(1, 13):
            end_day = calendar.monthrange(x, y)[1]
            end_daylist.append(end_day)

    start_timelist = []
    end_timelist = [] 
    count = 0             
    for x in year_list:
        for y in range(1,13):
            start_timelist.append("{0}{1:02d}01".format(x, y))
            end_timelist.append("{0}{1:02d}{2:02d}".format(x, y, end_daylist[count]))
            count += 1

    return start_timelist, end_timelist

def Makeurllist(url, id, start_timelist, end_timelist):
    """開始年,終了年,観測所からスクレイピングするurlを作成する

    Args:
        url (str): _description_
        id (int): _description_
        start_timelist (list): _description_
        end_timelist (list): _description_

    Returns:
        list: スクレイピングするurl
    """
    url_list = []
    for i in range(len(start_timelist)):
        url_list.append(url.format(id, start_timelist[i],end_timelist[i]))

    return url_list

def DataScraping(url):
    """水門水質DBからスクレイピングする
        取得間隔時間は5秒としている(サーバーに負荷をかけないように)

    Args:
        url (_type_): _description_

    Returns:
        _type_: _description_
    """
    options = Options()
    options.add_argument('--headless') 
            
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get(url)
    sleep(5)
            
                # 水位データを取得するためにifrmaを取得
    iframe = driver.find_element(By.TAG_NAME,'iframe')
    driver.switch_to.frame(iframe) # ここでiframeの操作に切り替える

            # iframeのソースを取得し表データの'tr'タグを全て取得する
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    table = soup.findAll("table")[0]
    rows = table.findAll("tr")
    driver.close()

    wl_data_list = []
    for row in rows:
        list_tmp = []
        td_list = row.findAll(['td'])
        for td in td_list:
            list_tmp.append(td.get_text().replace('\u3000', ''))
        wl_data_list.append(list_tmp)

    return wl_data_list

def list1dataframe(url):
    """取得したデータをまとめるときに使うカレンダーの作成

    Args:
        url (_type_): _description_

    Returns:
        _type_: _description_
    """
    defect_time = []
    year, month  =int(url[83:87]), int(url[87:89])
    end_day = calendar.monthrange(year, month)[1]
    for day in range(1,end_day+1):
        for hour in range(1,25):
            list_tmp = []
            list_tmp.append("{0}/{1:02d}/{2:02d}".format(year, month, day))
            list_tmp.append("{0:02d}:00".format(hour))
            list_tmp.append(0)
            defect_time.append(list_tmp)

    return defect_time

def list2dataframe(wl_list,url):
    """スクレイピングしたデータの加工

    Args:
        wl_list (_type_): _description_
        url (_type_): _description_

    Returns:
        _type_: _description_
    """
    try:
        df_wl = pd.DataFrame(wl_list,columns=['date', 'time', 'Dischage'])
    except:
        df_wl = pd.DataFrame(list1dataframe(url),columns=['date', 'time', 'Dischage'])

    df_wl['Dischage'] = pd.to_numeric(df_wl['Dischage'], errors='coerce')
    df_wl.set_index('date', inplace=True)
    df_wl.dropna(how='all', inplace=True)

    return df_wl

def WriteDatalist(wl,result):
    """データの出力

    Args:
        wl (_type_): _description_
        result (_type_): _description_
    """
    wl.to_excel(result, sheet_name='dischage')  

def main():
    url, id, start_year, end_year, file_name =init() 
    dataframe1 = pd.DataFrame()
    
    start_timelist, end_timelist = MakeYearList(start_year, end_year)
    urls = Makeurllist(url, id, start_timelist, end_timelist)
    
    for count,url in enumerate(urls):
        wl = DataScraping(url)
        dataframe2 =list2dataframe(wl, url)
        dataframe1 = pd.concat([dataframe1, dataframe2])

        print(count+1, "page finish!")   
                
    WriteDatalist(dataframe1, file_name)
    print("all finish!!")  

if __name__ == "__main__":
    main()


    
    
