"""
A simple Douban top250 movies parser
"""
# coding:utf-8
import multiprocessing as mp
import requests
import pandas as pd
from bs4 import BeautifulSoup
import logging
import sys
import os
import time
def find_movieinfo(html_text):
    movies = []
    for item in html_text:
        index = item.find('em', {'class': ""})
        web_link = item.find('a')['href']
        poster = item.find('img')['src'] if item.find('img') else item.find('img')
        title = item.find('span', {'class': "title"})
        director = item.find('p',{'class':""})
        rate=item.find('span',{'class':"rating_num"})
        quote = item.find('p',{'class':"quote"})
        tmp_dict=dict()
        if index and title:
            tmp_dict["Index"]=index.get_text(strip=True) if index else index
            tmp_dict["Title"] = title.get_text(strip=True) if title else title
            tmp_dict["Poster"] = (tmp_dict["Title"],poster)
            tmp_dict["Link"] = web_link
            tmp_dict["Director"] = director.get_text(strip=True) if director else director
            tmp_dict["Rate"] = rate.get_text(strip=True) if rate else rate
            tmp_dict["Quote"] = quote.get_text(strip=True) if quote else quote
            movies.append(tmp_dict)
    return movies
def url_worker(url,all_data):
    print(time.strftime('%Y-%m-%d %H:%M:%S'),"process {} is on".format(os.getpid()))
    time.sleep(2)
    reconnect=0
    while reconnect<3:
        try:
            request = requests.get(url, timeout=(5,20))
            soup = BeautifulSoup(request.text, "lxml")
            all = soup.find_all('li')
            movies = find_movieinfo(all)
            all_data+=movies
            print("successful")
            return
        except requests.exceptions.RequestException as e:
            reconnect+=1
            time.sleep(reconnect)
            print(time.strftime('%Y-%m-%d %H:%M:%S'), e, "Reconnecting {}".format(reconnect))
def img_download_worker(url,full_path):
    response = requests.get(url)
    try:
        with open(full_path, "wb") as infile:
            infile.write(response.content)
        print("Downloaded: {}".format(url))
    except:
        print("Can't write")

def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    url = "https://movie.douban.com/top250"
    url_list=[ url + "?start={}&filter=".format(count) for count in range(0,250,25)]
    ##multiprocess from url parse data
    mp.freeze_support()# Windows 平台要加上这句，避免 RuntimeError
    manager=mp.Manager()
    all_data=manager.list()
    pool=mp.Pool(mp.cpu_count())
    for index,url in enumerate(url_list):
        #proc=mp.Process(target=url_worker,args=(url,all_data,))
        pool.apply_async(url_worker,args=(url,all_data,))
    pool.close()
    pool.join()

    img_path_dict=dict()
    movie_dataFrame = pd.DataFrame(data=list(all_data))
    path=os.getcwd()+"\image"
    if not os.path.exists(path):
        os.makedirs(path)
    url_list=movie_dataFrame["Poster"]
    ##multiprocess for pictrue downloading
    procs = []
    pool = mp.Pool(mp.cpu_count())
    for title, url in url_list:
        full_path = os.path.abspath(os.path.join(path, title + ".jpg"))
        img_path_dict[title] = full_path
        if os.path.exists(full_path):
            continue
        pool.apply_async(img_download_worker, args=(url, full_path,))
    pool.close()
    pool.join()
    column_titles=["Title","Director","Rate","Quote","Link"]
    movie_dataFrame.set_index("Index",inplace=True)
    movie_dataFrame.index=pd.to_numeric(movie_dataFrame.index, errors='coerce')
    movie_dataFrame.sort_index()
    movie_dataFrame=movie_dataFrame.reindex(columns=column_titles)
    xlsxwriter=pd.ExcelWriter("./movie.xlsx",engine="xlsxwriter")
    movie_dataFrame.to_excel(xlsxwriter, sheet_name="movies")
    worksheet = xlsxwriter.sheets['movies']
    for index,column in enumerate(movie_dataFrame.columns):
        column_width=max(max([len(value) if value else 0 for value in movie_dataFrame[column]]),len(column))+2
        worksheet.set_column(index+1,index+1,column_width)
    for index,row in enumerate(movie_dataFrame["Title"]):
        worksheet.write_url(index+1,1,img_path_dict[row],string=row)
    xlsxwriter.save()



if __name__ == '__main__':
    main()



