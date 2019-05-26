"""
A simple Douban top250 movies parser
"""
# coding:utf-8
import requests
import pandas as pd
from bs4 import BeautifulSoup
import logging
import sys
import os
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
####download images into local directory
def download_img_from_url(url_list,path=os.curdir+r"\image"):
    if os.path.exists(path)==False:
        os.makedirs(path)
    logging.info("\t Total images:{}".format(len(url_list)))
    total_list=len(url_list)
    count=1
    img_path_dict={}
    if len(url_list)>0:
        for title,url in url_list:
            full_path = os.path.join(path, title + ".jpg")
            print(full_path)
            if os.path.exists(full_path):
                img_path_dict[title] = full_path
                continue
            response=requests.get(url)
            try:
                with open(full_path, "wb",encoding='utf-8') as infile:
                    infile.write(response.content)
                logging.info("\tTotal:{}/{} image downloaded:{}".format(count,total_list,full_path))
                img_path_dict[title]=full_path
                count+=1
            except:
                logging.error("Failed to download:{} {}".format(title,url))
    return img_path_dict
####insert image url into excel
def insert_img_to_excel(url_list,worksheet,df):
    from io import BytesIO
    # Import urlopen() for either Python 2 or 3.
    try:
        from urllib.request import urlopen
    except ImportError:
        from urllib3 import urlopen
    # Create the workbook and add a worksheet.
    row_index = df["Title"].tolist()
    print(row_index)
    for title,url in url_list:
        image_data = BytesIO(urlopen(url).read())
        # Write the byte stream image to a cell. Note, the filename must be
        # specified. In this case it will be read from url string.
        worksheet.insert_image(row_index.index(title)+1,1, url, {'image_data': image_data})
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,stream=sys.stdout)
    url = "https://movie.douban.com/top250"
    count = 0
    movie_list=[]
    while True:
        next_page = url + "?start={}&filter=".format(count)
        try:
            web = requests.get(next_page,timeout=10)
            web.raise_for_status()
        except:
            msg="Wrong url:{}".format(next_page)
            logging.warning(msg)
            continue
        count += 25
        soup = BeautifulSoup(web.text, "lxml")
        all = soup.find_all('li')
        tmp_list = find_movieinfo(all)
        if len(tmp_list) == 0:
            break
        movie_list += tmp_list
        logging.info("\tTotal: {} movies found".format(count))
    logging.info("\tcomplete")
    movie_dataFrame=pd.DataFrame(movie_list)

    ###save images
    img_path_dict=download_img_from_url(movie_dataFrame["Poster"])
    #movie_dataFrame["Title"]=movie_dataFrame["Title"].apply(lambda x: r'external:{}'.format(img_path_dict[x]))
    try:
        column_titles=["Title","Director","Rate","Quote","Link"]
        movie_dataFrame.set_index("Index",inplace=True)
        movie_dataFrame=movie_dataFrame.reindex(columns=column_titles)
    except:
        movie_dataFrame.to_excel("./movie.xlsx")
    xlsxwriter=pd.ExcelWriter("./movie.xlsx",engine="xlsxwriter")
    movie_dataFrame.to_excel(xlsxwriter, sheet_name="movies")
    worksheet = xlsxwriter.sheets['movies']
    #insert_img_to_excel(url_list=movie_dataFrame["Poster"], worksheet=worksheet,df=movie_dataFrame)
    for index,column in enumerate(movie_dataFrame.columns):
        column_width=max(max([len(value) if value else 0 for value in movie_dataFrame[column]]),len(column))+2
        worksheet.set_column(index+1,index+1,column_width)
    for index,row in enumerate(movie_dataFrame["Title"]):
        worksheet.write_url(index+1,1,img_path_dict[row],string=row)
    xlsxwriter.save()



