import requests
import pandas as pd
import pymysql

con_local = pymysql.connect(
    host='localhost',
    user='root',
    password='actowiz',
    database='realestate_au',
)
cur_local = con_local.cursor()

con = pymysql.connect(
    host='172.27.131.60',
    user='root',
    password='actowiz',
    database='realestate_co_au',
)
cur = con.cursor()

cur.execute("select url, page_count from realestate_au_get_links where page_count != '80' and page_count is not null")
page_links = cur.fetchall()
print(page_links)
for page in page_links:
    url, total_page = page
    url = url.replace('list-1', '')
    total_page = total_page if total_page != 'None' else 1
    print("total pages", total_page)
    for i in range(1, int(total_page) + 1):
        link = url + 'list-' + str(i)
        cur_local.execute('insert ignore into property_page_links (link) values (%s)', (link))
        con_local.commit()