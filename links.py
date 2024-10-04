import hashlib
import os
import random
import sys
import time
import shutil
import lxml.html
from botasaurus.browser import Wait
from botasaurus.browser import browser, Driver
import pymysql
import gzip

from botasaurus.utils import NotFoundException

import db_config

profiles = ['nirmal', 'jay', 'joy', 'tom', 'jems', 'mahesh', 'suresh', 'ketan', 'karan', 'dip', 'deep']


@browser(
    profile=f"{random.choice(profiles)} {random.randint(0, 100000)}",
    reuse_driver=True,
    block_images_and_css=True,
    output=None
)
def scrape_data(driver: Driver, url):
    referers = [
        "https://www.google.com/",
        "https://www.bing.com/",
        "https://search.yahoo.com/",
        "https://duckduckgo.com/"
    ]
    print("URL", url)

    if driver.config.is_new:
        driver.get_via(
            url,
            bypass_cloudflare=True,
            referer=random.choice(referers)
        )
    time.sleep(1.13)
    response = driver.requests.get(url)
    print('Response: ', response.status_code)

    if response.status_code == 429:
        print("Script restarting..")
        time.sleep(3)
        os.execv(sys.executable, ['python'] + sys.argv)

    response.raise_for_status()

    html_content = lxml.html.fromstring(response.text)
    links = html_content.xpath('//a[@class="details-link residential-card__details-link"]/@href')
    links = [("https://www.realestate.com.au" + link) for link in links]
    cur.executemany('insert into property_links (link) values (%s)', links)
    conn.commit()


if __name__ == '__main__':
    os.system('python -m close_chrome')
    folder_path = r'C:\Nirmal\My Projects\realestate_au_page_save\profiles'
    try:
        shutil.rmtree(folder_path)
    except OSError as e:
        print(f"Error deleting folder {folder_path}: {e.strerror}")

    conn = pymysql.connect(host=db_config.db_locahost, user=db_config.db_user, password=db_config.db_password,
                           database=db_config.db_local_name)
    cur = conn.cursor()
    cur.execute('SELECT index_id, link from property_page_links where status="pending"')
    links = cur.fetchall()
    for link in links:
        index_id, url = link
        scrape_data(url)
        cur.execute('update property_page_links set status="DONE" where index_id=%s', (index_id))
        conn.commit()
