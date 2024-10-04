import hashlib
import os
import random
import sys
import time

import lxml.html
from botasaurus.browser import Wait
from botasaurus.browser import browser, Driver
import pymysql
import gzip
from botasaurus.utils import NotFoundException
import db_config
import json
from botasaurus.request import request, Request

# Generate random profile
profiles = ['nirmal', 'person', 'profile', 'tom', 'jems']


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
    print("Property Page URL: ", url)
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
    html_content = response.text
    hash_id = hashlib.sha256(url.encode()).hexdigest()

    html_data = lxml.html.fromstring(html_content)
    raw_data = html_data.xpath('//script[contains(text(), "window.ArgonautExchange=")]/text()')
    json_data = clean_json(raw_data[0])
    # Save the HTML content to a file
    with gzip.open(fr'{page_save_path}\{hash_id}.html.gz', 'wb') as f:
        f.write(json.dumps(json_data).encode('utf-8'))
    print("Property Page Saved..")

    # get lat long
    lat = None
    long = None
    address_data = json_data['details']['listing']['address']['display']
    if address_data:
        lat = address_data['geocode'].get('latitude', None)
        long = address_data['geocode'].get('longitude', None)

    data = {}
    data['lat'] = str(lat)
    data['long'] = str(long)
    data['hash_id'] = hash_id

    if lat and long:
        return data


def clean_json(raw_data):
    raw_data = raw_data.replace('window.ArgonautExchange=', '').strip(';')
    raw_data = json.loads(raw_data)['resi-property_listing-experience-web']['urqlClientCache']
    raw_data = json.loads(raw_data)
    key = list(raw_data.keys())
    return json.loads(raw_data[key[0]]['data'])


@request(
    output=None
)
def scrape_school_data(request: Request, link):
    print("School Data URL: ", link)
    time.sleep(1.13)
    res_school = request.get(link)
    print('Response: ', res_school.status_code)
    if res_school.status_code == 429:
        print("Script restarting..")
        time.sleep(3)
        os.execv(sys.executable, ['python'] + sys.argv)

    res_school.raise_for_status()
    # Save the HTML content to a file
    with gzip.open(fr'{page_save_path}\{data["hash_id"]}_school_data.html.gz',
                   'wb') as f:
        f.write(res_school.text.encode())
    print("School Data Page Saved..")


@request(
    output=None
)
def scrape_child_care_data(request: Request, link):
    # page save for child care data
    print("Child Care URL: ", link)
    time.sleep(1.13)
    res_child_care = request.get(link)
    print('Response: ', res_child_care.status_code)
    if res_child_care.status_code == 429:
        print("Script restarting..")
        time.sleep(3)
        os.execv(sys.executable, ['python'] + sys.argv)

    res_child_care.raise_for_status()
    # Save the HTML content to a file
    with gzip.open(fr'{page_save_path}\{data["hash_id"]}_child_care_data.html.gz',
                   'wb') as f:
        f.write(res_child_care.text.encode())
    print("Child Care Data Page Saved..")
    print("=" * 25)


if __name__ == '__main__':
    page_save_path = r"C:\Nirmal\page_save\realestate"
    os.system('python -m close_chrome')
    conn = pymysql.connect(host=db_config.db_locahost, user=db_config.db_user, password=db_config.db_password,
                           database=db_config.db_local_name)
    cur = conn.cursor()
    cur.execute('SELECT link from property_links where status="pending"')
    links = cur.fetchall()
    for link in links:
        url = link[0]
        data = scrape_data(url)
        child_care_url = f"https://rea.careforkids.com.au/api/realestate/closest?lat={data['lat']}&lon={data['long']}&count=5"
        school_url = f"https://school-service.realestate.com.au/closest_by_type?lat={data['lat']}&lon={data['long']}&count=5"
        if data:
            scrape_school_data(school_url)
            scrape_child_care_data(child_care_url)
        cur.execute('update property_links set status="DONE" where link= %s', (url))
        conn.commit()
