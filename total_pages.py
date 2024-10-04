import os
import random
import sys
from botasaurus.browser import browser, Driver
from curl_cffi import requests
from parsel import Selector
import pandas as pd
import pymysql
from botasaurus.request import request, Request
import time

connect = pymysql.connect(
    host='172.27.131.60',
    user='root',
    password='actowiz',
    database='realestate_co_au'
)
cursor = connect.cursor()


# cookie_df = pd.read_csv('https://docs.google.com/spreadsheets/d/1URB5RNLoLB1mckdyxmUmAm5UWhF7OWJ-6D8TDUrfMuI/export?format=csv')
#
# refresh_cookie_1 = cookie_df['cookie_1'][0]
#
# cookies = refresh_cookie_1.split("; ")
# cookie_dict = {}
# for cookie in cookies:
#     key, value = cookie.split("=", 1)
#     cookie_dict[key] = value
#
#
# cookie = cookie_dict

# headers = {
#     'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
#     'accept-language': 'en-GB,en;q=0.9,en-US;q=0.8',
#     'cache-control': 'max-age=0',
#     'cookie': 's_fid=2EE0CC790BBD9B74-367BC0BADC047408; s_vi=[CS]v1|337B4382E393CD1C-4000064336E9E624[CE]; reauid=1566cd1736ab34000587f6660003000077de0e00; split_audience=e; topid=REAUID:1566CD1736AB34000587F6660003000077DE0E00; s_ecid=MCMID%7C19450401578401317251706578806867811487; VT_LANG=language%3Den-GB; _gcl_au=1.1.1695956652.1727432483; _fbp=fb.2.1727432484253.971561089295928314; _ga_F962Q8PWJ0=GS1.1.1727432482.1.0.1727432725.0.0.0; s_cc=true; _sp_ses.2fe7=*; Country=IN; KP_UIDz-ssn=02C4FZeDSgKN4jIsqDzfBO1OOtBpEnhECcZDgny1UE9ghZnEhrsM8y05oox814eqWvQhSbsVezRKTPlrB0dRVSMQos4pYzmAxXPT4aiPSyWGnYwhdIdlIuHOVnoCWPIA2eNQjpRs7kTDwoH6reduE1yeeJo2o0G7u6G5iKlqP7; KP_UIDz=02C4FZeDSgKN4jIsqDzfBO1OOtBpEnhECcZDgny1UE9ghZnEhrsM8y05oox814eqWvQhSbsVezRKTPlrB0dRVSMQos4pYzmAxXPT4aiPSyWGnYwhdIdlIuHOVnoCWPIA2eNQjpRs7kTDwoH6reduE1yeeJo2o0G7u6G5iKlqP7; KFC=yVr1G2AfXSDxtqpo6rUrRY2VdNghQvPJmtHdfcycZ4M=; pageview_counter.srs=1; s_nr30=1727862864135-Repeat; AMCVS_341225BE55BBF7E17F000101%40AdobeOrg=1; s_sq=%5B%5BB%5D%5D; AMCV_341225BE55BBF7E17F000101%40AdobeOrg=179643557%7CMCAID%7C337B4382E393CD1C-4000064336E9E624%7CMCIDTS%7C19999%7CMCMID%7C19450401578401317251706578806867811487%7CMCOPTOUT-1727870065s%7CNONE%7CMCAAMLH-1728467665%7C12%7CMCAAMB-1728467665%7Cj8Odv6LonN4r3an7LhD3WZrU1bUpAkFkkiY1ncBR96t2PTI%7CMCSYNCSOP%7C411-20006%7CvVersion%7C5.5.0; DM_SitId1464=1; DM_SitId1464SecId12708=1; _ga_3J0XCBB972=GS1.1.1727862868.2.0.1727862868.0.0.0; myid5_id=ID5*4Pc1fVPwt6IMsa-t-wTkZgosgS_27X2qDhemhQWj7jzKjDxn5waXxyW-cCnjm3GC; _lr_geo_location_state=GJ; _lr_geo_location=IN; _ga=GA1.3.2052508381.1727432483; _gid=GA1.3.366969648.1727862869; _gat_gtag_UA_143679184_2=1; nol_fpid=ts1npqzrdljd8jwnppufui1zmjhhi1727432484|1727432484680|1727862868671|1727862868938; utag_main=v_id:019232ff59690003617434a993780507d002807500978$_sn:2$_se:6$_ss:0$_st:1727864670600$vapi_domain:realestate.com.au$dc_visit:2$ses_id:1727862857335%3Bexp-session$_pn:2%3Bexp-session$_prevpage:rea%3Abuy%3Asearch%20result%20-%20list%3Bexp-1727866470614$dc_event:1%3Bexp-session$dc_region:ap-southeast-2%3Bexp-session; _sp_id.2fe7=98d1b8e2-f730-44ac-846b-ef69674e13d6.1727432452.2.1727862871.1727432624.f9ab8ab2-15a0-4af9-9c5c-5bba04b04da4; QSI_HistorySession=https%3A%2F%2Fwww.realestate.com.au%2Fbuy%2Fin-Sydney%2C%2520NSW%25202000%2Flist-1~1727862871447',
#     'priority': 'u=0, i',
#     'referer': 'https://www.realestate.com.au/buy/in-Sydney,%20NSW%202000/list-1',
#     'sec-ch-ua': '"Microsoft Edge";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
#     'sec-ch-ua-mobile': '?0',
#     'sec-ch-ua-platform': '"Windows"',
#     'sec-fetch-dest': 'document',
#     'sec-fetch-mode': 'navigate',
#     'sec-fetch-site': 'same-origin',
#     'sec-fetch-user': '?1',
#     'upgrade-insecure-requests': '1',
#     'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0',
# }
#
# sq = """SELECT id, url FROM realestate_au_get_links WHERE status='Pending'"""
# cursor.execute(sq)
# rows = cursor.fetchall()
# print(len(rows))
#
# for row in rows:
#     id = row[0]
#     url = row[1]
#     response = requests.get(
#         url=url,
#         headers=headers,
#         cookies=cookie,
#         impersonate='edge99'
#     )
#     if response.status_code == 200:
#         selector = Selector(response.text)
#         page_count = selector.xpath('//nav[@aria-label="Pagination Navigation"]//a[last()]/text()').get()
#
#         print(response.url, page_count)
#         uq = f"""UPDATE realestate_au_get_links SET page_count='{page_count}', status='Done' WHERE id={id}"""
#         cursor.execute(uq)
#         connect.commit()
#     else:
#         print(response.status_code, 'Error Refresh cookie')


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
    selector = Selector(response.text)
    page_count = selector.xpath('//nav[@aria-label="Pagination Navigation"]//a[last()]/text()').get()

    print(response.url, page_count)
    uq = f"""UPDATE realestate_au_get_links SET page_count='{page_count}', status='Done' WHERE id={id}"""
    cursor.execute(uq)
    connect.commit()

if __name__ == '__main__':
    os.system('python -m close_chrome')
    sq = """SELECT id, url FROM realestate_au_get_links WHERE status='Pending'"""
    cursor.execute(sq)
    rows = cursor.fetchall()
    for row in rows:
        id, url = row
        scrape_data(url)
