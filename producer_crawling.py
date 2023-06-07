
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import time
import os
import re
import openpyxl
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from datetime import datetime
import requests
from io import BytesIO
from PIL import Image
import json
import re
from confluent_kafka import Producer, KafkaError
import ntplib
from datetime import datetime
bootstrap_servers = '3.35.96.120:9092'

def value_serializer(value):
    return str(value).encode('utf-8')
ensure_ascii=False

def key_serializer(key):
    return str(key).encode('utf-8')


# 프로듀서 설정
config = {
    'bootstrap.servers': bootstrap_servers,
    'batch.num.messages': 18,  
}

topic_name = 'instar_ph.kafka'
producer = Producer(config)


# step2.아이디, 비밀번호 설정
id = "hwang99131"
pw = "ad45217"
#태그 추가가능
hashtags = ['강남맛집']

#01. 웹 열기
driver = webdriver.Chrome(service= Service(ChromeDriverManager().install()))

caps = DesiredCapabilities().CHROME
caps["pageLoadStrategy"] = "none"
def login(id, pw):
    driver.set_window_size(1200, 800) 	#브라우저 크기 1000*800으로 고정
    driver.get('https://www.instagram.com/') #인스타그램 웹 켜기
    time.sleep(15) 

    driver.find_element(By.NAME, 'username').send_keys(id)
    # 비밀번호 입력
    driver.find_element(By.NAME, 'password').send_keys(pw)
    # 엔터
    driver.find_element(By.XPATH,'//*[@id="loginForm"]/div/div[3]').click()
    time.sleep(25)
    

def search(word):
    url = "https://www.instagram.com/explore/tags/" + word
    driver.get(url) #tag 검색
    time.sleep(20)

def select_first(driver):
    driver.find_element(By.CLASS_NAME,'_aagw').click()
    time.sleep(17)



def get_content(driver):
   
    html = driver.page_source
    soup = BeautifulSoup(html,'lxml')
  
    try:
        writer = soup.select('div.xt0psk2')[0].text
        
  
    except:
        writer = "작성자 정보 없음"

    #contents
    try:

        content = soup.select('div._a9zs')[0].text
    except:
        content = '내용 없음'

    #hashtags
    try:
        tags = re.findall(r'#[^\s#,\\]+', content)

    except:
        tags = ''
    
    #date
    date = soup.select('time._a9ze._a9zf')[0]['datetime'][:10]

    #likes 
    like = soup.select('section._ae5m._ae5n._ae5o')[0].text[4:-1]
    
    if '여러 명' in like:
        like = '0'

    #place
    try: 
        place = soup.select('div._aaqm')[0].text

    except:
        place = ''

    data = [writer, content, date, like, place, tags]

    return data

def move_next(driver):
    driver.find_element(By.CLASS_NAME,'_aaqg._aaqh').click()
    time.sleep(13)

# NTP 서버에 연결하여 시간 동기화


login(id,pw)
count =0

for word in hashtags:
    search(word)
    select_first(driver)
    keys = ['writer', 'content', 'date', 'like', 'place','tag']
    # target = 20000
    target = 2000
   

   
    for i in range(target):
        try:
           
            client = ntplib.NTPClient()
            response = client.request('pool.ntp.org')
            ntp_time = datetime.fromtimestamp(response.tx_time)
            formatted_date = ntp_time.strftime("%m/%d/%Y")

            data = get_content(driver)
            content15 = data[1][0:11]
            item_dict = dict(zip(keys,data))
            json_result = json.dumps(item_dict,ensure_ascii=False)
            output_dict = {word: json_result}
            output_json = json.dumps(output_dict,ensure_ascii=False)
            
             # #프로듀서부분
            key1= key_serializer(content15+formatted_date) #비교를 위한 키
            msg1 = value_serializer(output_json)

            try:
                producer.produce(topic_name, value=msg1, key=key1)
                count = count+1
            except KafkaError as ke:
                print('Failed to send message:', ke)
            print("게시글수=",count)
            print(key1)
            print(output_json)
            #이거
            move_next(driver)
        except:
            time.sleep(3) 
            move_next(driver)
        time.sleep(5)
        producer.flush()

producer.flush()
producer.poll(0)

