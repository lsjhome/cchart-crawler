import time
import os
import random
import json
import logging

import requests
from bs4 import BeautifulSoup
import pymysql
from urllib3 import request


class CchartCrawler(object):    


    def __init__(self, host, user, passwd, db, port, charset='utf8'):
        
        self.conn = pymysql.connect(host=host, user=user, passwd=passwd, db=db, port=port, charset=charset)
        self.curs = self.conn.cursor(pymysql.cursors.DictCursor)
        
    
    def query_input(self, query):
        
        self.query = query 


    def run(self):

        BASE_URL = 'http://cchart.xyz/ytcollect/YoutubeChannelTrackingService/more/?format=json&currentViewType=channelSubscribers'

        response = requests.get('http://cchart.xyz/')
        soup = BeautifulSoup(response.text, 'html.parser')
        category_tags = soup.select('a.btn.btn-block')
        cat_eng = [category.get('href').split('/')[-2] for category in category_tags]
        cat_kor = [category.text.strip() for category in category_tags]
        cat_dic = {eng:kor for (eng, kor) in zip(cat_eng, cat_kor)}
        cat_eng_use = cat_eng[:-3]
        params_list = [{'pk' :cat, 'itemTotal': number } for cat in cat_eng_use for number in page_list]
        
        for param in params_list:
            response = requests.get(BASE_URL, param)

            if response.status_code == 200:

                parsed_json_str = json.loads(response.text)
                parsed_json = json.loads(parsed_json_str)
                date_key = list(parsed_json.keys())[0]
                parsed_json_01 = parsed_json[date_key]
                cate_key = list(parsed_json_01.keys())[0]
                parsed_json_02 = parsed_json_01[cate_key]
                sub_key = list(parsed_json_02.keys())[0]
                parsed_json_03 = parsed_json_02[sub_key]
                rank_keys = list(parsed_json_03.keys())

                for key in rank_keys:
                    detail = parsed_json_03[key]
                    youtube_id = detail['ci']
                    c_name = detail['ct']
                    category = cat_dic[param['pk']]
                    source_url = BASE_URL + '&' + request.urlencode(param)
                    json_data = {'name':c_name, 'id':youtube_id, 'category':category, 'source_url':source_url}    
                    self.curs.execute(self.query, args=json_data)
                    self.conn.commit()

                time.sleep(random.random() + 1)

            else:
                source_url = BASE_URL + '&' + urllib3.request.urlencode(param)
                logging.error('%s %s' % (str(response.status_code), source_url))
                
        self.conn.close()
        

if __name__ == '__main__':

    host, user, passwd = os.environ['host'], os.environ['user'], os.environ['passwd']
    
    upsert = 'INSERT INTO t_channel_list (name, id, category, source_url) \
           values (%(name)s, %(id)s, %(category)s, %(source_url)s) \
           ON DUPLICATE KEY UPDATE category=%(category)s, source_url=%(source_url)s'
    
    cc = CchartCrawler(host=host, user=user, passwd=passwd, db='YOUTUBE', port=3306, charset='utf8')
    cc.query_input(upsert)
    cc.run()
