from selenium import webdriver
from bs4 import BeautifulSoup
import redis
import cherrypy
from cherrypy.process.plugins import Monitor
import os, os.path
import time, json

class nifty50(object):

    def __init__(self):
        self.objRedis = redis.StrictRedis(host='192.168.0.103', port=6379, db=0)
        self.browser = webdriver.PhantomJS()
        Monitor(cherrypy.engine, self.scrape_data, frequency=300).subscribe()

    @cherrypy.expose
    def index(self):
        return open('index.html')

    @cherrypy.expose
    def gainerdata(self):
        gainerslist = list()
        keys = self.objRedis.keys("*gainer*")
        for key in keys:
            gainerslist.append(self.objRedis.hgetall(key))
        return json.dumps(gainerslist)

    @cherrypy.expose
    def loserdata(self):
        loserslist = list()
        keys = self.objRedis.keys("*loser*")
        for key in keys:
            loserslist.append(self.objRedis.hgetall(key))
        return json.dumps(loserslist)

    def store_in_redis(self, key, json):
        self.objRedis.hmset(key, json)

    def scrape_table_data(self, table_element, REDIS_KEY):
        isGetThead = True
        for row in table_element.find_all('tr'):
            if (isGetThead):
                theaders = [th._attr_value_as_string('title') for th in row.find_all('th')]
                isGetThead = False
                continue
            data = [str(td.text) for td in row.find_all('td')]
            json = dict(zip(theaders, data))
            self.store_in_redis(REDIS_KEY + ":" + json['Symbol'], json)

    def scrape_data(self):
        self.browser.get('https://www.nseindia.com/live_market/dynaContent/live_analysis/top_gainers_losers.htm?cat=G')
        self.browser.find_element_by_id('tab7').click()
        soup = BeautifulSoup(self.browser.page_source)
        table_topGainers = soup.find('table', attrs={'id':'topGainers'})
        self.browser.find_element_by_id('tab8').click()
        time.sleep(5)
        soup2 = BeautifulSoup(self.browser.page_source)
        table_topLosers = soup2.find('table', attrs={'id':'topLosers'})
        self.scrape_table_data(table_topGainers, "gainer")
        self.scrape_table_data(table_topLosers, "loser")



if __name__ == '__main__':
    conf = {
        '/': {
            'tools.sessions.on': True,
            'tools.staticdir.root': os.path.abspath(os.getcwd())
        },
        '/static': {
            'tools.staticdir.on': True,
            'tools.staticdir.dir': './public'
        }
    }
    cherrypy.quickstart(nifty50(), '/', conf)



