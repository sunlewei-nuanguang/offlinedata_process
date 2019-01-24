#coding:utf-8
#author:sunlewei
'''
拆自dbutile，和爬虫相关的db操作
'''

import pymongo
import redis
import hashlib
import logging
import pickle
import time,json,re,copy,os,sys
import numpy as np
from pymongo import MongoClient, DESCENDING
from conf.publib_conf import CONN_ADDR_AWS, USERNAME_AWS, PWD_AWS
from conf.publib_conf import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
reload(sys)
sys.setdefaultencoding('utf-8')


class DbSpider:
    '''
        爬虫数据库
    '''
    log_cache = {}
    depth_life = 60*2
    log_length = 10 # 日志达到10条写磁盘 

    def __init__(self):
        self.client = MongoClient("172.31.140.84:27777")
        self.client2 = MongoClient("172.31.140.205:27777")
        self.client_trade = MongoClient("172.31.140.86:28888")
        if USERNAME_AWS is not None:
            self.client.admin.authenticate(USERNAME_AWS, PWD_AWS)
            self.client2.admin.authenticate(USERNAME_AWS, PWD_AWS)
            self.client_trade.admin.authenticate(USERNAME_AWS, PWD_AWS)
        #redis 
        #print REDIS_HOST,REDIS_PORT,REDIS_PASSWORD
        self.depth_redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, password=REDIS_PASSWORD)


    def __del__(self):
        try:
            self.client.close()
        except:
            pass

    def close(self):
        '''
        供显式调用释放数据等连接资源
        '''
        try:
            self.client.close()
            self.client2.close()
            self.client_trade.close()
        except Exception,e:
            logging.warning("显式调用释放数据等连接资源异常:%s" % e)
            pass 
    def get_mongo_data(self,db,table,where={}):
        return self.client[db][table].find(where)
      
    def get_data_md5(self,item):
        data = {k: v for k, v in item.items() if k not in ['_id', 'time','md5']}
        if data == {}:
            return None
        return hashlib.md5(json.dumps(data)).hexdigest()
 
    def get_redis_data(self,key):
       try:
           data = self.depth_redis.get(key)
           data = pickle.loads(data) 
           return data
       except:
           return None
    
    def check_redis_coin_time(self,market,symbol,iswhile=False):
       '''
       '''
       key = "depth_%s_%s" % (market,symbol)
       while True:
           qs_time = time.time()*1000
           k_d = self.get_redis_data(key)
           if k_d is None:
              print k_d,'========='
              continue
           time_delay = float(time.time()*1000-k_d.get("time",0))/1000
           print key,"延迟：",time_delay," 查询时间：",time.time()*1000-qs_time,k_d["md5"],self.get_data_md5(k_d)
           time.sleep(0.5)
           if iswhile is False:
               break
       
       
    def check_redis_time(self,market):
       '''检测单个交易所数据延迟情况
       '''
       pairs = self.get_online_market_coins()
       online_list = pairs.get(market,[])
       print online_list
       keys = self.depth_redis.keys("*h_%s_*" % market)  
       print market,"总量：",len(keys),"线上：", len(online_list)
       for key in keys:
           coin = key.split("_")[2]
           quote = key.split("_")[3]
           if coin+"_"+quote not in online_list:
              #continue
              pass
           qs_time = time.time()*1000
           k_d = self.get_redis_data(key) 
           if k_d is None:
              print k_d,'========='
              continue
           time_delay = float(time.time()*1000-k_d.get("time",0))/1000
           #if time_delay > 1000*1 or time_delay < 0:
           print key,"延迟：",time_delay," 查询时间：",time.time()*1000-qs_time
    
    def get_market_top(self,market,pairs,is_circle=False):
       '''获取交易所前两条数据
       '''
       key = "depth_%s_%s" % (market,pairs)
       i = 1
       while True:
           k_d = self.get_redis_data(key)
           if k_d is None:
              print k_d,'=======None'
           else:
              print i
              print k_d["time"]
              print "bids:",k_d["bids"][:10]
              print "asks:",k_d["asks"][:10]
           if is_circle is False:
              break
           time.sleep(1)

    def get_online_market_coins(self):
        """
        获取线上所有的交易对
        """
        path_info = self.client["fundinfo"]["hedge_conf"].find({})
        pairs = {}
        for o_p in path_info:
           if "coins" not in o_p:
              continue
           pma = o_p.get("pma")
           coin_list = pairs.get(pma,[])
           coin_list.extend([coin+"_"+re.split("\d+",o_p['quote_a'])[-1] for coin in o_p["coins"]])
           pairs[pma] = list(set(coin_list))
           pmb = o_p.get("pmb")
           coin_list = pairs.get(pmb,[])
           coin_list.extend([coin+"_"+re.split("\d+",o_p['quote_b'])[-1] for coin in o_p["coins"]])
           pairs[pmb] = list(set(coin_list))
        return pairs
    
    def get_faild_pairs(self,markets=None):
        market_coins = self.client_trade["market_info"]["market_coins"].find_one({})
        http_mk = market_coins.get("http_market")
        mk_pri_coin = market_coins.get("mk_pri_coins")
        mk_coin = market_coins.get("mk_coins")
        pairs = {mk:mk_pri_coin[mk] if mk in http_mk else mk_coin[mk] for mk in market_coins.get("all_market")}
        if markets is None:
           markets = market_coins["all_market"]
        for market in markets:
           keys = self.depth_redis.keys("*h_%s_*" % market) 
           c_coins = [k.split("_")[2]+"_"+k.split("_")[3] for k in keys]
           coins = pairs.get(market,[])
           no_coins = list(  set(coins)-set(c_coins) )
           print market,",",len(coins),",",len(keys),","," ".join(no_coins)
                 
    def get_fist_huilv(self,coin):
        #cur = self.client2['finance_info']['huilv_coin'].find_one({"coin":coin.upper()},sort=[("time",1)])
        where = {"coin":coin.upper(),"time":{"$gte":int((time.time()-60*60*24)*1000)} }
        cur = self.client2['finance_info']['huilv_coin'].find_one( where )
        return cur

    def get_data_md5(self,item):
        data = {k: v for k, v in item.items() if k not in ['_id', 'time','md5']}
        if data == {}:
            return None
        return hashlib.md5(json.dumps(data)).hexdigest()

    def get_item_price(self,item):
        return item["asks"][0].get("price"),item["bids"][0].get("price")
        
    def check_depth_data(self,market):
        """
        对比汇率，检查深度数据
        买卖深度的中间价和汇率价的误差大于20%
        """
        keys = self.depth_redis.keys("*h_%s_*" % market)
        redis_data = {}  
        print "总量：",len(keys)
        for key in keys:
            k_d = self.get_redis_data(key) 
            redis_data[key] = k_d
        for k,v in redis_data.items():
            coin = k.split("_")[2]
            quote = k.split("_")[3]
            one = self.get_fist_huilv(coin)
            if one is None:
               print "coin",coin,"无汇率数据！"
               continue
            coin_usd = one["usd"]
            standard_price = -1
            if quote.upper() in one:
                standard_price = one[quote.upper()]
            else:
                one = self.get_fist_huilv(quote)
                if one is None:
                   print "qupte",quote,"无汇率数据！"
                   continue
                quote_usd = one["usd"]
                standard_price = float(coin_usd/quote_usd)
            bids,asks = self.get_item_price(v)
            if float((float(bids+asks)/2 - standard_price)/standard_price)>0.2:
                print k,"买卖中间价",str(float(bids+asks)/2),"对比标准价：",str(standard_price)
    
    def check_depth_file_data(self,file_path):
       """查看文件数据"""
       file_obj = open(file_path,"r+")
       values = []
       for line in file_obj:
          line = line.strip()
          item = json.loads(line)
          if self.get_data_md5(item) != item["md5"]:
             print self.get_data_md5(item) , item["md5"]
 
    def check_api_exchange(self,market):
        keys = self.depth_redis.keys("*h_%s_*" % market)
        redis_data = {}
        print "总量：",len(keys)
        for key in keys:
            k_d = self.get_redis_data(key)
            redis_data[key] = k_d
        for k,v in redis_data.items():
            if v is None:
              continue
            if v["md5"] != self.get_data_md5(v):
               print k,v["md5"],self.get_data_md5(v)
            else:
               print "same",k,v["md5"],self.get_data_md5(v)


if __name__ == "__main__":
    db=DbSpider() 
    mk_list = ["buda","coinx","rightbtc","indodax","btcturk","dex","bitmarket","gaex","zgtop","hashwang","bw","bishang","koinex","hht","bitalong","bittrex","yoe","bitso","yobit","bitkonan","cex","btcdo"]
    http_mk = ["cryptopia", "uex", "korbit", "coinbene", "dsx", "liqui", "cpdax", "gopax", "coinw", "kucoin", "cointobe", "coinyee", "bibox", "bige", "bitforex", "bcex", "big", "coinegg", "fubt", "bleutrade", "upbit", "bitasiabit", "coinmex", "qryptos", "allcoin", "btctrade", "kraken", "bitbay", "coinnest", "acx", "itbit"]
    testsd=["upbit"]
    #for mk in mk_list:
    db.get_faild_pairs(testsd)
    #b.check_redis_time("yobit")
        #db.check_redis_coin_time("huobi","bch_usdt",True)
        #db.check_depth_data(mk)
    #db.get_online_market_coins()
    #db.get_market_top("yobit","req_inr",True)
    #db.check_api_exchange("huobiaus")
    #db.check_depth_file_data("/data1/depth_log/2018101111/bitfinex/hot_eth.log")
