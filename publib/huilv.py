#coding:utf-8
#author:shiyuming 
#modify:hudongdong hanwanhe guoxiangchao
'''
拆自dbutile，和汇率相关的操作。汇率数据取自数据库
'''

import pymongo
import redis
import hashlib
import logging
import cPickle as pickle
import time,json,re,copy,os,sys
from pymongo import MongoClient, DESCENDING
from conf.publib_conf import CONN_ADDR_AWS, USERNAME_AWS, PWD_AWS, OTC_TYPE
from conf.publib_conf import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

class DbHuilv:
    '''
        汇率数据库
    '''
    def __init__(self):
        self.client = MongoClient(CONN_ADDR_AWS)
        if USERNAME_AWS is not None:
            self.client.admin.authenticate(USERNAME_AWS, PWD_AWS)

        self.huilvdb = self.client.get_database("huobipro")
        self.huilv_finance = self.client.get_database("finance_info")
        self.moveresultdb = self.client.get_database("moveresult")
        self.interval = 0.5 * 3600 * 1000
        self.huilv_dict = {
            "usdt" : {"huilv":0., "last_update_time":0.}
        }
        #redis 
        self.redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, password=REDIS_PASSWORD)


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
        except Exception,e:
            logging.warning("显式调用释放数据等连接资源异常:%s" % e)
            pass 

    def get_huilv(self, quote, is_go = None, end_time = None, lock = OTC_TYPE):
        params_str = json.dumps({
                            'quote' : quote,
                            'code' : 'usd',
                            'is_go' : is_go,
                            'end_time' : int(end_time/(1000*60)) if end_time is not None else None,
                            'lock' : lock,
                            'otc' : True
                        })
        m = hashlib.md5()
        m.update(params_str)
        cache_key = 'huilv_'+m.hexdigest()
        cache_content = self.redis.get(cache_key)
        if(cache_content):
            logging.debug(u"查询汇率 "+quote+" hit redis cache")
            return pickle.loads(str(cache_content))
        else:
            ret = self.get_huilv_orign(quote, is_go, end_time, lock)
            if ret:
                self.redis.set(cache_key, pickle.dumps(ret), ex=60)
            return ret



    def get_huilv_orign(self, quote, is_go = None, end_time = None, lock=False):
        '''
        查询获得指定计价单位的汇率，如果是法币，则统一为cny人民币；如果是btc eth，则不需要，直接返回1.0
        '''
        #离线跑的过程下

        if quote in ['btc', 'eth', 'usd']:
            logging.debug(u"查询汇率 quote[%s] 无需转换"% quote)
            return 1.0
        elif quote in ["usdt", 'usdt_buy', 'usdt_sell']:
            if lock == True:
                end_time = time.time() * 1000
                start_time = end_time - self.interval
                where = {"time" : {"$gt" : start_time, "$lt":end_time}, "code":'USD'}
                usd_cny = self.huilv_finance['huilv_item'].find_one(where, sort = [("time", pymongo.DESCENDING)])
                if usd_cny != None:
                    if is_go:
                        usdt_buy = self.get_usdtcny_huilv_buy(end_time = end_time)
                        logging.debug(u"查询汇率 usdt买入价，供usdt专程法币 quote[usdt] usdt_buy[%s] " % (usdt_buy))
                        return 1/(usdt_buy * usd_cny['refePrice'])
                    else:
                        usdt_sell = self.get_usdtcny_huilv_sell(end_time = end_time)
                        logging.debug(u"查询汇率 usdt卖出价，供法币转出usdt用 quote[usdt] usdt_sell[%s] " % (usdt_sell))
                        return 1/(usdt_sell * usd_cny['refePrice'])
                return None
            else:
                return 1.0
            
        elif quote in ['krw',"gbp","eur","usd","rub","idr","sgd","jpy","cny","pln","uah","cad","aud","brl","php","hkd","thb","zar","sek","nzd","nok","mxn","ils","huf","dkk","cnh","chf","inr","rur","try"]:
            end_time = time.time() * 1000
            start_time = end_time - self.interval
            where  = {"time" : {"$gt" : start_time, "$lt":end_time}, "code":quote.upper(), "quote" : "USD"}
            ret = None

            if quote in ['jpy',]:
                ret = self.huilv_finance['huilv_item_new_tws_fx'].find_one(where, sort = [("time", pymongo.DESCENDING)])
            else:
                ret = self.huilv_finance['huilv_item_new'].find_one(where, sort = [("time", pymongo.DESCENDING)])
            if ret is None:
                logging.warning(u"查询汇率 quote[%s] 失败 where:%s" % (quote, where))
                return None
            else:
                self.huilv_dict[quote] = { "huilve":ret['refePrice'], 'last_update_time':time.time() }
                logging.debug(u"查询汇率 quote[%s] ret:[%s]" % (quote, ret['refePrice']))
                return ret['refePrice']
        else:
            logging.warning(u"未熟虑的法币单位[%s]" % quote)
            return None


    def get_usdtcny_huilv_buy(self,  end_time = None):
        params_str = json.dumps({
                            'end_time' : int(end_time/(1000*60)) if end_time is not None else None,
                        })
        m = hashlib.md5()
        m.update(params_str)
        cache_key = 'usdtcny_huilv_buy_'+m.hexdigest()
        cache_content = self.redis.get(cache_key)
        if(cache_content):
            logging.debug(u"买入usdt价:hit redis cache")
            return pickle.loads(str(cache_content))
        else:
            ret = self.get_usdtcny_huilv_buy_orign(end_time)
            if ret:
                self.redis.set(cache_key, pickle.dumps(ret), ex=60)
            return ret

    def get_usdtcny_huilv_buy_orign(self, end_time = None):
        '''
            买入usdt的汇率，这个汇率是当需要将usdt转移成其他外币时使用
        '''
        min_bigtrade = 5
        min_normaltrade = 5
        huilv_cny_usdt_buy = 0.0
        if end_time is None:
            end_time = time.time() * 1000
        start_time = end_time - self.interval
        ret = self.huilvdb['normalTrade_usdt_buy'].find_one({"time" : {"$gt" : start_time, "$lt":end_time}}, sort = [("time", pymongo.DESCENDING)])
        if ret is None:
            ret = self.huilvdb['normalTrade_usdt_buy_okex'].find_one({"time" : {"$gt" : start_time, "$lt":end_time}}, sort = [("time", pymongo.DESCENDING)])
        if ret is not None and len(ret['record']) >= min_normaltrade:
            for i in range(5):
                huilv_cny_usdt_buy += ret['record'][i]['price']
            huilv_cny_usdt_buy = round(huilv_cny_usdt_buy / 5, 3)
            logging.debug(u"买入usdt价: 普通交易区有挂单量[%s]个，符合要求 huilv_cny_usdt_buy:[%s] 前3的价格详情:%s" % (len(ret['record']), huilv_cny_usdt_buy, ret['record'][:3]))
        else:
            logging.debug(u"买入usdt价: 普通交易区有挂单量 不符合要求   价格详情:%s" % (ret))
            ret = self.huilvdb['bigTrade_usdt_buy'].find_one({"time" : {"$gt" : start_time, "$lt":end_time}}, 
                                                        sort = [("time", pymongo.DESCENDING)])
            if ret is not None and len(ret['record']) >= min_bigtrade:
                for i in range(5):
                    huilv_cny_usdt_buy += ret['record'][i]['price']
                huilv_cny_usdt_buy = round(huilv_cny_usdt_buy / 5, 3)
                logging.debug(u"买入usdt价: 大宗交易区符合挂单量[%s]要求 huilv_cny_usdt_buy:[%s] 价格详情:%s" % (len(ret['record']), huilv_cny_usdt_buy, ret['record'][:5]))
            else:
                logging.debug(u"买入usdt价: 大宗交易区不符合挂单量  价格详情:%s" % (ret))

        logging.info(u"获取汇率。 huilv_cny_usdt_buy：%s  start_time[%s] end_time[%s] " % (huilv_cny_usdt_buy, start_time, end_time))
        #汇率异常判断 必须是在6.30 -- 6.49之间 否则短信报警
        if huilv_cny_usdt_buy is None or huilv_cny_usdt_buy > 7.3 or huilv_cny_usdt_buy < 5.9:
            msg = "huilv_cny_usdt_buy[%s] 异常！ 超出阀值范围[5.9 -- 7]" % huilv_cny_usdt_buy
            logging.warning(msg)
            # notice(msg)
            huilv_cny_usdt_buy = None
        return huilv_cny_usdt_buy



    def get_usdtcny_huilv_sell(self,  end_time = None):
        params_str = json.dumps({
                            'end_time' : int(end_time/(1000*60)) if end_time is not None else None,
                        })
        m = hashlib.md5()
        m.update(params_str)
        cache_key = 'usdtcny_huilv_sell_'+m.hexdigest()
        cache_content = self.redis.get(cache_key)
        if(cache_content):
            logging.debug("卖出usdt价:hit redis cache")
            return pickle.loads(str(cache_content))
        else:
            ret = self.get_usdtcny_huilv_sell_orign(end_time)
            if ret:
                self.redis.set(cache_key, pickle.dumps(ret), ex=60)
            return ret

    def get_usdtcny_huilv_sell_orign(self, end_time = None):
        '''
            获取到usdt汇率的卖出价。这个汇率在将其他法币转成usdt时使用
        '''
        min_bigtrade = 5
        min_normaltrade = 5
        huilv_cny_usdt_sell = 0.
        if end_time is None:
            end_time = time.time() * 1000
        start_time = end_time - self.interval
        ret = self.huilvdb['normalTrade_usdt_sell'].find_one({"time" : {"$gt" : start_time, "$lt":end_time}}, sort = [("time", pymongo.DESCENDING)])
        if ret is None:
            ret = self.huilvdb['normalTrade_usdt_sell_okex'].find_one({"time" : {"$gt" : start_time, "$lt":end_time}}, sort = [("time", pymongo.DESCENDING)])
        if ret is not None and len(ret['record']) >= min_normaltrade:
            for i in range(5):
                huilv_cny_usdt_sell += ret['record'][i]['price']
            huilv_cny_usdt_sell = round(huilv_cny_usdt_sell / 5, 3)
            logging.debug(u"卖出usdt价: 普通交易区有挂单量[%s]个，符合要求 huilv_cny_usdt_sell:[%s] 前3的价格详情:%s" % (len(ret['record']), huilv_cny_usdt_sell, ret['record'][:3]))
        else:
            logging.debug(u"卖出usdt价: 普通交易区有挂单量 不符合要求   价格详情:%s" % (ret))
            ret = self.huilvdb['bigTrade_usdt_sell'].find_one({"time" : {"$gt" : start_time, "$lt":end_time}}, 
                                                        sort = [("time", pymongo.DESCENDING)])
            if ret is not None and len(ret['record']) >= min_bigtrade:
                for i in range(5):
                    huilv_cny_usdt_sell += ret['record'][i]['price']
                huilv_cny_usdt_sell = round(huilv_cny_usdt_sell / 5, 3)
                logging.debug(u"卖出usdt价: 大宗交易区符合挂单量[%s]要求 huilv_cny_usdt_sell:[%s] 价格详情:%s" % (len(ret['record']), huilv_cny_usdt_sell, ret['record'][:5]))
            else:
                logging.debug(u"卖出usdt价: 大宗交易区不符合挂单量  价格详情:%s" % (ret))

        if huilv_cny_usdt_sell is None or huilv_cny_usdt_sell > 7.3 or huilv_cny_usdt_sell < 5.9:
            msg = "huilv_cny_usdt_sell[%s] 异常！ 超出阀值范围[5.9 -- 6.9]" % huilv_cny_usdt_sell
            logging.warning(msg)
            # notice(msg)
            huilv_cny_usdt_sell = None
        return huilv_cny_usdt_sell

    def get_price_check(self,  coin, quote, start_time = int(time.time()*1000)-5*60*1000):
        params_str = json.dumps({
                            'coin' : coin,
                            'quote' : quote,
                            'start_time':int(start_time/(1000*60)) if start_time is not None else None
                        })
        m = hashlib.md5()
        m.update(params_str)
        cache_key = 'price_check_'+m.hexdigest()
        cache_content = self.redis.get(cache_key)
        if(cache_content):
            return pickle.loads(str(cache_content))
        else:
            ret = self.get_price_check_orign(coin, quote, start_time = start_time)
            if ret:
                self.redis.set(cache_key, pickle.dumps(ret), ex=60)
            return ret

    def get_price_check_orign(self,coin,quote,start_time=int(time.time()*1000)-5*60*1000):
        """
        获取coinmarket价格
        """
        price_rec=self.huilv_finance['huilv_coin'].find_one({'coin':coin.upper(),'time':{'$gte':start_time}},sort=[('time',-1)])
        if price_rec is None:
            return None
        return price_rec.get(quote.upper(),None)
    

    def adj_pct_usdt_usd(self,  end_time = None):
        params_str = json.dumps({
                            'end_time' : int(end_time/(1000*60)) if end_time is not None else None,
                        })
        m = hashlib.md5()
        m.update(params_str)
        cache_key = 'adj_pct_usdt_usd_'+m.hexdigest()
        cache_content = self.redis.get(cache_key)
        if(cache_content):
            return pickle.loads(str(cache_content))
        else:
            ret = self.adj_pct_usdt_usd_orign(end_time)
            self.redis.set(cache_key, pickle.dumps(ret), ex=60)
            return ret

    def adj_pct_usdt_usd_orign(self, end_time = None):
        """
        USDT变动策略
        买入usdt的汇率正常在1.1 -- 1.9之间，超出这个范围，溢价增加多少个百分点，阈值就相应升降多少百分点 
        溢价高，就降低pct，降最多降4个千分点，方便usdt转出其他稳定的国际货币；
        溢价低，就提高pct，最多4个千分点，
        """
        if end_time is None:
            end_time = int(time.time() * 1000)
        start_time = end_time - self.interval
        huilv_usdt = self.get_usdtcny_huilv_buy()
        huilv_usd = self.huilv_finance['huilv_item'].find_one({"time" : {"$gte" : start_time, "$lte" : end_time}, "code" : "USD"}, sort = [("time", -1)])
        if huilv_usdt is None or huilv_usd is None:
            logging.error("获取udst变动策略失败,huilv_usdt[%s] huilv_usd[%s] start_time[%s] end_time[%s]" % (huilv_usdt, huilv_usd, start_time, end_time))
            return None
        usdt_cny = huilv_usdt
        usd_cny = 1.0 / huilv_usd['refePrice']
        rate = round((usdt_cny - usd_cny)/usd_cny, 3) * 100.
        pct_adj = 0.
        add_max = 1.8
        add_min = 1.2
        if add_min <= rate <= add_max:
            logging.debug("usdt溢价比例[%s] 在[1.1, 1.9]%%之间 不需要特意调整阈值" % rate)
        else:
            if rate >= add_max:
                pct_adj = max((add_max - rate)/1.5, -0.4)
                logging.debug("usdt溢价[%s]超过%s%% pct_adj为[%s] 方便流出到国际货币" % (rate, add_max, pct_adj))
            else:
                pct_adj = min((add_min - rate)/1.5, 0.4)
                logging.debug("usdt溢价[%s]低于%s%% pct_adj为[%s] 方便国际货币流入" % (rate, add_min, pct_adj))
        msg = "初始汇率调整：获取usdt变动策略成功,买入usdt价[%s] usd_cny[%s] 溢价[%s%%] 阈值调整[%s]" % ( usdt_cny, usd_cny, rate, pct_adj)
        logging.debug(msg)
        return {"pct_adj" : round(pct_adj, 3), "msg":msg}
