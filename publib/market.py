# coding:utf-8
# author:shiyuming 
# modify:hudongdong hanwanhe guoxiangchao
'''
    行情数据库
'''
import pymongo
import logging,traceback
import time,json,re,copy,os,sys,datetime
import cPickle as pickle
from pymongo import MongoClient, DESCENDING

from conf.publib_conf import CONN_ADDR_AWS, USERNAME_AWS, PWD_AWS
from conf.publib_conf import CONN_ADDR_FUNDINFO, USERNAME_FUNDINFO, PWD_FUNDINFO
from conf.publib_conf import CONN_ADDR_AWS_TRADE, USERNAME_TRADE, PWD_TRADE
from conf.publib_conf import DEPTH_LOG_DIR

from huilv import DbHuilv


class DbMarket:
    '''
    行情数据库
    '''
    
    def __init__(self, market, quote):
        '''
        market: 市场，如huobi
        col:    市场的一个币种行情
        '''
        self.market = market
        self.quote = quote
        logging.info("初始化dbutil. market:[%s] quote:%s" % (self.market, self.quote))
        self.client = MongoClient(CONN_ADDR_AWS)
        if USERNAME_AWS is not None:
            self.client.admin.authenticate(USERNAME_AWS, PWD_AWS)
        self.db = self.client.get_database(self.market)
        self.client_trade = MongoClient(CONN_ADDR_AWS_TRADE)
        if USERNAME_TRADE is not None:
            self.client_trade.admin.authenticate(USERNAME_TRADE, PWD_TRADE)
        self.db_trade = self.client_trade.get_database(self.market)

        self.last_apicall = time.time()
        self.max_interval = 600
        self.indexnames= set()
        self.dbhuilv = DbHuilv()

        self.log_cache = {}
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
    
    def ensure_index(self,table, where, is_trade=False):
        '''
        如果索引不存在，则加索引
        '''
        index_name = '%s_%s'%(table,'_'.join(where.keys()))
        if index_name == '' or index_name in self.indexnames:
            return
        try:
            indexs = [(x, DESCENDING) for x in where.keys()]
            if is_trade == False:
                self.db[table].ensure_index(indexs)
            else:
                self.db_trade[table].ensure_index(indexs)
            self.indexnames.add(index_name)
        except:
            pass

    def save_depth(self,table_name, depth, data_life=5):
        '''
        将数据保存到对应的mongo数据库
        '''
        #self.ensure_index(table_name,{"time":''})
        try:
            if depth is not None and len(depth.get("asks", [])) > 1 and len(depth.get("bids", [])) > 1:
                logging.debug("通过api获取行情数据无误" )
                ret_check = self.check_depth(depth, table_name)
                if ret_check['status'] != "ok":
                    depth = None
                    logging.warning("金融数据校验不通过"+ret_check['msg'])
                else:
                    logging.debug("金融数据校验正常")
                    self.dbhuilv.redis.set('depth_'+self.market+'_'+table_name, pickle.dumps(depth), data_life)
                    #self.db[table_name].insert_one(depth)
                    self.save_depth_log(table_name, depth)

            return depth
        except Exception, e:
            traceback.print_exc()
            print(e)

    def save_depth_log(self, table_name, depth):
        log_cache_key = self.market+'_'+table_name
        if log_cache_key not in self.log_cache:
            self.log_cache[log_cache_key] = []
        line = json.dumps(depth)+'\n'
        self.log_cache[log_cache_key].append(line)
        if(len(self.log_cache[log_cache_key]) > 10):
            hourdir = datetime.datetime.now().strftime("%Y%m%d%H")
            file_dir = DEPTH_LOG_DIR + '/' + hourdir + '/' + self.market + '/'

            if(not os.path.exists(file_dir)):
                os.makedirs(file_dir)
            file = file_dir + table_name+'.log'
            with open(file, 'a+') as f:
                for line in self.log_cache[log_cache_key]:
                    f.write(line)
                self.log_cache[log_cache_key] = []
        pass

    def save_order_placing_data(self,table_name, depth):
        '''
        将盘口数据保存到对应的数据库
         params:
            table_name:表名
            depth:asks和bids数据
        '''
        trade_result=self.get_order_placing_data(depth,table_name)
        if trade_result:
            self.ensure_index(table_name,{"time":''}, is_trade=True)
            try:
                self.db_trade[table_name].insert_one(trade_result)
            except Exception, e:
                print(e)

    def get_order_placing_data(self,depth,table_name):
        '''
        将交易一定额度之后的数据返回
        '''
        result={}
        quote=table_name.split("_")[1]
        move_one=0
        if quote=='btc':
            move_one=0.2
        if quote=='eth':
            move_one=2
        else:
            move_one=10000
        asks_list =depth['asks']
        bids_list =depth['bids']
        huilv=None
        if quote in ["btc","eth","gbp","usdt","eur","usd","rub","idr","sgd","krw","jpy","cny","pln","uah","cad","aud","php","hkd"]:
            huilv = self.dbhuilv.get_huilv(quote, is_go = True)     
        if not huilv:
            return None
        else:
            price_ask=self.match_process(asks_list, move_one, 'asks', huilv)
            price_bid=self.match_process(bids_list, move_one, 'bids', huilv)
            price_asks_first = asks_list[0]['price']
            price_bids_first = bids_list[0]['price']
            result['price_ask_trade']=price_ask/huilv
            result['price_bid_trade']=price_bid/huilv
            result['price_asks_first']=price_asks_first/huilv
            result['price_bids_first']=price_bids_first/huilv
            result['huilv']=huilv     
            result['time']=depth['time'] 
            return result

    def match_process(self,lists,move_one,_type,huilv):
        '''
        交易过程：即将交易之后的价格返回
        '''
        sum=0
        idx=0
        while idx< len(lists):
            sum=sum+lists[idx]['size']*lists[idx]['price']/huilv
            if sum>=move_one:
                return lists[idx]['price']
            idx+=1
        if _type=='bids':
            return lists[0]['price']*0.7
        else :
            return lists[0]['price']*1.3
    
    def get_markets(self, coins,duration = 5, contract_type = None, contract_types = None):
        '''
        获取行情深度，单位是毫秒
        
        返回所有coins最近一个行情深度信息
        '''
        markets={}
        depths =[]
        if self.market in ['bitbank','binance']:
            coins1 = [coin if coin !='bch' else 'bcc' for coin in coins]
        else:
            coins1 = coins
        if contract_type == None and contract_types == None:
            colnames = ["%s_%s" % (coin, self.quote) for coin in coins1]
        elif contract_type != None:
            colnames = ["%s%s_%s" % (coin, contract_type, self.quote) for coin in coins1]
        else:
            colnames = ["%s%s_%s" % (coin, contract_types[coin], self.quote) for coin in coins1]
        cache_keys = ['depth_'+self.market+'_'+colname for colname in colnames]
        cache_contents =self.dbhuilv.redis.mget(cache_keys)
        for cache_content in cache_contents:
            if(cache_content):
                depth = pickle.loads(str(cache_content))
            else:
                depth = None 
            if depth is not None and len(depth.get("asks", [])) > 1 and len(depth.get("bids", [])) > 1:
                logging.debug("查询[%s]-[%s]的行情深度成功" % (self.market, colname))
                if  depth['time']< (time.time() - duration) * 1000:
                    logging.warning("%s金融数据时间校验不通过 %s" % (self.market,time.time()*1000 - depth['time']))
                    depth = None
            depths.append(depth)
        markets = dict(zip(coins,depths))
        return markets
            
    def get_market(self, coin, start_time, end_time, duration = 5,contract_type = None):
        '''
        获取行情深度，由[start_time, end_time]控制 单位是毫秒
        
        返回最近一个行情深度信息
        '''
        if self.market in ['bitbank','binance']:
            if coin =='bch':
                coin ='bcc'
        if contract_type:
            colname = "%s%s_%s" % (coin, contract_type, self.quote)
        else:
            colname = "%s_%s" % (coin, self.quote)
        where = {"time" : {"$gt" : start_time, "$lt":end_time}}
        cache_key = 'depth_'+self.market+'_'+colname
        cache_content = self.dbhuilv.redis.get(cache_key)
        if(cache_content):
            depth = pickle.loads(str(cache_content))
        else:
            #depth = self.db[colname].find_one(where, sort = [("time", pymongo.DESCENDING)])
            depth = None
        #金融数据校验，有风险则打个FATAL锁，报警等处理
        if depth is not None and len(depth.get("asks", [])) > 1 and len(depth.get("bids", [])) > 1:
            logging.debug("查询[%s]-[%s]的行情深度成功" % (self.market, colname))
            if  depth['time']< (time.time()-duration) * 1000:
                logging.warning("coin %s %s金融数据时间校验不通过,延迟%s" % (coin,self.market,(time.time() *1000 - depth['time'])/1000.0 ))
                depth = None
                return depth
            # ret_check = self.check_depth(copy.deepcopy(depth), colname)
            # if ret_check['status'] != "ok":
            #     depth = None
            #     logging.warning("金融数据校验不通过")
            # else:
            #     logging.debug("金融数据校验正常")
        else:
            logging.info("查询[%s]-[%s]的行情深度失败 where:%s  interval:%s" % (self.market, colname, where, where['time']['$lt'] - where['time']['$gt']))

        return depth

    def check_depth(self, depth, colname):
        '''
        金融数据校验：
            所有的吃单必须是低于卖单价格
            所有的吃单必须是降序的
            所有卖单必须高于吃单
            所有的卖单必须是升序的
        params:
            depth:深度
        return:
            {"status":"ok", "msg":""} ok是检验通过，其他是校验不通过，msg是不通过的原因
        '''
        ret_check = {"status":"ok", "msg":""}
        depth_data = copy.deepcopy(depth)
        #转float
        try:
            for p in depth['asks']:
                p['price'] = float(p['price'])
                p['size'] = float(p['size'])
            for p in depth['bids']:
                p['price'] = float(p['price'])
                p['size'] = float(p['size'])
            #所有的买单必须是低于卖单价格，
            price_bids_last = None
            for p in depth['bids']:
                if p['price'] >= depth['asks'][0]['price']:
                    msg = "price error: bids[%s] >= asks[%s] . depth of [%s]-[%s] md5[%s]" % (p['price'], depth['asks'][0]['price'], self.market, colname, depth.get('md5', ''))
                    logging.warning(msg)
                    logging.debug("depth:%s" % depth)
                    ret_check = {"status":"failed", "msg":msg}
                    break
                #所有的吃单必须是降序的
                if price_bids_last is None:
                    price_bids_last = p['price']
                if p['price'] > price_bids_last:
                    msg = "price error: bids not sort descending. [%s] after[%s] depth of [%s]-[%s] md5[%s]" % (p['price'], price_bids_last, self.market, colname, depth.get('md5', ''))
                    logging.warning(msg)
                    logging.debug("depth:%s" % depth)
                    ret_check = {"status":"failed", "msg":msg}
                    break
            #所有卖单必须高于吃单
            price_asks_last = None
            for p in depth['asks']:
                if p['price'] <= depth['bids'][0]['price']:
                    msg = "price error: asks[%s] <= bids[%s]. depth of [%s]-[%s] md5[%s]" % (p['price'], depth['bids'][0]['price'], self.market, colname, depth.get('md5', ''))
                    logging.warning(msg)
                    logging.debug("depth:%s" % depth)
                    ret_check = {"status":"failed", "msg":msg}
                    break
                #所有的卖单必须是升序的
                if price_asks_last is None:
                    price_asks_last = p['price']
                if p['price'] < price_asks_last:
                    msg = "asks not sort ascending. [%s] after [%s] depth of [%s]-[%s] md5[%s]" % (p['price'], price_asks_last, self.market, colname, depth.get('md5', ''))
                    logging.warning(msg)
                    logging.debug("depth:%s" % depth)
                    ret_check = {"status":"failed", "msg":msg}
                    break
        except :
            traceback.print_exc()
            print depth_data
            ret_check = {"status":"failed",  "msg":""}
        return ret_check

    def update_market(self, coin, asks = None, bids = None, contract_type= None):
        '''
        更新深度信息，使撮合引擎可以共享
        params:
            depth_id, 深度的id 
            asks, 卖出单列表
            bids, 买入单列表
        return:
            None
        '''
        colname = "%s_%s" % (coin, self.quote)
        if contract_type==None:
            cache_key = 'depth_'+self.market+'_'+colname
        else:
            cache_key = 'depth_'+self.market+'_'+"%s%s_%s" % (coin, contract_type, self.quote)
        cache_content = self.dbhuilv.redis.get(cache_key)
        depth = None
        if(cache_content):
            depth = pickle.loads(str(cache_content))
        if depth is None:
            logging.warning("更新深度信息失败：查询失败。cache_key%s,colname:%s" % (cache_key, colname) )
        else:
            logging.info("更新深度信息。colname:%s" % colname )
            if asks is not None:
                depth['asks_old'] = copy.deepcopy(depth['asks'])
                asks=self.drop_zero(asks)
                depth['asks'] = asks
                logging.debug("更新深度信息的asks")
            if bids is not None:
                depth['bids_old'] = copy.deepcopy(depth['bids'])
                bids=self.drop_zero(bids)
                depth['bids'] = bids
                logging.debug("更新深度信息的bids")
            # self.dbhuilv.redis.set('depth_'+self.market+'_'+colname, pickle.dumps(depth), 5)
            self.dbhuilv.redis.set(cache_key, pickle.dumps(depth), 5)
            # self.db[colname].update_one(where, { '$set':depth }, upsert = True)
            
    
    def drop_zero(self,x):
        res=[]
        for item in x:
            if item['size']!=0:
                res.append(item)
        return res


    def get_ticker(self, coin, duration = 5, contract_type = None):
        """
        获取ticker数据
        return [{
            'price': 4509000.0, 
            'side': 'sell', 
            'time_format': '2018-12-04 16:52:02', 
            'amount': 0.0114, 
            'crawl_time': 1543913531242.951, 
            'time': 1543913522000.0, 
            'type': 'spot', 
            'id': u'31627521'}
        ]
        """
        key = "exchange_%s_exchange_%s%s" % (self.market, coin, self.quote)
        if contract_type:
            key = "exchange_%s_exchange_%s%s" % (self.market, coin, self.quote)
        value = self.dbhuilv.redis.get(key)
        if value is None:
            return None
        value = pickle.loads(str(value))
        if value is None or value == '':
            return None
        #检测数据
        if len(value) == 0:
            return None
        if time.time() - value[0]['time'] > duration * 1000:
            logging.debug("获取ticker数据,时间校验不通过,%s" % (time.time() - value[0]['time']/1000))
            return None
        return value
            