# coding:utf-8
# author:shiyuming 
# modify:hudongdong hanwanhe guoxiangchao
'''
    行情数据库
'''
import pymongo
import logging,traceback
import time,json,re,copy,os,sys,datetime
import pickle,hashlib,cPickle
from pymongo import MongoClient, DESCENDING

from conf.publib_conf import CONN_ADDR_AWS, USERNAME_AWS, PWD_AWS
from conf.publib_conf import CONN_ADDR_FUNDINFO, USERNAME_FUNDINFO, PWD_FUNDINFO
from conf.publib_conf import CONN_ADDR_AWS_TRADE, USERNAME_TRADE, PWD_TRADE
from conf.publib_conf import DEPTH_LOG_DIR

from huilv import DbHuilv

class DbUtil:
    def __init__(self):
        self.client = MongoClient(CONN_ADDR_AWS)
        if USERNAME_AWS is not None:
            self.client.admin.authenticate(USERNAME_AWS, PWD_AWS)
        self.client_trade = MongoClient(CONN_ADDR_AWS_TRADE)
        if USERNAME_TRADE is not None:
            self.client_trade.admin.authenticate(USERNAME_TRADE, PWD_TRADE)
 
    def close(self):
        '''
        供显式调用释放数据等连接资源
        '''
        try:
            self.client.close()
            self.client_trade.close()
        except Exception,e:
            logging.warning("显式调用释放数据等连接资源异常:%s" % e)
            pass 
dbutil = DbUtil()
dbhuilv = DbHuilv()

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
        self.client = dbutil.client 
        self.db = self.client.get_database(self.market)
        self.client_trade = dbutil.client_trade 
        self.db_trade = self.client_trade.get_database(self.market)
        self.last_apicall = time.time()
        self.max_interval = 600
        self.indexnames= set()
        self.dbhuilv = dbhuilv

        self.log_cache = {}
    def __del__(self):
        try:
            self.client.close()
            self.client_trade.close()
        except:
            pass

    def close(self):
        '''
        供显式调用释放数据等连接资源
        '''
        try:
            self.client.close()
            self.client_trade.close()
        except Exception,e:
            logging.warning("显式调用释放数据等连接资源异常:%s" % e)
            pass 
    
    def ensure_index_client(self,client,db_name,table, where):
        '''
        如果索引不存在，则加索引
        '''
        index_name = '%s_%s'%(table,'_'.join(where.keys()))
        if index_name == '' or index_name in self.indexnames:
            return
        try:
            indexs = [(x, DESCENDING) for x in where.keys()]
            client[db_name][table].ensure_index(indexs)
            self.indexnames.add(index_name)
        except:
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
    
    def get_data_md5(self,item):
        #data = {k: v for k, v in item.items() if k not in ['_id', 'time','md5']}
        data = {"bids":item["bids"],"asks":item["asks"]}
        if data == {}:
            return None
        return hashlib.md5(json.dumps(data)).hexdigest()
    
    def set_null_tag(self,table_name,depth):
        """标记空交易对"""
        try:
           redis_data = self.dbhuilv.redis.set('depth_'+self.market+'_'+table_name+'_null', pickle.dumps(depth),60*5)
        except:
            pass
        return None
        

    def get_depth(self,table_name):
        """查询redis数据"""
        try:
           redis_data = self.dbhuilv.redis.get('depth_'+self.market+'_'+table_name)
           if redis_data :
               return pickle.loads(redis_data)
        except:
            pass
        return None

    def save_depth(self,table_name, depth, data_life=5,log_path=None,cach_length=10,set_redis = True):
        '''
        将数据写入磁盘和redis中
        set_redis ：是否写入redis
        cach_length:一次写入磁盘的数据条数
        '''
        #self.ensure_index(table_name,{"time":''})
        try:
            if depth is not None and len(depth.get("asks", [])) > 0 and len(depth.get("bids", [])) > 0:
                logging.debug("通过api获取行情数据无误" )
                ret_check = self.check_depth(depth,table_name)
                if depth["md5"] != self.get_data_md5(depth):
                    logging.warning("数据MD5值不一致:"+self.market+'_'+table_name)
                    return 
                if ret_check['status'] != "ok":
                    logging.warning("db_name:"+self.market+"table_name:"+table_name+"金融数据校验不通过"+ret_check['msg'])
                    return 
                else:
                    logging.debug("金融数据校验正常")
                    if set_redis is True:
                        self.dbhuilv.redis.set('depth_'+self.market+'_'+table_name, pickle.dumps(depth), data_life)
                    self.save_depth_log(table_name, depth,log_path=log_path,cach_length=cach_length)
                    return True
            elif "bids" in depth and "asks" in depth and (len(depth["bids"]) == 0 or len(depth["asks"]) == 0):
                self.set_null_tag(table_name,depth)
                return False
            else:
               logging.error("db_name:"+self.market+"table_name:"+table_name+"  depth is None! ") 
            return 
        except Exception, e:
            traceback.print_exc()
            logging.error("db_name:"+self.market+"table_name:"+table_name+"   "+ str(e) )

    def save_depth_log(self, table_name, depth,log_path=None,cach_length=10):
        log_cache_key = self.market+'_'+table_name
        if log_cache_key not in self.log_cache:
            self.log_cache[log_cache_key] = []
        line = json.dumps(depth)+'\n'
        self.log_cache[log_cache_key].append(line)
        if(len(self.log_cache[log_cache_key]) > cach_length):
            #hourdir = datetime.datetime.now().strftime("%Y%m%d%H")
            hourdir = datetime.datetime.now().strftime("%Y%m%d")
            default_path = DEPTH_LOG_DIR
            if log_path is not None:
                default_path = log_path
            file_dir = default_path + '/' + hourdir + '/' + self.market + '/'

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

    def get_market(self, coin, start_time, end_time, contract_type = None):
        '''
        获取行情深度，由[start_time, end_time]控制 单位是毫秒
        
        返回最近一个行情深度信息
        '''
        if contract_type:
            colname = "%s%s_%s" % (coin, contract_type, self.quote)
        else:
            colname = "%s_%s" % (coin, self.quote)
        where = {"time" : {"$gt" : start_time, "$lt":end_time}}
        cache_key = 'depth_'+self.market+'_'+colname
        cache_content = self.dbhuilv.redis.get(cache_key)
        if(cache_content):
            depth = pickle.loads(cache_content)
        else:
            #depth = self.db[colname].find_one(where, sort = [("time", pymongo.DESCENDING)])
            depth = None
        #金融数据校验，有风险则打个FATAL锁，报警等处理
        if depth is not None and len(depth.get("asks", [])) > 1 and len(depth.get("bids", [])) > 1:
            logging.debug("查询[%s]-[%s]的行情深度成功" % (self.market, colname))
            if  depth['time']< (time.time()-10) * 1000:
                depth = None
                logging.warning("%s金融数据时间校验不通过" % self.market)
                return depth
            ret_check = self.check_depth(copy.deepcopy(depth), colname)
            if ret_check['status'] != "ok":
                depth = None
                logging.warning("金融数据校验不通过")
            else:
                logging.debug("金融数据校验正常")
        else:
            logging.info("查询[%s]-[%s]的行情深度失败 where:%s  interval:%s" % (self.market, colname, where, where['time']['$lt'] - where['time']['$gt']))
        return depth

    def check_depth(self, depth_data, colname):
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
        depth = copy.deepcopy(depth_data)
        #转float
        try:
            if len(depth["bids"])==0 or len(depth["asks"])==0:
                return ret_check
            for p in depth['asks']:
                p['price'] = float(p['price'])
                p['size'] = float(p['size'])
            for p in depth['bids']:
                p['price'] = float(p['price'])
                p['size'] = float(p['size'])
            #所有的买单必须是低于卖单价格，
            price_bids_last = None
            for p in depth['bids'] :
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
            for p in depth['asks'] :
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
            logging.error("金融校验异常 %s %s " % (self.market,colname))
            ret_check = {"status":"failed",  "msg":""}
        return ret_check

    def update_market(self, coin, asks = None, bids = None):
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
        cache_key = 'depth_'+self.market+'_'+colname
        cache_content = self.dbhuilv.redis.get(cache_key)
        depth = None
        if(cache_content):
            depth = pickle.loads(cache_content)
        if depth is None:
            logging.warning("更新深度信息失败：查询失败。colname:%s" % colname )
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
            self.dbhuilv.redis.set('depth_'+self.market+'_'+colname, pickle.dumps(depth), 5)
            # self.db[colname].update_one(where, { '$set':depth }, upsert = True)
            
    
    def drop_zero(self,x):
        res=[]
        for item in x:
            if item['size']!=0:
                res.append(item)
        return res


