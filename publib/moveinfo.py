# coding:utf-8
# author:shiyuming 
# modify:hudongdong hanwanhe guoxiangchao
'''
拆自dbutile
保存对冲信息：
    每次对冲尝试的结果：how（go/goback) 
'''
import pymongo
import logging
import time,json,re,copy,os,sys
from pymongo import MongoClient, DESCENDING
from conf.publib_conf import LOCK_ERROR
from conf.publib_conf import CONN_ADDR_FUNDINFO, USERNAME_FUNDINFO, PWD_FUNDINFO, SERVER_NAME, KARKEN_USDT_ADDR
from huilv import DbHuilv
import traceback
from send_sms import notice_by_dingding,hedging_notice
TRADE_TYPE_MARGIN = 'margin' #交易方式--杠杆交易
TRADE_TYPE_SPOT = 'spot'     #交易方式--现货交易
class DbMoveinfo:
    '''
    保存对冲信息：
        每次对冲尝试的结果：how（go/goback) 
    '''
    def __init__(self):
        self.karken_client = MongoClient(KARKEN_USDT_ADDR)
        self.karken_client.admin.authenticate(USERNAME_FUNDINFO, PWD_FUNDINFO)
        self.karken_db = self.karken_client.get_database("fundinfo")
        self.client_fundinfo = MongoClient(CONN_ADDR_FUNDINFO)
        self.db = self.client_fundinfo.get_database("fundinfo")
        if USERNAME_FUNDINFO is not None:
            self.client_fundinfo.admin.authenticate(USERNAME_FUNDINFO, PWD_FUNDINFO)

        self.db = self.client_fundinfo.get_database("fundinfo")
        # self.db["move_result"].ensure_index([ ("op_id", DESCENDING)])
        # self.db['move_result'].ensure_index([('market_name', DESCENDING), ('price_finaly' ,DESCENDING)])
        self.moveinfo_indexes = {
            "talbe":1 #表示这个表已经加了索引
        }
        self.max_interval = 100 #通用最大时间间隔
        self.dbhuilv = DbHuilv()

    def __del__(self):
        try:
            self.client_fundinfo.close()
        except:
            pass

    def close(self):
        '''
        供显式调用释放数据等连接资源
        '''
        try:
            self.client_fundinfo.close()
        except Exception, e:
            logging.warning("显式调用释放数据等连接资源异常:%s" % e)
            pass 
    
    def save_lend_coin_contract_config(self, path, pm, symbol,coin, is_dispenser = False):
        '''
        如果币不足两次交易并且满足行情，则将该币以及交易所信息插入数据库
        '''
        result = {
                'path': path,
                'name': pm, 
                'symbol' : symbol, 
                'coin' : coin , 
                'key' : pm +'_'+ coin,
                'create_time' : int(time.time()), 
                }
        if is_dispenser:
            self.db['margin_order_dispenser'].insert(result)
        else:
            self.db['margin_order_contract_catch'].insert(result)

    def save_lend_coin_config(self,pm, path, symbol, coin):
        '''
        如果币不足两次交易并且满足行情，则将该币以及交易所信息插入数据库
        '''
        result = {
                'name': pm, 
                'path': path,
                'symbol' : symbol, 
                'coin' : coin , 
                'create_time' : int(time.time()), 
                }
        self.db['margin_order_catch'].insert(result)


    def save_lack_coin_config(self, pm, symbol, coin):
        '''
        如果huobi上币不够的话，将数据插入数据库，供祥超借币
        '''
        where = {'name': pm, 'symbol': symbol, 'coin' :coin ,"type" : "auto_margin"}
        config = self.db['margin_order'].find_one(where, sort = [("create_time", pymongo.DESCENDING)])
        result = {
                'name': pm, 
                'symbol' : symbol, 
                'coin' : coin , 
                'status' : 1, 
                'create_time' : int(time.time()), 
                "type" : "auto_margin"}
        if config is None:
            self.db['margin_order'].insert(result)
            return 
        else:
            if (time.time() - config['create_time'] > 120  and config['status'] == -1 ) or (time.time() - config['create_time'] > 600 and config['status'] == 3):
                self.db['margin_order'].insert(result)
           
    def save_depth_config(self, depth_result):
        '''
        将一分钟内未取到数据的depth存入数据库
        '''
        self.db['depth_config'].update_one(depth_result, { '$set':depth_result }, upsert = True)
    
    def get_hedge_configs(self, path, balance = None, money_move_min = None, pct_force = False, end_time = None, is_future = False):
        '''
        对冲配置信息表 来自两个地方：hedge_conf 对冲配置信息表：对冲配置信息和本对冲路径的风控信息  风控表：risk_manage_total
        {
            "path":"huobi-usdt_xrp_bitbank-jpy",
            "update_time":时间戳，单位是秒。 path+update_time(倒序)建索引。我总是取该路径下最近一条
            "max_b10_count":1 #实际利差和预期利差相比，少了超过10个千分点的风险次数
            "max_b5_count":3   #实际利差和预期利差相比，少了超过5个千分点的风险次数
            "max_b2_count":20  #实际利差和预期利差相比，少了超过2个千分点的风险次数
            "max_fail_count":10 #1天交易失败10次，是需要检修系统的
            "max_loss_money_path":-10000 #本路径资金最大亏损额度
            "min_pct_move":1.0 #对冲阀
            "money_move_one":100 #一次的对冲量（法币统一用cny，如1000 和 10000 或用btc eth计价最小搬动量）

            #coin指定币的风控信息 来自表warn_coin_level
            "max_loss_money_total":-50000, #总账户总计亏损不应该超过指定数量
            "max_loss_coin":-10000, #币的最大损失量 对币不平进行风控
            "max_inc_coin":100000 #币的最大增多量 对币不平进行风控
            "coin_a2b_total":1000000, #币 
        }
        '''
        if end_time is None:
            end_time = time.time()
        #从hedge_conf取
        where = { "path": path }
        if is_future == False:
            config = self.db['hedge_conf'].find_one(where, sort = [("update_time", pymongo.DESCENDING)])
        else:
            config = self.db['hedge_conf_future'].find_one(where, sort = [("update_time", pymongo.DESCENDING)])
        if config is None:
            logging.warning("从hedge_conf获取本路径的风控信息失败 where:%s" % where)
            return None
        del config['update_time']
        for k,v in config.items():
            try:
                if k not in ['hedge_path','coins']:
                    vv = float(v)
                    config[k] = vv
            except :
                pass
        #从warn_coin_level取
        coins = config['coins']
        config_total = self.db['warn_coin_level'].find_one({"_id":"total"})
        if config_total is None:
            logging.warning("获取总体资金配置和风控信息失败 表[warn_coin_level]")
            return None
        config['money_init_uni'] = float(config_total['money_cny']['init_amount'])
        config['max_loss_money_total'] = float(config_total['money_cny']['max_loss'])
        config['coin_a2b_total'] = {}
        config['max_loss_coin'] = {}
        config['max_inc_coin'] = {}
        for coin  in coins:
            try:
                config['coin_a2b_total'][coin] = float(config_total[coin]['init_amount'])
                config['max_loss_coin'][coin] = float(config_total[coin]['max_loss'])
                config['max_inc_coin'][coin] = float(config_total[coin]['max_inc'])
            except :   
                config['coin_a2b_total'][coin] = 0
                config['max_loss_coin'][coin] = -10000
                config['max_inc_coin'][coin] =  10000

        #更新对冲阈值
        if pct_force == False:
            logging.info("路径将使用预设定的安全阈值 ")
        else:
            where_pct_a2b = { "_id" : path , "update_time" : { "$gt":end_time - 1800, "$lt": end_time + 20}, "top_pct_avg":{"$lt":99.0} }
            pct_apts_a2b = self.db['pct_apt'].find_one(where_pct_a2b)
            pma, quote_a, pmb, quote_b = re.split(r'[_-]', path)
            path_b2a = "%s_%s-%s_%s" % (pmb, quote_b, pma, quote_a)
            where_pct_b2a = { "_id" : path_b2a , "update_time" : { "$gt":end_time - 1800, "$lt": end_time + 20}, "top_pct_avg":{"$lt":99.0} }
            pct_apts_b2a = self.db['pct_apt'].find_one(where_pct_b2a)
            # min_pct_moves=re.split(',', str(config['min_pct_move']))
            # min_pct_move_a2b = float(config['min_pct_move_a2b'])
            # min_pct_move_b2a = float(config['min_pct_move_b2a'])
            #if pct_apts_a2b.count() != 0 and config.get("force_manual", 0) != 1:
            #    pct_moves = []
            #    for item in pct_apts_a2b:
            #        coin =item['coin']
            #        pma_money = '%s_money' % pma
            #        pmb_coin = '%s_%s' % (pmb, coin)
            #        if balance[coin][pma_money] > 1.5 * money_move_min and balance[coin][pmb_coin] * item['coin_price'] > 1.5 * money_move_min:
            #            pct_moves.append(item['top_pct_avg'])
            #    logging.debug("获取到的")
            #    if len(pct_moves)>0:
            #        logging.debug("路径[%s]获取到自适应的对冲阈值:%s  min_pct_move_a2b:%s  pct_go:%s " % (path,  max( config['min_pct_move_a2b'], min(pct_moves)), config['min_pct_move_a2b'], min(pct_moves)))
            #        config['min_pct_move_a2b'] = max( config['min_pct_move_a2b'], max(pct_moves))
            #if pct_apts_b2a.count() != 0 and config.get("force_manual", 0) != 1:
            #    pct_moves = []
            #    for item in pct_apts_b2a:
            #        coin =item['coin']
            #        pmb_money = '%s_money' % pmb
            #        pma_coin = '%s_%s' % (pma, coin)
            #        if balance[coin][pmb_money] > 1.5 * money_move_min and balance[coin][pma_coin] * item['coin_price'] > 1.5 * money_move_min:
            #            pct_moves.append(item['top_pct_avg'])
            #    logging.debug("获取到的")
            #    if len(pct_moves)>0:
            #        logging.debug("路径[%s]获取到自适应的对冲阈值:%s  min_pct_move_b2a:%s  pct_go:%s " % (path,  max( config['min_pct_move_b2a'], min(pct_moves)), config['min_pct_move_b2a'], min(pct_moves)))
            #        config['min_pct_move_b2a'] = max(config['min_pct_move_b2a'], max(pct_moves))
            # config['min_pct_move'] =  str(min_pct_move_a2b) + "," + str(min_pct_move_b2a)      
            if pct_apts_a2b is not None and config.get("force_manual", 0) != 1:
                config['min_pct_move_a2b'] = pct_apts_a2b["top_pct_avg"]
                logging.info("路径[%s] 获取自适应阈值：%s" %(path,config['min_pct_move_a2b']))
            if pct_apts_b2a is not None and config.get("force_manual", 0) != 1:
                config['min_pct_move_b2a'] = pct_apts_b2a["top_pct_avg"]
                logging.info("路径[%s] 获取自适应阈值：%s" %(path_b2a,config['min_pct_move_b2a']))
        #获取usdt的OTC市场价格范围，来自usdt_threshold表
        usdt_threshold = self.db['usdt_threshold'].find_one({"_id":"usdt"})
        if usdt_threshold is None:
            logging.debug("获取usdt的OTC价格失败，表[usdt_threshold]")
            return None
        else:
            otc_low = float(usdt_threshold['usdt_low'])
            otc_high = float(usdt_threshold['usdt_high'])
            config['otc_usdt'] = {"otc_low":otc_low,"otc_high":otc_high}
        return config
    
    def get_hedge_config_contract(self, path, coin = None, end_time = None):
        '''
        对冲配置信息表 来自两个地方：hedge_conf 对冲配置信息表：对冲配置信息和本对冲路径的风控信息  风控表：risk_manage_total
        {
            "path":"huobi-usdt_nxrp_bitbank-jpy",
            "update_time":时间戳，单位是秒。 path+update_time(倒序)建索引。我总是取该路径下最近一条
            "max_b10_count":1 #实际利差和预期利差相比，少了超过10个千分点的风险次数
            "max_b5_count":3   #实际利差和预期利差相比，少了超过5个千分点的风险次数
            "max_b2_count":20  #实际利差和预期利差相比，少了超过2个千分点的风险次数
            "max_fail_count":10 #1天交易失败10次，是需要检修系统的
            "max_loss_money_path":-10000 #本路径资金最大亏损额度
            "min_pct_move":1.0 #对冲阀
            "money_move_one":100 #一次的对冲量（法币统一用cny，如1000 和 10000 或用btc eth计价最小搬动量）

            #coin指定币的风控信息 来自表warn_coin_level
            "max_loss_money_total":-50000, #总账户总计亏损不应该超过指定数量
            "max_loss_coin":-10000, #币的最大损失量 对币不平进行风控
            "max_inc_coin":100000 #币的最大增多量 对币不平进行风控
            "coin_a2b_total":1000000, #币 
        }
        '''
        if coin is None:
            coin = path.split("_")[1]
            logging.debug("从path[%s] 解析出coin[%s]" % (path, coin))
        if end_time is None:
            end_time = time.time()
        #从hedge_conf取
        where = { "path":path }
        config = self.db['hedge_conf_new'].find_one(where, sort = [("update_time", pymongo.DESCENDING)])
        if config is None:
            logging.warning("从hedge_conf获取本路径的风控信息失败 where:%s" % where)
            return None
        del config['update_time']
        for k,v in config.items():
            try:
                if k != 'hedge_path':
                    vv = float(v)
                    config[k] = vv
            except :
                pass
        #从warn_coin_level取
        config_total = self.db['warn_coin_level'].find_one({"_id":"total"})
        if config_total is None:
            logging.warning("获取总体资金配置和风控信息失败 表[warn_coin_level]")
            return None
        config['money_init_uni'] = float(config_total['money_cny']['init_amount'])
        config['max_loss_money_total'] = float(config_total['money_cny']['max_loss'])
        try:
            config['coin_a2b_total'] = float(config_total[coin]['init_amount'])
            config['max_loss_coin'] = float(config_total[coin]['max_loss'])
            config['max_inc_coin'] = float(config_total[coin]['max_inc'])
        except :   
            config['coin_a2b_total'] = 0
            config['max_loss_coin'] =-10000
            config['max_inc_coin'] = 10000
        return config

    def get_balance_info_coin(self, coin, pma, quote_a, huilv_quote_a, flag_pma_margin, pmb, quote_b, huilv_quote_b, flag_pmb_margin, time_interval = 100):
        '''
        获取到当前的全部账户详情：
            账户明细信息来自exchange_balance_detail表
            账户总体信息来自exchange_balance_total表
        返回 
            {
                "money_uni":5000000.,
                "coin_amount": 100., #self.coin_a2b的总量
                "huobi": #pma
                    {
                        "coin_amount":50,
                        "money_org":10000,
                        "money":650000,
                    },
                "bitbank":#pmb
                    {
                        "coin_amount":50,
                        "oney_org":100000000,
                        "money":65000
                    },
                "frozen":{
                    "huobi": #pma
                        {
                            "coin_amount":50,
                            "money_org":10000,
                            "money":650000,
                        },
                    "bitbank":#pmb
                        {
                            "coin_amount":50,
                            "oney_org":100000000,
                            "money":65000
                        },
                }
            }
        '''

        where = {"update_time":{"$gt" : time.time() - 6 * time_interval}}
        balance_detail = self.db['exchange_balance_detail'].find_one(where, sort=[("update_time", DESCENDING)])
        if balance_detail is None:
            logging.warning("获取账户详细信息失败 colname[exchange_balance_detail] where:%s time_interval:%s" % (where, time_interval))
            return None
        if pma not in balance_detail:
            logging.warning("获取账户详细信息失败 获取的账户详情中缺少平台[%s]的数据" % pma)
            return None
        if pmb not in balance_detail:
            logging.warning("获取账户详细信息失败 获取的账户详情中缺少平台[%s]的数据" % pmb)
            return None
        # logging.debug("获取账户详细信息:%s" % balance_detail)
        del balance_detail['_id']

        balanceinfo = { "frozen": {pma:{}, pmb:{}} }
        for (pm, trade_type, coin, quote, huilv) in [(pma, flag_pma_margin, coin, quote_a, huilv_quote_a), (pmb, flag_pmb_margin, coin, quote_b, huilv_quote_b)]:
            if trade_type == TRADE_TYPE_MARGIN:
                for symbol, tmp_balance in balance_detail[pm]['margin'].items():
                    if coin in tmp_balance and quote in tmp_balance:
                        balanceinfo[pm] = {
                            "coin_amount":float(tmp_balance[coin]),
                            "money_org":float(tmp_balance[quote]),
                            "money" : float(tmp_balance[quote] / huilv)
                        }
                        break
                for symbol, tmp_balance in balance_detail[pm]['margin_frozen'].items():
                    if coin in tmp_balance and quote in tmp_balance:
                        balanceinfo["frozen"][pm] = {
                            "coin_amount":float(tmp_balance[coin]),
                            "money_org":float(tmp_balance[quote]),
                            "money" : float(tmp_balance[quote] / huilv)
                        }
                        break
                if pm not in balanceinfo:
                    logging.debug("获取期货账户详细信息失败，  pm[%s]-balanceinfo:%s" % (pm, balance_detail[pm]))
                    return None
                else:
                    logging.debug("整理平台[%s]的期货账户信息:%s" % (pm, balanceinfo[pm]))
            elif trade_type == TRADE_TYPE_SPOT:
                if coin not in balance_detail[pm]['spot'] or quote not in balance_detail[pm]['spot'] :
                    logging.debug("获取现货账户详细信息失败 coin[%s]或quote[%s] 在spot中，pm[%s]-balanceinfo:%s" % (pm, coin, quote, balance_detail[pm]))
                    return None
                balanceinfo[pm] = {
                    "coin_amount" : float(balance_detail[pm]['spot'][coin]),
                    "money_org" : float(balance_detail[pm]['spot'][quote]),
                    "money" : float(balance_detail[pm]['spot'][quote] / huilv)
                }
                if coin in balance_detail[pm]['spot_frozen'] and quote in balance_detail[pm]['spot_frozen']:
                    logging.debug("现货存在frozen，取出来")
                    balanceinfo['frozen'][pm] = {
                        "coin_amount" : float(balance_detail[pm]['spot_frozen'][coin]),
                        "money" : float(balance_detail[pm]['spot_frozen'][quote]),
                        "money" : float(balance_detail[pm]['spot_frozen'][quote] / huilv)
                    }
                logging.debug("整理平台[%s]的现货账户信息:%s" % (pm, balanceinfo[pm]))
            else:
                for symbol, tmp_balance in balance_detail[pm]['future'].items():
                    if coin in tmp_balance and quote in tmp_balance:
                        key = coin + "_available"
                        balanceinfo[pm] = {
                            "total_coin" : float(tmp_balance[coin]),
                            "coin_amount":float(tmp_balance.get(key, 0)) * 10,
                            "money_org":0,
                            "money" : 0
                        }
                        break
                for symbol, tmp_balance in balance_detail[pm]['future_frozen'].items():
                    if coin in tmp_balance and quote in tmp_balance:
                        balanceinfo["frozen"][pm] = {
                            "coin_amount": float(tmp_balance[coin]),
                            "money_org": float(tmp_balance[quote]),
                            "money" : float(tmp_balance[quote]),
                            "contract_detail" : tmp_balance['contract_detail'] if 'contract_detail' in tmp_balance.keys() else {}
                        }
                        break
                if pm not in balanceinfo:
                    logging.debug("获取期货账户详细信息失败.pm[%s]-balanceinfo:%s" % (pm, balance_detail[pm]))
                    return None
                else:
                    logging.debug("整理平台[%s]的期货账户信息:%s" % (pm, balanceinfo[pm]))

        where_total = {"update_time":{"$gt":time.time() - self.max_interval}}
        balance_total = self.db['exchange_balance_total'].find_one(where_total, sort = [("update_time", DESCENDING)])
        if balance_total is None:
            logging.warning("获取账户详细信息失败 获取或生产balance_total失败 where:%s" % where_total)
            return None
        else:
            # logging.debug("获取到balance_total:%s" % balance_total)
            pass
        balanceinfo['money_uni'] = float(balance_total['money_cny'])
        balanceinfo['coin_amount'] = float(balance_total['coins'].get(coin, 0.0))
        
        logging.debug("获取得到账户信息:%s" % balanceinfo)
        return balanceinfo
    
    def get_hedge_config(self, path, balance = None, money_move_min = None, pct_force = False, end_time = None):
        '''
        对冲配置信息表 来自两个地方：hedge_conf 对冲配置信息表：对冲配置信息和本对冲路径的风控信息  风控表：risk_manage_total
        {
            "path":"huobi-usdt_xrp_bitbank-jpy",
            "update_time":时间戳，单位是秒。 path+update_time(倒序)建索引。我总是取该路径下最近一条
            "max_b10_count":1 #实际利差和预期利差相比，少了超过10个千分点的风险次数
            "max_b5_count":3   #实际利差和预期利差相比，少了超过5个千分点的风险次数
            "max_b2_count":20  #实际利差和预期利差相比，少了超过2个千分点的风险次数
            "max_fail_count":10 #1天交易失败10次，是需要检修系统的
            "max_loss_money_path":-10000 #本路径资金最大亏损额度
            "min_pct_move":1.0 #对冲阀
            "money_move_one":100 #一次的对冲量（法币统一用cny，如1000 和 10000 或用btc eth计价最小搬动量）

            #coin指定币的风控信息 来自表warn_coin_level
            "max_loss_money_total":-50000, #总账户总计亏损不应该超过指定数量
            "max_loss_coin":-10000, #币的最大损失量 对币不平进行风控
            "max_inc_coin":100000 #币的最大增多量 对币不平进行风控
            "coin_a2b_total":1000000, #币 
        }
        '''
        if end_time is None:
            end_time = time.time()
        #从hedge_conf取
        where = { "path":path }
        config = self.db['hedge_conf'].find_one(where, sort = [("update_time", pymongo.DESCENDING)])
        if config is None:
            logging.warning("从hedge_conf获取本路径的风控信息失败 where:%s" % where)
            return None
        del config['update_time']
        for k,v in config.items():
            try:
                if k not in ['hedge_path','hedge_path','coins']:
                    vv = float(v)
                    config[k] = vv
            except :
                pass
        #从warn_coin_level取
        coins = config['coins']
        config_total = self.db['warn_coin_level'].find_one({"_id":"total"})
        if config_total is None:
            logging.warning("获取总体资金配置和风控信息失败 表[warn_coin_level]")
            return None
        config['money_init_uni'] = float(config_total['money_cny']['init_amount'])
        config['max_loss_money_total'] = float(config_total['money_cny']['max_loss'])
        config['coin_a2b_total'] = {}
        config['max_loss_coin'] = {}
        config['max_inc_coin'] = {}
        for coin  in coins:
            try:
                config['coin_a2b_total'][coin] = float(config_total[coin]['init_amount'])
                config['max_loss_coin'][coin] = float(config_total[coin]['max_loss'])
                config['max_inc_coin'][coin] = float(config_total[coin]['max_inc'])
            except :   
                config['coin_a2b_total'][coin] = 0
                config['max_loss_coin'][coin] = -10000
                config['max_inc_coin'][coin] =  10000

        #更新对冲阈值
        where_pct = { "path":path , "update_time":{ "$gt":end_time - 1800, "$lt": end_time+ 20}, "top_pct_avg":{"$lt":99.0} }
        pct_apts = self.db['pct_apt'].find(where_pct)
        if pct_apts.count() == 0 or pct_force == False:
            logging.info("路径[%s]没有统计到自适应的对冲阈值，将使用预设定的安全阈值[%s] where:%s" % (path, config.get("min_pct_move", 10.), where_pct))
        else :
            if config.get("force_manual", 0) == 1:
                logging.debug("强制使用人工设定的阈值进行对冲 ")
            else:
                pct_moves = []
                pma, _, pmb, _ = re.split(r'[_-]', path)
                for item in pct_apts:
                    coin =item['coin']
                    pma_money = '%s_money' % pma
                    pmb_coin = '%s_%s' % (pmb, coin)
                    if balance[coin][pma_money] > 1.5 * money_move_min and balance[coin][pmb_coin] * item['coin_price'] > 1.5 * money_move_min:
                        pct_moves.append(item['top_pct_avg'])
                logging.debug("获取到的")
                if len(pct_moves)>0:
                    pct_go = max( config['min_pct_move'], min(pct_moves))
                    logging.debug("路径[%s]获取到自适应的对冲阈值:%s  min_pct_move:%s  pct_go:%s " % (path,  min(pct_moves), config['min_pct_move'], pct_go))
                    config['min_pct_move'] = pct_go
        return config

    def update_hedge_money(self, path, path_own, hedge_path, money): 
        '''
        当对冲路线的交易所a的钱不足的时候，可以不用考虑2倍money_move_one的问题
        '''
        where = { "path" : path }
        config = self.db['hedge_money_config'].find_one(where)
        if config is None:
            logging.debug("还没有进行对冲过")
            config = {
                'path' : path,
                "hedge_money" : {path_own : money , hedge_path : 0}
            }
            self.db['hedge_money_config'].insert_one(config)
        else:
            logging.debug("目前对冲的资金量为%s" % config)
            config['hedge_money'][path_own] += money
            self.db['hedge_money_config'].update_one(where, { '$set':config }, upsert = True)
    
    def check_hedge_money(self, path, path_own, hedge_path, money): 
        '''
        一般情况下进行环路对冲需要两个方向对冲量在2倍money_move_one之内
        '''
        where = { "path" : path }
        config = self.db['hedge_money_config'].find_one(where)
        if config is None:
            # logging.debug("还没有进行对冲过")
            # config = {
            #     'path' : path,
            #     "hedge_money" : {path_own : money , hedge_path : 0}
            # }
            # self.db['hedge_money_config'].insert_one(config)
            return True
        # logging.debug("目前对冲的资金量为%s" % config)
        if config['hedge_money'][path_own] - config['hedge_money'][hedge_path] < money * 2:
            # config['hedge_money'][path_own] += money
            # self.db['hedge_money_config'].update_one(where, { '$set':config }, upsert = True)
            return True
        return False

    def save_moveinfo_contract(self, type_ ,moveinfo):
        #自动加索引
        colname = "move_info_%s_%s_%s" % (moveinfo['pma'], moveinfo['coin_a2b'], moveinfo['pmb'])
        if colname not in self.moveinfo_indexes:
            self.db[colname].ensure_index([ ('time_str', DESCENDING), ("move_path", DESCENDING) ])
            self.db[colname].ensure_index([ ("time", -1), ("quote_a", 1), ("quote_b", 1), ("pct", -1) ])
            self.db[colname].ensure_index( [ ("op_id", DESCENDING)])
            self.moveinfo_indexes[colname] = 1
            logging.debug("自动索引[%s]" % colname)
        if type_ == True:
            self.db[colname].insert_one(moveinfo)
        else:
            where = { "op_id":moveinfo['op_id'] }
            self.db[colname].update_one(where, { '$set':moveinfo }, upsert = True)
    
    def deal_message(self, results):
        '''
        处理报警
        '''
        if len(results) ==0:
            pass
        else:
            for res in results:
                msg = res['msg']
                title = res['title']
                is_lack_coin = res['is_lack_coin']
                if is_lack_coin == True:
                    hedging_notice(msg, title, 3)
                else:
                    hedging_notice(msg, title, 2)
                # notice_by_dingding(msg,title = title, must = True,sleepTime = 0, is_lack_coin = is_lack_coin)
                time.sleep(0.1)

    def save_noticeresult(self, results):
        if len(results)==0:
            pass 
        else:
            for res in results:
                self.db['notice_result'].insert_one(res)

    def save_moveinfo(self, type_ , moveinfo):
        #自动加索引
        colname = "move_info_%s" % moveinfo["path"]
        if colname not in self.moveinfo_indexes:
            self.db[colname].ensure_index([ ('time_str', DESCENDING), ("path", DESCENDING) ])
            self.db[colname].ensure_index([ ("time", -1), ("path", DESCENDING) ])
            self.db[colname].ensure_index( [ ("op_id", DESCENDING)])
            self.moveinfo_indexes[colname] = 1
            logging.debug("自动索引[%s]" % colname)
        if type_ == True:
            self.db[colname].insert_one(moveinfo)
        else:
            where = { "op_id" : moveinfo['op_id'] }
            self.db[colname].update_one(where, { '$set':moveinfo }, upsert = True)

    def get_karken_usdt_huilv(self):
        '''
        获取karken滑动平均数据 最后结果为1个usd等于多少个usdt
        '''
        data = self.karken_db['usdt_ma_1'].find_one({}, sort = [("time",-1)] )
        if (time.time() - data['time'] / 1000.0 > 300 ):
            return None 
        return 1 / data['usdt_avg']
    def find_new_hedge_pct(self,hedge_path):
        '''
        从moveinfo表中查询最新的数据
        '''
        result = {}
        column = "move_info_%s" % hedge_path
        pma, _, pmb, _ = re.split(r'[_-]', hedge_path)
        end_time = time.time() 
        where = {'time' : { "$gt":end_time - 5, "$lt": end_time} }
        move_info = self.db[column].find_one(where, sort = [("time", pymongo.DESCENDING)])
        if move_info is None:
            logging.info("表[%s]下路径[%s]3秒内没有及时更新" % (column, hedge_path))
            return None
        else:
            if  move_info['coins'] == {}:
                return None
            for key,value in  move_info['coins'].items():
                lockfile_a= "%s_%s_%s" % (LOCK_ERROR, pma, key)
                lockfile_b= "%s_%s_%s" % (LOCK_ERROR, pmb, key)
                if not (os.path.exists(lockfile_a) or os.path.exists(lockfile_b)):
                    result[key] = {"pct" : value['loop']['pct'], "amount" : value['loop']['amount']}
            if result ==  {}:
                return None
            return result

    
    def find_hedge_total_coin(self, hedge_path, coin, trade_type_a, trade_type_b, huilv, time_interval = 100):
        '''
        获取某个交易所某种币的数量
        '''
        where = {"update_time":{"$gt":time.time() - 2*time_interval}}
        hedge_coin = 0
        hedge_money = 0
        pma, quote_a, pmb, quote_b = re.split(r'[_-]',hedge_path)
        balance_detail = self.db['exchange_balance_detail'].find_one(where, sort=[("update_time", DESCENDING)])
        if balance_detail is None:
            logging.warning("获取账户详细信息失败 colname[exchange_balance_detail] where:%s time_interval:%s" % (where, time_interval))
            return None
        if pmb not in balance_detail:
            logging.warning("获取账户详细信息失败 获取的账户详情中缺少平台[%s]的数据" % pmb)
            return None
        if pma not in balance_detail:
            logging.warning("获取账户详细信息失败 获取的账户详情中缺少平台[%s]的数据" % pma)
            return None
        if trade_type_a == TRADE_TYPE_MARGIN:
            for _, tmp_balance in balance_detail[pma]['margin'].items():
                if coin in tmp_balance and quote_a in tmp_balance:
                    hedge_money = float(tmp_balance[quote_a] / huilv)
                    break
        else :
            if coin in balance_detail[pma]['spot'] :
                hedge_money = float(balance_detail[pma]['spot'][quote_a] / huilv)
            else:
                hedge_money = 0
        if trade_type_b == TRADE_TYPE_MARGIN:
            for _, tmp_balance in balance_detail[pmb]['margin'].items():
                if coin in tmp_balance and quote_b in tmp_balance:
                    hedge_coin = float(tmp_balance[coin])
                    break
        else :
            if coin in balance_detail[pmb]['spot'] :
                hedge_coin = float(balance_detail[pmb]['spot'][coin])
            else:
                hedge_coin = 0
        return  [hedge_coin,hedge_money]
        

    
    def save_moveresult(self, moveresult):
        '''
        保存对冲订单结果
        '''
        if 'time' not in moveresult:
            moveresult['time'] = time.time()
        self.db["move_result"].insert_one(moveresult)
    
    def save_moveresult_error(self, moveresult):
        '''
        保存失败对冲订单结果
        '''
        if 'time' not in moveresult:
            moveresult['time'] = time.time()
        self.db["move_result_error"].insert_one(moveresult)

    def update_moveresult(self, moveresult):
        '''
        更新对冲订单信息。在获取到了定价的价格后，会执行这步操作
        '''
        where = {}
        if '_id' in moveresult:
            where['_id'] = moveresult['_id']
            del moveresult['_id']
        else:
            where['op_id'] = moveresult['op_id']
            where['market_name'] = moveresult['market_name']
        self.db['move_result'].update_one(where, {'$set':moveresult} )

    def get_moveresult_noprice(self, market_name):
        '''
        查询没有成交价的订单
        params:
            market_name 指定的交易所
        self.db['move_result'].ensure_index([('market_name', DESCENDING), ('price_finaly' ,DESCENDING)])
        return :
            [
                ... order_info
            ]
        '''
        orders = []
        where = { 'market_name':market_name, 'order_type':'market', 'price_finaly' : {"$lt" : 0.0001}, 'time' : {"$gt" : time.time() - 60 * 30} }
        cursor = self.db['move_result'].find(where)
        for p in cursor:
            orders.append(p)
        return orders

    def get_balance_info(self, coins, pma, quote_a, huilv_quote_a, flag_pma_margin_dict, pmb, quote_b, huilv_quote_b, flag_pmb_margin_dict, time_interval = 100):
        '''
        获取到当前的全部账户详情：
            账户明细信息来自exchange_balance_detail表
            账户总体信息来自exchange_balance_total表
        返回 
            {
                "money_uni":5000000.,
                "coin_amount": 100., #self.coin_a2b的总量
                "huobi": #pma
                    {
                        'xrp':{"coin_amount":50,"money_org":10000,"money":650000},
                        'btc':{"coin_amount":50,"money_org":10000,"money":650000}
                    },
                "bitbank":#pmb
                    {
                        'xrp':{"coin_amount":50,"money_org":10000,"money":650000},
                        'btc':{"coin_amount":50,"money_org":10000,"money":650000}
                    },
                "frozen":
                    {
                        "huobi": #pma
                            {
                            'xrp':{"coin_amount":50,"money_org":10000,"money":650000},
                                'btc':{"coin_amount":50,"money_org":10000,"money":650000}
                            }
                    }
            }
        '''

        where = {"update_time" : {"$gt" : time.time() - 6 * time_interval}}
        balance_detail = self.db['exchange_balance_detail'].find_one(where, sort = [("update_time", DESCENDING)])
        if balance_detail is None:
            logging.warning("获取账户详细信息失败 colname[exchange_balance_detail] where:%s time_interval:%s" % (where, 6 * time_interval))
            return None
        if pma not in balance_detail:
            logging.warning("获取账户详细信息失败 获取的账户详情中缺少平台[%s]的数据" % pma)
            return None
        if pmb not in balance_detail:
            logging.warning("获取账户详细信息失败 获取的账户详情中缺少平台[%s]的数据" % pmb)
            return None
        # logging.debug("获取账户详细信息:%s" % balance_detail)
        del balance_detail['_id']

        balanceinfo = { pma : {}, pmb : {},"frozen" : {pma : {}, pmb : {}} }
        for coin in coins:
            for (pm, trade_type,  quote, huilv) in [(pma, flag_pma_margin_dict[coin], quote_a, huilv_quote_a), (pmb, flag_pmb_margin_dict[coin], quote_b, huilv_quote_b)]:
                if trade_type == TRADE_TYPE_MARGIN:
                    balanceinfo[pm][coin] = {
                                "coin_amount":0,
                                "money_org":0,
                                "money" : 0
                            }
                    balanceinfo["frozen"][pm][coin] = {
                            "coin_amount":0,
                            "money_org":0,
                            "money" : 0
                        }
                    for _, tmp_balance in balance_detail[pm]['margin'].items():
                        if coin in tmp_balance and quote in tmp_balance:
                            balanceinfo[pm][coin] = {
                                "coin_amount":float(tmp_balance[coin]),
                                "money_org":float(tmp_balance[quote]),
                                "money" : float(tmp_balance[quote] / huilv)
                            }
                            break
                    for _, tmp_balance in balance_detail[pm]['margin_frozen'].items():
                        if coin in tmp_balance and quote in tmp_balance:
                            balanceinfo["frozen"][pm][coin] = {
                                "coin_amount":float(tmp_balance[coin]),
                                "money_org":float(tmp_balance[quote]),
                                "money" : float(tmp_balance[quote] / huilv)
                            }
                            break
                    if pm not in balanceinfo:
                        logging.debug("获取期货账户详细信息失败，  pm[%s]-balanceinfo:%s" % (pm, balance_detail[pm]))
                        return None
                    else:
                        logging.debug("整理平台[%s]的期货账户信息:%s" % (pm, balanceinfo[pm]))
                elif trade_type == TRADE_TYPE_SPOT:
                    balanceinfo[pm][coin] = {
                        "coin_amount" : float(balance_detail[pm]['spot'].get(coin,0)),
                        "money_org" : float(balance_detail[pm]['spot'].get(quote, 0)),
                        "money" : float(balance_detail[pm]['spot'].get(quote, 0) / huilv)
                    }
                    logging.debug("现货存在frozen，取出来")
                    balanceinfo['frozen'][pm][coin] = {
                        "coin_amount" : float(balance_detail[pm]['spot_frozen'].get(coin,0)),
                        "money_org" : float(balance_detail[pm]['spot_frozen'].get(quote,0)),
                        "money" : float(balance_detail[pm]['spot_frozen'].get(quote,0) / huilv)
                        }
                    logging.debug("整理平台[%s]的现货账户信息:%s" % (pm, balanceinfo[pm]))
                else:
                    for _, tmp_balance in balance_detail[pm]['future'].items():
                        if coin in tmp_balance and quote in tmp_balance:
                            key = coin + "_available"
                            balanceinfo[pm][coin] = {
                                "total_coin" : float(tmp_balance[coin]),
                                "coin_amount":float(tmp_balance[key]) * 10,
                                "money_org":0,
                                "money" : 0
                            }
                            break
                    for _, tmp_balance in balance_detail[pm]['future_frozen'].items():
                        if coin in tmp_balance and quote in tmp_balance:
                            balanceinfo["frozen"][pm][coin] = {
                                "coin_amount": float(tmp_balance[coin]),
                                "money_org": float(tmp_balance[quote]),
                                "money" : float(tmp_balance[quote]),
                                "contract_detail" : tmp_balance['contract_detail'] if 'contract_detail' in tmp_balance.keys() else {}
                            }
                            break
                    if pm not in balanceinfo:
                        logging.debug("获取期货账户详细信息失败.pm[%s]-balanceinfo:%s" % (pm, balance_detail[pm]))
                        return None
                    else:
                        logging.debug("整理平台[%s]的期货账户信息:%s" % (pm, balanceinfo[pm]))

        where_total = {"update_time":{"$gt":time.time() -  3 * self.max_interval}}
        balance_total = self.db['exchange_balance_total'].find_one(where_total, sort = [("update_time", DESCENDING)])
        if balance_total is None:
            logging.warning("获取账户详细信息失败 获取或生产balance_total失败 where:%s" % where_total)
            return None
        else:
            # logging.debug("获取到balance_total:%s" % balance_total)
            pass
        balanceinfo['money_uni'] = float(balance_total['money_cny'])
        balanceinfo['coin_amount'] = {}
        for coin in coins:
            balanceinfo['coin_amount'][coin] = float(balance_total['coins'].get(coin, 0.0))
        
        logging.debug("获取得到账户信息:%s" % balanceinfo)
        return balanceinfo

    def delete_warning(self,path):
        where ={"path" : path}
        future_info = self.db['warning_of_blowing'].find_one(where, sort=[("update_time", DESCENDING)])
        if future_info:
            self.db['warning_of_blowing'].delete_one(where)

    def update_warning(self, future_balance):
        where ={"path" : future_balance['path']}
        future_info = self.db['warning_of_blowing'].find_one(where, sort=[("update_time", DESCENDING)])
        if future_info is None:
            self.db['warning_of_blowing'].insert(future_balance)
        else:
            if future_balance['trade_type'] == 'buy':
                if future_balance['price'] < future_info['price']:
                    self.db['warning_of_blowing'].update_one(where, {"$set":future_balance})
            else:
                if future_balance['price'] > future_info['price']:
                    self.db['warning_of_blowing'].update_one(where, {"$set":future_balance})

    def update_balance(self, balance_updates):
        '''
            交易后，立即更新账户的币和钱的数量
        '''
        where = {"update_time":{"$gt":time.time() - self.max_interval * 2}}
        balance_detail = self.db['exchange_balance_detail'].find_one(where, sort=[("update_time", DESCENDING)])
        if balance_detail is None:
            logging.warning("更新账户信息时，获取账户信息失败 创建一个")
            return False
        else:
            balance_detail['op'] = "server_%s" % time.strftime("%Y-%m-%dT%H:%M%S", time.localtime())
        where = {"_id" : balance_detail['_id']}
        for balance in balance_updates:
            logging.debug("更新账户信息为：%s" % balance)
            pm = balance['pm']
            coin_name = balance['coin_name']
            coin_num = balance['coin_num']
            quote_name = balance['quote_name']
            quote_num = balance['quote_num']
            symbol = "%s%s" % (coin_name, quote_name)
            flag_pm_margin = balance['flag_pm_margin']
            if pm not in balance_detail:
                logging.warning("更新账户信息时，发现不存在平台[%s]的信息 忽略" %  pm)
                continue
            else:
                logging.debug("更新账户信息 pm[%s] org-info:%s" % (pm, balance_detail[pm]))
            if flag_pm_margin == TRADE_TYPE_MARGIN:
                if symbol not in balance_detail[pm]['margin'] or coin_name not in balance_detail[pm]['margin'][symbol]:
                    logging.warning("更新账户信息时，发现期货账户没有symbol[%s]或没有币[%s] info-org:%s" % (symbol, coin_name, balance_detail[pm]['margin']))
                    continue
                balance_detail[pm]['margin'][symbol][coin_name] = coin_num
                balance_detail[pm]['margin'][symbol][quote_name] = quote_num
                logging.info("更新期货账户: %s" % balance_detail[pm]['margin'][symbol])
            elif flag_pm_margin == TRADE_TYPE_SPOT:
                balance_detail[pm]['spot'][coin_name] = coin_num
                balance_detail[pm]['spot'][quote_name] = quote_num
                logging.info("更新现货账户: %s" % balance_detail[pm]['spot'])
            else :
                if symbol not in balance_detail[pm]['future'] or coin_name not in balance_detail[pm]['future'][symbol]:
                    logging.warning("更新账户信息时，发现合约账户没有symbol[%s]或没有币[%s] info-org:%s" % (symbol, coin_name, balance_detail[pm]['future']))
                    continue
                key = coin_name + "_available"
                balance_detail[pm]['future'][symbol][key] = coin_num
                balance_detail[pm]['future'][symbol][quote_name] = quote_num
                balance_detail[pm]['future_frozen'][symbol][coin_name] = balance['future_coin_num']
                balance_detail[pm]['future_frozen'][symbol][quote_name] = balance['future_pm_money']
                balance_detail[pm]['future_frozen'][symbol]['contract_detail'] = balance['contract_detail']
                logging.debug("更新合约账户: %s" % balance_detail[pm]['future'][symbol])

            balance_detail[pm]['update_time'] = time.time()
        balance_detail['update_time'] = time.time()
        self.db['exchange_balance_detail'].update_one(where, {"$set":balance_detail})
        return True

    def update_balance_all(self, pm, balance_all, is_check = False):
        '''
        更新pm指定平台的所有的币的信息
        '''
        #where = {"update_time":{"$gt":time.time() - self.max_interval * 2}}
        where = {}
        updates = {}
        # date_updates = {}
        balance_detail = self.db['exchange_balance_detail'].find_one(where, sort=[("update_time", DESCENDING)])
        if balance_detail is None:
            logging.warning("更新账户信息时，获取账户信息失败 创建一个")
            balance_detail = {
                "_id":time.strftime("%Y-%m-%dT%H", time.localtime()),
            }
        if is_check:
            try:
                last_time = balance_detail[pm]['update_time']
            except:
                last_time = 0
            if time.time() - last_time <8:
                logging.debug("通过账户pm[%s]更新时间发现时间小于8s，不对账户进行更新" % pm)
                return True
        logging.debug("通过账户pm[%s]更新时间发现时间大于8s，对账户进行更新" % pm)
        logging.debug("更新账户信息 pm[%s] 更新前:%s" % (pm, balance_detail.get(pm,None)))
        # balance_detail[pm] = balance_all
        logging.debug("更新账户信息 pm[%s] 更新后:%s" % (pm, balance_all))
        # balance_detail['op'] = "server_%s" % time.strftime("%Y-%m-%dT%H:%M%S", time.localtime())
        # balance_detail['update_time'] = time.time()
        where = {"_id" : balance_detail['_id']}
        updates['op'] = "server_%s" % time.strftime("%Y-%m-%dT%H:%M%S", time.localtime())
        updates['update_time'] = time.time()
        updates[pm] = balance_all
        self.db['exchange_balance_detail'].update_one(where, {"$set" : updates}, upsert = True)
        return True

    def get_trading_volumn_info(self):
        '''
        获取两个交易所之间交易额
        '''
        trader_info = self.db['trading_column_info'].find_one()
        return trader_info

    def update_trading_volumn_info(self, path_trading_volume, path, trading_money):
        '''
        更新两个交易所之间的交易额
        '''
        if path_trading_volume is None:
            self.db['trading_column_info'].insert_one({"paths":{path :trading_money}})
        else:
            where = {"_id" :path_trading_volume['_id'] }
            path_trading_volume['paths'][path] = path_trading_volume['paths'].get(path, 0) + trading_money
            self.db['trading_column_info'].update_one(where, {"$set" : path_trading_volume}, upsert = True)

    def get_trader_info(self,is_contract = False):
        '''
        获取到最近的traderinfo
        '''
        #where = {"update_time":{"$gt":"%s" % (time.strftime("%Y-%m-%dT%H:%M", time.localtime(time.time() - 300)))}}
        where = {}
        if is_contract:
            trader_info =  self.db['trader_quick'].find_one(where, sort = [("_id", pymongo.DESCENDING)])
        else:
            trader_info =  self.db['trader_quick_new'].find_one(where, sort = [("_id", pymongo.DESCENDING)])
        if trader_info is None:
            logging.warning("查询自己账户失败 where:%s " % where)
        return trader_info
    
    def get_markets_balance(self,pma,pmb):
        '''
        获取每个market的最大最小钱数
        '''
        markets_balance = {}
        pma_balance =  self.db['exchange_usd_limit'].find_one({"_id":pma})
        if pma_balance is None:
            pma_balance = {'usd_max': 1000000,'usd_min': 0}
        markets_balance[pma] = pma_balance
        pmb_balance =  self.db['exchange_usd_limit'].find_one({"_id":pmb})
        if pmb_balance is None:
            pmb_balance = {'usd_max': 1000000,'usd_min': 0}
        markets_balance[pmb] = pmb_balance
        return markets_balance


    def save_trade_cmd(self, cmd_trade):
        '''
        保存模拟器的指令
        params:
            cmd_trade:{
                'side' : side,
                'amount' : amount,
                'price' : price,
                'market' : self.market_name,
                'time' : cmd_time,
                'order_type' : order_type,
                'trade_type' : trade_type,
                'coin' : self.apiclient.coin,
                'quote' : self.apiclient.quote,
                'symbol' : self.apiclient.symbol
            }
        return :
            None
        '''
        self.db['monitor_cmd_trade'].insert_one(cmd_trade)
        
    def get_trade_cmd_result(self, market_name = None, symbol = None, time = None):
        '''
        获取模拟器指令的执行结果
        '''
        where = { 'market':market_name, 'symbol':symbol, 'time':time }
        return self.db['monitor_cmd_trade'].find_one(where)
    
    def get_trade_cmd(self, market_name):
        """
        获取交易指令
        """
        try:
            where = {'market':market_name, 'run_status' : 0}
            res = self.db['monitor_cmd_trade'].find_one_and_update(where, {'$set' : {'run_status' : 1}})
            if res is None:
                return None
            res['run_status'] = 1
            return res
        except Exception as e:
            logging.warning(u"获取交易指令失败，market:%s msg:%s" % (market_name, e.message))
        return None
    
    def update_trade_cmd(self, cmd):
        """
        更新交易指令
        """
        try:
            where = {'_id':cmd['_id']}
            return self.db['monitor_cmd_trade'].update(where, cmd, True)
        except Exception as e:
            logging.warning(u"更新交易指令，msg:%s" % e.message)
            raise Exception(u"更新交易指令，msg:%s" % e.message)
        return None
    
    def update_balance_new(self, balance_updates):
        '''
            交易后，立即更新账户的币和钱的数量
        '''
        where = {"update_time":{"$gt":time.time() - self.max_interval * 2}}
        balance_detail = self.db['exchange_balance_detail'].find_one(where, sort=[("update_time", DESCENDING)])
        if balance_detail is None:
            logging.warning("更新账户信息时，获取账户信息失败 创建一个")
            return False
        else:
            balance_detail['op'] = "server_%s" % time.strftime("%Y-%m-%dT%H:%M%S", time.localtime())
        where = {"_id" : balance_detail['_id']}
        updates = {}
        date_updates = {}
        for balance in balance_updates:
            logging.debug("更新账户信息为：%s" % balance)
            pm = balance['pm']
            trans_num = balance['trans_num']
            coin_name = balance['coin_name']
            flag_pm_margin = balance['flag_pm_margin']
            if pm not in balance_detail:
                logging.warning("更新账户信息时，发现不存在平台[%s]的信息 忽略" %  pm)
                continue
            else:
                pass
                # logging.debug("更新账户信息 pm[%s] org-info:%s" % (pm, balance_detail[pm]))
            if flag_pm_margin == TRADE_TYPE_MARGIN:
                symbol ='%susdt' % coin_name
                if symbol not in balance_detail[pm]['margin'] or coin_name not in balance_detail[pm]['margin'][symbol]:
                    logging.warning("更新账户信息时，发现期货账户没有symbol[%s]或没有币[%s] info-org:%s" % (symbol, coin_name, balance_detail[pm]['margin']))
                    continue
                if trans_num is not None:
                    updates['%s.margin.%s.%s' % (pm, symbol, coin_name)] = trans_num
                # logging.debug("更新期货账户: %s" % balance_detail[pm]['margin'][symbol])
            elif flag_pm_margin == TRADE_TYPE_SPOT:
                if trans_num is not None:
                    updates['%s.spot.%s' % (pm, coin_name)] = trans_num
                # logging.debug("更新现货账户: %s" % balance_detail[pm]['spot'])
            else :
                symbol = "%susd" % coin_name
                if symbol not in balance_detail[pm]['future'] or coin_name not in balance_detail[pm]['future'][symbol]:
                    logging.warning("更新账户信息时，发现合约账户没有symbol[%s]或没有币[%s] info-org:%s" % (symbol, coin_name, balance_detail[pm]['future']))
                    continue
                key = coin_name + "_available"
                if trans_num is not None:
                    updates['%s.future.%s.%s' % (pm, symbol, key)] = trans_num
                    updates['%s.future.%s.%s' % (pm, symbol, coin_name)] = trans_num
                if balance.get('future_coin_num', None) is not None:
                    updates['%s.future_frozen.%s.%s' % (pm, symbol, coin_name)] = balance['future_coin_num']
                if balance.get('contract_detail', None) is not None:
                    updates['%s.future_frozen.%s.contract_detail' % (pm, symbol)] = balance['contract_detail']
                # logging.debug("更新合约账户: %s" % balance_detail[pm]['future'][symbol])
            date_updates['%s.update_time' % pm] = time.time()
        date_updates['update_time'] = time.time()
        self.db['exchange_balance_detail'].update_one(where, {"$inc":updates, '$set':date_updates})
        return True

    def update_balance_inc(self, balance_updates):
        '''
        通过inc更新资产不需要获取资产信息
        '''
        where = {"update_time":{"$gt":time.time() - self.max_interval * 2}}
        balance_detail = self.db['exchange_balance_detail'].find_one(where, sort=[("update_time", DESCENDING)])
        if balance_detail is None:
            logging.warning("更新账户信息时，获取账户信息失败 创建一个")
            return False
        else:
            balance_detail['op'] = "server_%s" % time.strftime("%Y-%m-%dT%H:%M%S", time.localtime())
        where = {"_id" : balance_detail['_id']}
        updates = {}
        date_updates = {}
        for balance in balance_updates:
            logging.debug("更新账户信息为：%s" % balance)
            pm = balance['pm']
            coin_name = balance['coin_name']
            coin_num = balance.get('coin_num', None)
            quote_name = balance.get('quote_name', None)
            quote_num = balance.get('quote_num', None)
            symbol = "%s%s" % (coin_name, quote_name)
            flag_pm_margin = balance['flag_pm_margin']
            if pm not in balance_detail:
                logging.warning("更新账户信息时，发现不存在平台[%s]的信息 忽略" %  pm)
                continue
            else:
                logging.debug("更新账户信息 pm[%s] org-info:%s" % (pm, balance_detail[pm]))
            if flag_pm_margin == TRADE_TYPE_MARGIN:
                if symbol not in balance_detail[pm]['margin'] or coin_name not in balance_detail[pm]['margin'][symbol]:
                    logging.warning("更新账户信息时，发现期货账户没有symbol[%s]或没有币[%s] info-org:%s" % (symbol, coin_name, balance_detail[pm]['margin']))
                    continue
                if coin_num is not None:
                    updates['%s.margin.%s.%s' % (pm, symbol, coin_name)] = coin_num
                if quote_num is not None:
                    updates['%s.margin.%s.%s' % (pm, symbol, quote_name)] = quote_num
                logging.debug("更新期货账户: %s" % balance_detail[pm]['margin'][symbol])
            elif flag_pm_margin == TRADE_TYPE_MARGIN + '_frozen':
                if symbol not in balance_detail[pm]['margin_frozen'] or coin_name not in balance_detail[pm]['margin'][symbol]:
                    logging.warning("更新账户信息时，发现期货冻结账户没有symbol[%s]或没有币[%s] info-org:%s" % (symbol, coin_name, balance_detail[pm]['margin']))
                    continue
                if coin_num is not None:
                    updates['%s.margin_frozen.%s.%s' % (pm, symbol, coin_name)] = coin_num
                if quote_num is not None:
                    updates['%s.margin_frozen.%s.%s' % (pm, symbol, quote_name)] = quote_num
                logging.debug("更新期货账户: %s" % balance_detail[pm]['margin'][symbol])
            elif flag_pm_margin == TRADE_TYPE_SPOT:
                if coin_num is not None:
                    updates['%s.spot.%s' % (pm, coin_name)] = coin_num
                if quote_num is not None:
                    updates['%s.spot.%s' % (pm, quote_name)] = quote_num
                logging.debug("更新现货账户: %s" % balance_detail[pm]['spot'])
            elif flag_pm_margin == TRADE_TYPE_SPOT + '_frozen':
                if coin_num is not None:
                    updates['%s.spot_frozen.%s' % (pm, coin_name)] = coin_num
                if quote_num is not None:
                    updates['%s.spot_frozen.%s' % (pm, quote_name)] = quote_num
                logging.debug("更新现货冻结资产账户: %s" % balance_detail[pm]['spot_frozen'])
            else :
                if symbol not in balance_detail[pm]['future'] or coin_name not in balance_detail[pm]['future'][symbol]:
                    logging.warning("更新账户信息时，发现合约账户没有symbol[%s]或没有币[%s] info-org:%s" % (symbol, coin_name, balance_detail[pm]['future']))
                    continue
                key = coin_name + "_available"
                if coin_num is not None:
                    updates['%s.future.%s.%s' % (pm, symbol, key)] = coin_num
                if quote_num is not None:
                    updates['%s.future.%s.%s' % (pm, symbol, quote_name)] = quote_num
                if balance.get('future_coin_num', None) is not None:
                    updates['%s.future_frozen.%s.%s' % (pm, symbol, coin_name)] = balance['future_coin_num']
                if balance.get('future_pm_money', None) is not None:
                    updates['%s.future_frozen.%s.%s' % (pm, symbol, quote_name)] = balance['future_pm_money']
                if balance.get('contract_detail', None) is not None:
                    updates['%s.future_frozen.%s.contract_detail' % (pm, symbol)] = balance['contract_detail']
                logging.debug("更新合约账户: %s" % balance_detail[pm]['future'][symbol])
            date_updates['%s.update_time' % pm] = time.time()
        date_updates['update_time'] = time.time()
        self.db['exchange_balance_detail'].update_one(where, {"$inc":updates, '$set':date_updates})
        return True

    def get_dealer_cmd(self, future_spot = 'future'):
        """
        获取dealer指令
        return {
            'cmd_id' : xxx,
            'exchange': 'huobi',
            'coin' : 'xxx',
            'side' : 'sell/buy',
            'amount' : xxx
        }
        """
        try:
            where = {}
            if future_spot == 'future':
                where = {'price_exchange':{'$exists':False}}
            else:
                where = {'price_spot':{'$exists':False},'price_future':{'$exists':False}}
            return self.db['cmd_hold_token'].find_one(where)
        except:
            logging.error("获取dealer指令异常：%s" % traceback.format_exc())
    
    def update_dealer_cmd(self, cmd, future_spot = 'future'):
        """
        更新dealer指令
        return True/False
        """
        try:
            updates = {'msg':cmd.get('msg',None)}
            if future_spot == 'future':
                updates['price_spot'] = cmd['price_spot']
                updates['price_future'] = cmd['price_future']
                updates['amount_spot'] = cmd['amount_spot']
                updates['amount_future'] = cmd['amount_future']
            else:
                updates['price_exchange'] = cmd['price_exchange']
                updates['amount_exchange'] = cmd['amount_exchange']
            self.db['cmd_hold_token'].update_one({'_id':cmd['_id']}, {'$set':updates})
            return True
        except:
            logging.error("获取dealer指令异常：%s" % traceback.format_exc())
        return False

    def save_transe_record(self, market, coin, amount):
        """
        将转账记录保存在redis中
        """
        cost = {
            'huobi' : 30 * 60,
            'bithumb' : 40 * 60,
            'bitbank' : 60 * 60
        }
        key = "%s_check_risk_%s_%s" % (SERVER_NAME, coin, time.time() * 1000)
        self.dbhuilv.redis.set(key, str(amount))
        self.dbhuilv.redis.expire(key, int(cost.get(market, 30 * 60)))
        return key
    
    def remove_transe_record(self, key):
        """
        转账到账后删除redis记录
        """
        self.dbhuilv.redis.delete(key)

    def get_transe_record(self, cion):
        """
        查询redis中的转账记录
        """
        keys = self.dbhuilv.redis.keys("%s_check_risk_%s*" % (SERVER_NAME, cion))
        if len(keys) == 0:
            return 0.0
        values = self.dbhuilv.redis.mget(keys)
        return sum(map(float, values))
