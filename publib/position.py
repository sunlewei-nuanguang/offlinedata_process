# coding:utf-8
# author:guoxiangchao
import pymongo
import logging
import traceback
import datetime, time
import time,json,re,copy,os,sys
from pymongo import MongoClient, DESCENDING
from conf.publib_conf import CONN_ADDR_FUNDINFO, USERNAME_FUNDINFO, PWD_FUNDINFO
from conf.publib_conf import LOCK_ERROR
from bson.son import SON
from bson import ObjectId
from decimal import Decimal


'''
自动化仓位管理数据类库：
        交易所信息
        取款指令
        借款指令
        更新指令状态
        仓位平衡指令
        杠杆信息
        更新账户信息
        自动化更新初始值
'''

class DbPosition:
    '''
    自动化仓位管理数据类库：
        交易所信息
        取款指令
        借款指令
        更新指令状态
        仓位平衡指令
        杠杆信息
        更新账户信息
        自动化更新初始值
    '''
    def __init__(self):
        self.client_fundinfo = MongoClient(CONN_ADDR_FUNDINFO)
        if USERNAME_FUNDINFO is not None:
            self.client_fundinfo.admin.authenticate(USERNAME_FUNDINFO, PWD_FUNDINFO)
        self.db = self.client_fundinfo.get_database("fundinfo")

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
        except Exception,e:
            logging.warning("显式调用释放数据等连接资源异常:%s" % e)
            pass 

    def get_exchenge_info(self, market):
        '''
        获取交易所的基础信息
        返回
        {
            "_id" : ObjectId("5b1bd4e2c40788a007a4b9c2"),
            "name" : "bitbank",
            "account_ids" : [ 
                {
                    "id" : "0",
                    "symbol" : "0"
                }
            ],
            "coins" : [ 
                {
                    "name" : "jpy"
                }, 
                {
                    "name" : "xrp",
                    "address" : "rw7m3CtVHwGSdhFjV4MyJozmZJv3DYQnsA",
                    "tag" : "109130326",
                    "fee" : 0.1,
                    "min" : 0.1
                }
            ],
            "status" : NumberLong(1)
        }
        '''
        try:
            info = self.db['exchange_info'].find_one({'name': market})
            if info is None:
                logging.info("未找到%s交易所基础信息" % market)
            return info
        except Exception,e:
            logging.info("获取%s交易所基础信息异常,%s" % (market, traceback.format_exc()))
        return None

    def get_repay_order(self, market):
        """
        获取款指令
        """
        try:
            return self.db['margin_order'].find_one({'name':market,'status':1, 'type' : 'repay'})
        except Exception,e:
            logging.error("获取转账指令异常，%s" % traceback.format_exc())
    
    def get_auto_margin_order(self, market):
        """
        获取借款指令
        """
        try:
            cmd = self.db['margin_order'].find_one({'name':market,'status':1, 'type' : 'auto_margin'})
            if cmd is None:
                return
            last = self.db['margin_order'].find_one({'name':market, 'symbol' : cmd['symbol'], 'status' : 3})
            if last is None or time.time() - last['create_time'] > 60 * 10:
                return cmd
        except Exception,e:
            logging.error("获取自动借款指令异常，%s" % traceback.format_exc())
    
    def get_auto_margin_orders(self, market):
        """
        获取借款指令
        """
        #删除十分钟钱的借币请求
        try:
            self.db['margin_order_catch'].remove({'create_time' : {'$lt':time.time() - 10 * 60}})
        except Exception as e:
            pass
        try:
            pipeline = [
                {"$match" : {"create_time" : {"$gte" : time.time() - 3 * 60}}},
                {"$group" : {"_id" : {"name" : "$name", "coin" : "$coin", "symbol" : "$symbol"}, "count" : {"$sum" : 1}}},
                {"$sort" : SON([("count", -1)])}
            ]
            cursors = self.db['margin_order_catch'].aggregate(pipeline)
            cmds = []
            for cursor in cursors:
                if cursor['_id']['name'] == 'huobi' and cursor['count'] == 0:
                    continue
                if cursor['_id']['name'] != 'huobi' and cursor['count'] < 3:
                    continue
                cmd = cursor['_id']
                cmd['type'] = "auto_margin"
                cmd['create_time'] = time.time()
                record = self.db['margin_order'].find_one({'name' : cmd['name'], 'coin' : cmd['coin'], 'type' : 'auto_margin'}, sort = [('create_time',-1)])
                #借款成功后三十分钟不再借款
                if cursor['_id']['name'] != 'huobi' and record is not None and record['status'] == 3 and time.time() - record['create_time'] < 30 * 60:
                    continue
                if cursor['_id']['name'] == 'huobi' and record is not None and record['status'] == 3 and time.time() - record['create_time'] < 5 * 60:
                    continue
                elif record is not None and record['status'] in [2, -1] and time.time() - record['create_time'] < 2 * 60:
                    continue
                cmds.append(cmd)
            return cmds
        except Exception,e:
            logging.error("获取自动借款指令异常，%s" % traceback.format_exc())
            return []

    def get_margin_order(self, market):
        """
        获取借款指令
        """
        try:
            return self.db['margin_order'].find_one({'name':market,'status':1, 'type' : 'margin'})
        except Exception,e:
            logging.error("获取还款指令异常，%s" % traceback.format_exc())

    def update_margin_order(self, cmd, where = None):
        """
        更新指令状态
        """
        try:
            if where is None and '_id' in cmd:
                where = {'_id':cmd['_id']}
            else:
                where = None
            cmd['finish_time'] = int(time.time())
            if where is None:
                self.db['margin_order'].insert(cmd)
            else:
                self.db['margin_order'].update(where, cmd, True)
        except Exception,e:
            logging.error("更新指令异常,cmd[%s] %s" % (cmd, traceback.format_exc()))
    
    def update_margin_order_inc(self, pm, coin, order_id, repay_num):
        """
        还款成功后，更新订单状态
        """
        try:
            where = {'name':pm, 'coin':coin, 'order_id':order_id}
            self.db['margin_order'].update(where, {'$inc':{'loan-left':float(repay_num)}})
        except Exception as e:
            logging.info("更新借款订单异常，%s" % traceback.format_exc())

    def get_balance_order(self, market = None):
        """
        获取仓位平衡指令
        """
        try:
            cmd = self.db['position_reblance_cmds'].find_one({'market': market,'status':1})
            if cmd is None:
                return None
            cmd['status'] = 2
            self.update_balance_order(cmd)
            return cmd
        except Exception,e:
            logging.error("获取平仓指令异常, %s" % traceback.format_exc())
    
    def get_balance_orders(self, market = None):
        """
        获取仓位平衡指令
        """
        try:
            cmds = []
            where = {'status' : 1}
            if market is not None:
                where['market'] = market
            for cmd in self.db['position_reblance_cmds'].find(where):
                cmds.append(cmd)
            return cmds
        except Exception,e:
            logging.error("获取平仓指令异常, %s" % traceback.format_exc())
        return cmds


    def update_balance_order(self, cmd):
        """
        更新平仓指令
        """
        try:
            cmd['finish_time'] = int(time.time())
            self.db['position_reblance_cmds'].update({'_id':cmd['_id']}, cmd)
        except Exception,e:
            logging.error("更新指令异常,cmd[%s] %s" % (cmd, traceback.format_exc()))

    def update_balance_margin(self, balance):
        """
        更新杠杆账户信息
        """
        try:
            balance['update_time'] = int(time.time())
            #balance['_id'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")
            #self.db['margin_balance'].update({'_id':balance['_id']},{"$set": {balance["symbol"]:balance}}, True)
            self.db['margin_balance'].update({'_id':balance['symbol']}, balance, True)
        except Exception,e:
            logging.error("更新杠杆账户信息失败,balance[%s] msg %s" % (balance, traceback.format_exc()))

    def get_warning_info(self,market_name):
        '''
        获取预警信息
        '''
        try:
            warning_info = self.db['warn_coin_level'].find_one({'_id' : market_name})
            for k,v in warning_info.items():
                if k in ['_id','update_time']:
                    continue
                for y,s in v.items():
                    try:
                        if s=='':
                            v[y] = 0.0
                            continue
                        v[y] = float(s)
                    except:
                        v[y] = 0
            return warning_info
        except Exception,e:
            return None

    def get_cmds_trans(self, market_name = None):
        """
        获取自动建仓指令
        """
        try:
            cmds = []
            where = {'status':1}
            if market_name is not None:
                where['from'] = market_name
            for cmd in self.db['position_cmds'].find(where):
                cmds.append(cmd)
            return cmds
        except Exception,e:
            logging.warning("获取自动减仓指令失败，market:%s msg:%s" %(market_name, e.message))
        return cmds
    
    def update_cmds_trans(self, item, status = 0, msg = '', other = {}, insert = False):
        """
        更新自动建仓指令状态
        item = {
            'from' : xxx,
            'to' : xxx,
            'coin' : 'xxx,
            'amount' : xxx,
            'address' : xxx,
            'tag' : xxx,
            'status' : 1
        }
        status 1准备建仓 2建仓中 3 建仓完成 0 -1建仓失败
        """
        try:
            other['status'] = status
            other['msg'] = msg
            if insert:
                self.db['position_cmds'].insert(item)
                return True
            self.db['position_cmds'].update({'_id':item['_id']},{'$set' : other})
            return True
        except Exception,e:
            logging.error(u"更新转账指令状态失败,item[%s] status[%s] msg[%s]" % (item, status,e.message))
            return False
    
    def send_cmd_trans(self, item):
        """
        插入转账指令
        item = {
            'from' : xxx,
            'to' : xxx,
            'coin' : 'xxx,
            'amount' : xxx,
            'address' : xxx,
            'tag' : xxx,
            'status' : 1
        }
        return item = {
            '_id' : xxx,成功后返回_id
            'from' : xxx,
            'to' : xxx,
            'coin' : 'xxx,
            'amount' : xxx,
            'address' : xxx,
            'tag' : xxx,
            'status' : 1 
        }
        """
        try:
            item.update({'status' : 1, 'create_time' : time.time()})
            self.db['position_cmds'].insert_one(item)
            return item
        except Exception as e:
            logging.error("发送转账指令异常，msg:%s" % traceback.format_exc())
            return item
            
    def get_cmd_trans(self, item):
        """
        查询转账指令的状态
        return item = {
            '_id' : xxx,
            'from' : xxx,
            'to' : xxx,
            'coin' : 'xxx,
            'amount' : xxx,
            'address' : xxx,
            'tag' : xxx,
            'status' : 1, #1准备建仓 2建仓中 3 建仓完成 0 -1建仓失败 -2查询异常，
            'msg' : 'empty'
        }
        """
        try:
            if '_id' not in item:
                return None
            item = self.db['position_cmds'].find_one({'_id' : item['_id']})
            return item
        except Exception as e:
            logging.error("查询转账指令异常，msg:%s" % traceback.format_exc())
            item['msg'] = e.message
            item['status'] = -2
            return item
    

    def update_balance_merge(self, item):
        """
        更新账户资产信息
        """
        try:
            where = {'_id' : datetime.datetime.now().strftime("%Y-%m-%dT%H")}
            self.db['exchange_balance_detail'].update(where, item, True)
        except Exception, e:
            print(e)
            try:
                time.sleep(10)
                self.__init__()
                print(u'mongodb重连成功')
            except:
                print(u'mongodb重连失败')

    def get_balance(self, start_time = None):
        """
        获取balance
        """
        if start_time is None:
            start_time = int(time.time()) - 5 * 60
        try:
            balance = self.db['exchange_balance_detail'].find_one({'update_time':{'$gte' : start_time}}, sort = [('update_time',-1)])
            return balance
        except Exception,e:
            logging.error(u"获取balance失败,msg[%s]" % e.message)
        return None
    
    def update_balance_total(self,item):
        """
        保存balance统计数据
        """
        try:
            _id = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")
            where = {'_id' : _id}
            item['update_time'] = int(time.time())
            self.db['exchange_balance_total'].update(where, item, True)
        except Exception,e:
            logging.error(u"保存balance_total失败,msg[%s]" % e.message)

    def get_balance_config(self,market_a = None, market_b = None):
        """
        获取自动转账配置数据
        """
        try:
            confs = []
            where = {'status':1}
            if market_a:
                where['market_a'] = market_a
            if market_b:
                where['market_b'] = market_b
            items = self.db['balance_config'].find(where)
            for item in items:
                confs.append(item)
            return confs
        except Exception,e:
            logging.error(u"保存balance_total失败,msg[%s]" % e.message)
        return {}

    def send_balance_cmds(self,cmd):
        """
        火币账户平衡
        """
        try:
            pre = self.db['position_reblance_cmds'].find_one({'src':'auto','symbol':cmd['symbol'], 'to':cmd['to'], 'status':{'$in': [1, 2]}})
            if pre is not None:
                logging.info("自动平衡账户指令未执行完毕,symbol[%s] from[%s] to[%s] status[%s]"% (pre['symbol'], pre['from'], pre['to'], pre['status']))
            else:
                cmd['src'] = 'auto'
                cmd['status'] = 1
                cmd['create_time'] = int(time.time())
                cmd['amount'] = round(cmd['amount'], 4)
                self.db['position_reblance_cmds'].insert(cmd)
        except Exception,e:
            logging.error(u"发送自动平账指令,msg[%s]" % e.message)
    
    def send_trans_cmds(self, cmd):
        """
        发送转账指令
        """
        try:
            pre = self.db['position_cmds'].find_one({'src':'auto','from':cmd['from'], 'to':cmd['to'], 'coin':cmd['coin'], 'status':{'$in': [1, 2], 'finish_time': {"$lte" : int(time.time()) - 20 * 60}}})
            if pre is not None:
                logging.info("自动平衡账户指令未执行完毕,from[%s] to[%s] to[%s] status[%s] finish_time[%s]"% (pre['from'], pre['to'], pre['coin'], pre['status'], pre['finish_time']))
            else:
                cmd['src'] = 'auto'
                cmd['status'] = 1
                cmd['create_time'] = int(time.time())
                cmd['amount'] = round(cmd['amount'], 4)
                self.db['position_cmds'].insert(cmd)
        except Exception,e:
            logging.error(u"发送自动转账指令,msg[%s]" % e.message)
    
    def get_coin_address(self, market, coin):
        try:
            info = self.db['exchange_info'].find_one({'name':market})
            for c in info['coins']:
                if c['name'] == coin:
                    return c
        except Exception,e:
            logging.error(u"获取配置信息失败,market[%s] coin[%s]" % (market, coin))
        return None

    def get_margin_out_info(self,market, symbol, coin):
        """
        获取杠杆账户的coin信息
        {
            coin:'xrp',
            'trad':0,
            'frozen':0,
            'load':0,
            'interest':0,
            'transfer-out-available':0,
            'loan-available':0
        }
        """
        try:
            info = self.db['margin_balance'].find_one({'name':market,'_id':symbol})
            res = {'coin': coin}
            for item in info['list']:
                if item['currency'] == coin:
                    res[item['type']] = float(item['balance'])
            return res
        except Exception,e:
            logging.error("获取杠杆信息失败，market[%s] symbol[%s] coin[%s] msg %s" % (market, symbol, coin, traceback.format_exc()))
        return None
    
    def get_ga_code(self, market_name):
        """
        获取30秒内的谷歌验证码
        {
            'name' : 'market_name',
            'code' : '333333'
        }
        """
        import publib.libs.pyotp as pyotp
        import importlib
        try:
            module = importlib.import_module('clients.conf.%s' % market_name)
            info = getattr(module, 'config_%s' % market_name)
            if info is None or 'ga' not in info:
                return None
            totp = pyotp.TOTP(info['ga'])
            while True:
                if time.localtime(time.time()).tm_sec % 30 > 1 and time.localtime(time.time()).tm_sec % 30 < 29:
                    return {'name' : market_name, 'code' : totp.now()}
        except:
            logging.error("获取谷歌验证码失效，market:%s" % market_name)
        return None
    
    def update_warn_coin_level_total(self, coin, amount, op = 'buy'):
        """
        自动换币完成后
        更新总资产的初始值
        """
        try:
            total = self.db['warn_coin_level'].find_one({'_id' : 'total'})
            update_time = None
            if total is None:
                logging.warning("更新总资产初始值失败，获取资产初始值失败")
                return False
            #没有的时候创建一个
            if coin not in total or total[coin]['init_amount'] == '':
                update_time = int(time.time())
                self.db['warn_coin_level'].update_one({'_id' : 'total'}, {
                    '$set':{
                        coin:{
                            "init_amount" : 0,
                            "max_loss" : -abs(float(amount)),
                            "max_inc" : abs(float(amount))
                        },'update_time':update_time}})
                num = 0
            else:
                num = float(total[coin]['init_amount'])
                update_time = total['update_time']
            logging.info("总资产初始值更新前，coin:%s amount:%s" % (coin, num))
            if op == 'buy':
                num = float(Decimal(str(num)) + Decimal(str(amount)))
            else:
                num = float(Decimal(str(num)) - Decimal(str(amount)))
            if num < 0:
                num = 0
            count = self.db['warn_coin_level'].update_one({'_id' : 'total', 'update_time' : update_time}, {'$set' : {"%s.init_amount" % coin : num, "update_time" : int(time.time())}})
            if count == 0:
                return self.update_warn_coin_level_total(coin, amount, op = op)
            logging.info("总资产初始值更新后，coin:%s amount:%s" % (coin, num))
            return True
        except Exception as e:
            logging.warning("更新总资产初始值异常，coin:%s amount:%s msg:%s" % (coin, amount, e.message))
            return False
    
    def get_balance_config_new(self, market_a = None, market_b = None):
        """
        通过balance_config和hedge_path组装自动转账规则
        路径近1个小时平均pct加权，按优先级执行
        """
        cmds = []
        balance_config = {}
        paths = []
        trader_info = None

        #获取一个小时路径的平均pct
        try:
            trader_info = self.db['trader_new'].find_one({}, sort = [('_id' , -1)])
        except Exception as e:
            logging.warning("获取自动转账计划，加权失败,msg:%s" % e.message)
        if trader_info is None:
            trader_info = {}
        
        path_cursors = []
        try:
            where = {}
            if market_a:
                where['pma'] = market_a
            if market_b:
                where['pmb'] = market_b
            for item in self.db['hedge_conf'].find(where):
                for coin in item['coins']:
                    path_cursors.append({
                        'pma' : item['pma'],
                        'quote_a' : item['quote_a'],
                        'pmb' : item['pmb'],
                        'quote_b' : item['quote_b'],
                        'token' : coin
                    })
        except Exception as e:
            logging.error("获取自动转账计划，路径失败，msg:%s" % e.message)
            return cmds
        for path in path_cursors:
            pm_key = '{0}_{1}'.format(*sorted([path['pma'], path['pmb']]))
            if pm_key not in balance_config:
                var_conf = self.db['balance_config_new'].find_one({'_id' : pm_key})
                if var_conf is None:
                    continue
                balance_config[pm_key] = var_conf
            if path['token'] not in balance_config[pm_key]:
                logging.debug("未设置阈值，pm_key:%s coin:%s" % (pm_key, path['token']))
                continue
            #计算转账方向bithumb_krw-huobi_usdt
            path_go = '%s_%s-%s_%s' % (path['pma'], path['quote_a'], path['pmb'], path['quote_b'])
            path_go_old = '%s-%s_%s_%s-%s' % (path['pma'], path['quote_a'], path['token'], path['pmb'], path['quote_b'])
            path_back = '%s_%s-%s_%s' % (path['pmb'], path['quote_b'], path['pma'], path['quote_a'])
            path_back_old = '%s-%s_%s_%s-%s' % (path['pmb'], path['quote_b'], path['token'], path['pma'], path['quote_a'])
            #有单条方向后不再查询计算方向
            step_min = 9999999999
            step_max = 9999999999
            if balance_config[pm_key][path['token']].replace(' ', '').replace('，',',') == '':
                continue
            steps = balance_config[pm_key][path['token']].replace(' ', '').replace('，',',').split(',')
            step_min = float(steps[0])
            if len(steps) > 1:
                step_max = float(steps[1])
            else:
                step_max = 2 * step_min
            if path_go + path['token'] in paths or path_back + path['token'] in paths or step_min <= 0:
                continue
            
            var_path = None
            var_path_old = None
            #先根据五分钟内的pct判断方向
            where = {
                "time" : {"$gte":time.time() - 10 * 60},
                "coins.%s" % path['token'] : {"$exists" : True},
                "$where" : "this.coins.{0} != undefined && (this.coins.{0}.pct >= this.coins.{0}.pct_td || this.coins.{0}.offset >= this.coins.{0}.pct_td)".format(path['token'])
            }
            flag_go = self.db['move_info_%s' % path_go].count(where)
            flag_back = self.db['move_info_%s' % path_back].count(where)
            logging.debug("根据moveinfo判断转账方向，flag_go:%s flag_back:%s" % (flag_go, flag_back))
            #根据pct没办法判断再通过四小时内交易判断方向
            if flag_go < 5 and flag_back < 5:
                flag_go = flag_back = 0
                logging.debug("通过move_result判断转账方向")
                cursors = []
                try:
                    where = {'time' : {'$gte' : time.time() - 12 * 60 * 60}, 'op_id' : {"$regex" : '^%s|^%s' % (path_go, path_back)}, 'coin' :  path['token']}
                    cursors = self.db['move_result'].find(where ,sort = [('time', -1)]).limit(5)
                except Exception as e:
                    logging.error("获取近五次交易失败，mes:%s" % e.message)
                for cur in cursors:
                    if path_go in cur['op_id']:
                        var_path = path_go if var_path is None else var_path
                        flag_go += 1
                    elif path_back in cur['op_id']:
                        var_path = path_back if var_path is None else var_path
                        flag_back += 1
                logging.debug("通过move_result判断转账方向，flag_go:%s flag_back:%s" % (flag_go, flag_back))
            cmd = {'step_min' : step_min,'step_max' : step_max, 'max_rate' : 0.25, 'min_rate' : 0.25, 'step' : 0.25, '_id' : pm_key}
            #组装cmd,以兼容之前的版本
            if flag_go > flag_back or (flag_go == flag_back and path_go == var_path):
                var_path = path_go
                var_path_old = path_go_old
                cmd['market_a'], cmd['market_b'], cmd['coin_a'], cmd['coin_b'], cmd['coin'] = path['pma'], path['pmb'], '%s/%s' % (path['token'].upper(), path['quote_a'].upper()) , '%s/%s' % (path['token'].upper(), path['quote_b'].upper()), path['token']
            elif flag_go < flag_back or (flag_go == flag_back and path_back == var_path):
                var_path = path_back
                var_path_old = path_back_old
                cmd['market_a'], cmd['market_b'], cmd['coin_a'], cmd['coin_b'], cmd['coin'] = path['pmb'], path['pma'], '%s/%s' % (path['token'].upper(), path['quote_b'].upper()) , '%s/%s' % (path['token'].upper(), path['quote_a'].upper()), path['token']
            else:
                logging.info("判断自动转账方向失败，flag_go:%s flag_back:%s path:%s" % (flag_go, flag_back, var_path))
                continue
            cmd['path'] = var_path
            cmd['pct'] = trader_info.get('paths',{}).get(var_path_old, {}).get(var_path_old, {}).get('last_hour', {}).get('pct', 0)
            cmds.append(cmd)
            paths.append(var_path + path['token'])
            #币币交易,qoute一致，且不为法币，添加反向的quote自动转账
            if path['quote_a'] == path['quote_b'] and path['quote_a'] not in ["gbp","eur","usd","rub","idr","sgd","krw","jpy","cny","pln","uah","cad","aud","php","hkd","thb"]:
                if path['quote_a'] not in balance_config[pm_key]:
                    logging.debug("未设置阈值，pm_key:%s coin:%s" % (pm_key, path['token']))
                    continue
                step_min = 9999999999
                step_max = 9999999999
                steps = balance_config[pm_key][path['quote_a']].replace(' ', '').replace('，',',').split(',')
                step_min = float(steps[0])
                if len(steps) > 1:
                    step_max = float(steps[1])
                else:
                    step_max = 2 * step_min
                cmd_quote = {'step_min' : step_min,'step_max' : step_max, 'max_rate' : 0.25, 'min_rate' : 0.25, 'step' : 0.25, '_id' : pm_key, 'path' : var_path}
                cmd_quote['market_a'], cmd_quote['market_b'], cmd_quote['coin_a'], cmd_quote['coin_b'], cmd_quote['coin'] = cmd['market_b'], cmd['market_a'], cmd['coin_b'], cmd['coin_a'], path['quote_a']
                cmd_quote['pct'] = trader_info.get('paths',{}).get(var_path_old, {}).get(var_path_old, {}).get('last_week', {}).get('pct', 0)
                cmds.append(cmd_quote)
                logging.debug("币币交易，添加反向quote自动转账，market_a:%s market_b:%s symbol_a:%s symbol_b:%s coin:%s" % (cmd_quote['market_a'], cmd_quote['market_b'], cmd_quote['coin_a'], cmd_quote['coin_b'], cmd_quote['coin']))
        return sorted(cmds, key = lambda x:x['pct'], reverse = True)

    def update_blow_history(self, record, is_dispenser = False):
        """
        合约转账记录保存到数据库
        """
        try:
            record['time'] = time.time()
            if is_dispenser:
                self.db['history_of_blowing_dispenser'].insert(record)
            else:
                self.db['history_of_blowing'].insert(record)
            return True
        except Exception as e:
            logging.warning("保存合约转账记录失败，market:%s coin:%s quote:%s msg:%s" % (record['pm'], record['coin'], record['quote'], e.message))
        return False
    
    def get_blow_history(self, market, symbol, is_dispenser = False):
        """
        查询最后合约转账记录
        """
        try:
            where = {
                'pm' : market,
                'symbol' : symbol
            }
            if is_dispenser:
                record = self.db['history_of_blowing_dispenser'].find_one(where, sort = [('time', -1)])
            else:
                record = self.db['history_of_blowing'].find_one(where, sort = [('time', -1)])
            return {'status' : 'ok', 'data' : record}
        except Exception as e:
            logging.warning("查询合约转账记录异常，market:%s symbol:%s msg:%s" % (market, symbol, e.message))
            return {'status' : 'failed', 'msg' : e.message}

    def get_market_info(self, market):
        """
        获取交易所基础信息
        {
            'xrp' :{
                "deposit-enabled" : false,
                "name" : "xrp",
                "withdraw-enabled" : false,
                "withdraw-precision" : 8,
                "state" : "online",
                "withdraw-min-amount" : "1",
                "deposit-min-amount" : "1"
            },
            ...
        }
        """
        try:
            where = {
                'name' : market
            }
            info = self.db['accounts_market'].find_one(where)
            if info is None:
                return info
            res = {}
            for coin in info.get('coins', []):
                res[coin['name']] = coin
            return res
        except Exception as e:
            logging.warning("获取交易所基础信息异常,market:%s msg:%s" % (market, e.message))
            return None


    def withdraw_check(self, cmd):
        """
        查看地址是否安全,是否验证或小验证
        cmd = {
            from : xxx,
            to : xxx,
            coin : xxx,
            address : xxx,
            tag : xxx
        }
        """
        try:
            #查询发币地址是否在地址簿中
            isok = False
            addr_withdraw = self.db['address_withdraw'].find_one({'market_name' : cmd['from'], 'coin' : cmd['coin']})
            if addr_withdraw is None:
                pass
            else:
                for addr in addr_withdraw['addresses']:
                    if addr['address'] == cmd['address'] and (cmd['tag'] == '' or cmd['tag'] is None or str(cmd['tag']) == str(addr['tag'])):
                        isok = True
                        break
            #如果没有看看之前是否有交易记录
            if isok == False:
                trans_rec = self.db['position_cmds'].find_one({'to' : cmd['to'], 'coin':cmd['coin'], 'address' : cmd['address'], 'status' : 3}, sort = [('_id', -1)])
                if trans_rec is None:
                    logging.debug("自动转账校验失败，转账金额大于1000但无交易记录，，from:%s to:%s coin:%s" % (cmd['from'], cmd['to'], cmd['coin']))
                    return False
            cmd_tag = None if cmd.get('tag', None) == '' else cmd.get('tag', None)
            rec_tag = None if trans_rec.get('tag', None) == '' else trans_rec.get('tag', None)
            if cmd_tag == rec_tag:
                logging.debug("自动转账校验成功，转账金额大于1000,且有交易记录，from:%s to:%s coin:%s" % (cmd['from'], cmd['to'], cmd['coin']))
                return True
            logging.debug("自动转账校验失败，转账金额大于1000,有建议记录，但tag不一致，from:%s to:%s coin:%s tag:%s tag_rec:%s" % (cmd['from'], cmd['to'], cmd['coin'], cmd['tag'],trans_rec['tag']))
        except Exception as e:
            logging.debug("自动转账校验异常,from:%s to:%s coin:%s msg:%s" % (cmd['from'], cmd['to'], cmd['coin'], traceback.format_exc()))
        return False


    def get_move_num(self, op_id, coin_trans, interval = 24 * 60 * 60, iscoin = True):
        """
        查询path路径一段时间内的交易量，用来判断转账数量
        return
        {'status':'ok','amount':1000.0}
        """
        result = {'status':'fail','amount':0.0, 'amount_5min' : 0.0}
        try:
            where = {
                "op_id" : {"$regex" : '^%s' % op_id},
                "time" : {"$gt" : time.time() - interval}
            }
            if iscoin:
                where['coin'] = coin_trans
            else:
                where['quote'] = coin_trans
            items = self.db["move_result"].find(where)
            for item in items:
                if item['status'] == 'ok':
                    result['amount'] += item['amount'] if iscoin else item['price'] * item['amount']
                    if time.time() - item.get('time', 0) < 20 * 60:
                        result['amount_5min'] += item['amount'] if iscoin else item['price'] * item['amount']
            result['amount'] = result['amount'] / 2
            result['amount_5min'] = result['amount_5min'] / 2
            result['status'] = 'ok'
            if result['amount'] > 0:
                return result
            #当6小时交易量为零时，查看五分钟内的pct，是否满足阈值
            # where = {
                # "op_id" : {"$regex" : '^%s' % op_id},
                # 'coin' : coin_trans,
                # "time" : {"$gt" : time.time() - 24 * 60 * 60 * 7}
            # }
            # items = dbposition.db["move_result"].find(where)
            # for item in items:
                # if item['status'] == 'ok':
                    # result['amount'] += item['amount'] if iscoin else item['price'] * item['amount']
            # result['amount'] = result['amount'] / 2
            # result['amount'] = result['amount'] / 28
            # result['status'] = 'ok'
            where = {
                "time" : {"$gte":time.time() - 5 * 60},
                "coins.%s" % coin_trans : {"$exists" : True},
                "$where" : "this.coins.{0} != undefined && (this.coins.{0}.pct >= this.coins.{0}.pct_td || this.coins.{0}.offset >= this.coins.{0}.pct_td)".format(coin_trans)
            }
            items = self.db['move_info_' + op_id].count(where)
            if items > 0:
                result['amount'] = 0.1
            logging.debug("通过move_info获取是否转账，结果%s" % result['amount'])   
        except Exception as e:
            logging.warning("查询交易量失败，op_id:%s msg:%s" % (op_id, e.message))
        return result
    
    def get_move_result(self, cmd):
        """
        获取路径的最后的交易记录
        """
        try:
            order = self.db['move_result'].find_one({'market_name' : cmd['market'], 'coin' : cmd['coin']}, sort = [('time', -1)])
            logging.debug("获取路径的最后的交易记录成功，market:%s coin:%s orderid:%s" % (cmd['market'], cmd['coin'], cmd['order_id']))
            return order
        except Exception as e:
            logging.error("获取路径的最后的交易记录异常，market:%s coin:%s orderid:%s" % (cmd['market'], cmd['coin'], cmd['order_id']))
        return {'time' : time.time()}


    def add_cacha(self, conf, insert = True):
        """
        查看缓存表，十分钟内有操作退出
        """
        conf['op_time'] = int(time.time())
        try:
            #判断20分钟内是否又成功转账得记录
            coin_a = conf['coin']
            record = self.db['position_cmds'].find_one({'from' : conf['market_a'], 'to' : conf['market_b'], 'coin' : coin_a, 'status' : 3},sort = [('create_time', -1)])
            if record is not None and time.time() - record['create_time'] < 20 * 60 and record.get('type', 'a2b') in ['a2b', 'send']:
                logging.debug("二十分钟内有自动转账记录，market_a:%s coin:%s market_b:%s" % (conf['market_a'], conf['coin_b'], conf['market_b']))
                #根据资产确定是否到账
                withdrawed = False
                try:
                    withdrawed = self.withdraw_wait(conf['market_a'], conf['market_b'], coin_a, balance = record.get('balance', 99999999999), time = record['create_time'], amount = record['amount'])
                except Exception as e:
                    logging.error("判断转账到账失败，meg:%s" % e.message)  
                return withdrawed
            cacha = self.db['autotran_cacha'].find_one({'_id': conf['_id'], 'op_time' : {'$gte':int(time.time() - 10 * 60)}})
            if cacha is None:
                if insert:
                    self.db['autotran_cacha'].update({'_id': conf['_id']}, conf, True)
                return True
            else:
                logging.debug("连分钟内有重复操作，market_a:%s coin:%s market_b:%s" % (conf['market_a'], conf['coin_b'], conf['market_b']))
            return False
        except Exception as e:
            logging.warning("添加缓存失败，market_a:%s coin:%s market_b:%s msg:%s" % (conf['market_a'], conf['coin_b'], conf['market_b'], traceback.format_exc()))
        return False

    def remove_cacha(self, conf):
        """
        查看缓存表，两分钟内有操作退出
        """
        try:
            self.db['autotran_cacha'].remove({'_id': conf['_id']})
            return True
        except Exception as e:
            logging.warning("删除缓存失败，market_a:%s coin:%s market_b:%s msg:%s" % (conf['market_a'], conf['coin_b'], conf['market_b'], traceback.format_exc()))
        return False

    def save_auto_trans_history(self, record, status = 3):
        """
        保存自动转账记录
        """
        try:
            record['msg'] = 'auto trans ' + record['msg']
            if 'create_time' not in record:
                record['create_time'] = time.time()
            record['finish_time'] = time.time()
            record['status'] = status
            self.db['position_cmds'].insert(record)
            '''
            if status == 3:
                balance_all = get_balance()
                if balance_all is None or record not in balance_all:
                    return
                asset = balance_all[record['to']][]
                for rec in dbposition.db['autotran_cacha'].find({'to' : record['to'], 'coin' : record['coin'], 'status' : 3}):

                dbposition.db['autotran_cacha'].update()
            '''
        except Exception as e:
            logging.warning("保存自动交易记录异常，msg:%s" % e.message)


    def get_balance_repay(self, type = 'last_24hour', days = 0):
        """
        params
        trype : [last_24hour, last_day, last_hour, last_month, last_week, month, today]
        获取每个交易币当前的交易信息
        {
            'date' : '2018-09-15',
            'xrp' : {
                'balance' : xxx,
                'move_num' : xxx,
                'income' : xxx,
                'funder_total' : xxx,
                'paths' : [], #每条路径的详细信息
                'loans' : [],
                'margins': [
                    {
                    'create_time': 1536070340,
                    'loan-left': 8.23912734,
                    'symbol': u'bchusdt',
                    'w': 7.445392881698079} #距离还款时间得小时数
                ],
            },
        }
        """
        ts = datetime.datetime.now() - datetime.timedelta(days = days)
        _id = ts.strftime("%Y-%m-%d %H:") + str(ts.minute - ts.minute % 10)
        try:
            if days == 0:
                trader_info = self.db['trader_new'].find_one({}, sort = [('_id', -1)])
            else:
                trader_info = self.db['trader_new'].find_one({'_id':_id})
            if trader_info is None:
                return None
            res = {'date':trader_info['_id']}
            b_id = ts.strftime("%Y-%m-%dT%H:%M")
            if type not in ['last_24hour', 'today']:
                b_id = ts.strftime("%Y-%m-%dT23:59")
            balance = self.db['exchange_balance_total'].find_one({'_id':b_id})
            for path, data in trader_info['paths'].items():
                for path_c, dd in data.items():
                    if path_c in ['funder_total','income']:
                        continue
                    coin = path_c.split('_')[1]
                    if coin not in res:
                        res[coin] = {
                            'move_num' : 0,
                            'funder_total' : 0,
                            'income' : 0,
                            'balance' : balance['coins'][coin],
                            'paths' : []
                        }
                    dd[type]['path'] = path_c
                    res[coin]['paths'].append(dd[type])
                    res[coin]['move_num'] += dd[type]['succ_count']
                    res[coin]['funder_total'] += dd[type]['funder_total']
                    res[coin]['income'] += dd[type]['income']
            for k, v in res.items():
                if k in ['_id', 'date']:
                    continue
                #获取所有币得借币订单
                v['margins'] = []
                cursur = self.db['margin_order'].find({'coin':k, 'type':'margin', 'loan-left':{'$gt':0}})
                for item in cursur:
                    #计算当前时间距离还款时间距离
                    w = (item['create_time'] - time.time()) % (24 * 60 * 60) / (60 * 60)
                    v['margins'].append({
                        'symbol': item['symbol'],
                        'loan-left': item['loan-left'],
                        'create_time' : item['create_time'],
                        'order_id' : item['order_id'],
                        'w' : w
                    })
                v['margins'].sort(key = lambda x:x['w'])
            return res
        except Exception as e:
            logging.debug("获取交易信息当前交易信息异常，msg:%s" % e.message)
            return None
    
    def save_report_day(self, report):
        """
        保存日报
        """
        try:
            self.db['report_day'].insert(report)
        except Exception as e:
            logging.error("保存日报错误，msg:%s" % e.message)

    def get_usdt_available(self, market):
        """
        获取交易所coin总资产
        """
        result = {
            'total' : 0,
            'available' : 0,
            'status' : 'failed'
        }
        try:
            items = self.db['margin_balance'].find({'name' : market})
            for item in items:
                for balance in item['list']:
                    if balance['currency'] == 'usdt' and balance['type'] == 'transfer-out-available':
                        if balance['balance'] >= 100:
                            result['available'] += (float(balance['balance']) - float(balance['balance']) % 100)
            balance_all = self.db['exchange_balance_detail'].find_one(sort = [{'_id', -1}])
            if balance_all is None:
                return result
            result['available'] += float(balance_all['huobi']['spot']['usdt'])
            balance_total = self.db['exchange_balance_total'].find_one(sort = [('_id', -1)])
            result['total'] = balance_total['money_cny']
            result['status'] = 'ok'
            return result
        except Exception as e:
            logging.warning("获取可用usdt异常，msg:%s" % e.message)
            return result

    def get_balance_repay_new(self, marekt):
        """
        获取火币杠杆账户借款详情
        """
        result = {'status' : 'failed', 'data' : []}
        try:
            plan_balance = self.get_plan_balance()
            cursurs = self.db['margin_balance'].find({'name' : marekt})
            for cursur in cursurs:
                usdt_trade = 0
                transfer_out_available = 0
                coin_trade = 0
                coin_loan = 0
                coin = cursur['_id'].replace('usdt', '')
                for balance in cursur['list']:
                    if balance['type'] == 'trade' and balance['currency'] == 'usdt':
                        usdt_trade = float(balance['balance'])
                    elif balance['type'] == 'transfer-out-available' and balance['currency'] == 'usdt':
                        transfer_out_available = float(balance['balance'])
                    elif balance['type'] == 'trade' and balance['currency'] != 'usdt':
                        coin_trade = float(balance['balance'])
                    elif balance['type'] == 'loan' and balance['currency'] != 'usdt':
                        coin_loan = float(balance['balance'])
                if usdt_trade - transfer_out_available <= 0:
                    continue
                item = {
                    'symbol' : cursur['symbol'],
                    'frozen' : usdt_trade - transfer_out_available,
                    'repayed' : 0,
                    'balance' : coin_trade,
                    'loan' : abs(coin_loan),
                    'plan_loan' : plan_balance.get(coin, 0),
                    'margins' :[]
                }
                #查询未还完的借款订单，并按到期排序
                orders = self.db['margin_order'].find({'name' : marekt,'symbol' : cursur['symbol'], 'type' : 'margin', 'loan-left' : {'$gt' : 0}})
                for order in orders:
                    #计算当前时间距离还款时间距离
                    w = (order['create_time'] - time.time()) % (24 * 60 * 60) / (60 * 60)
                    item['margins'].append({
                        'symbol': order['symbol'],
                        'loan-left': order['loan-left'],
                        'create_time' : order['create_time'],
                        'order_id' : order['order_id'],
                        'coin' : order['coin'],
                        'w' : w
                    })
                if len(item['margins']) == 0:
                    continue
                item['margins'].sort(key = lambda x:x['w'])
                result['data'].append(item)
            result['data'].sort(key = lambda x:x['frozen'], reverse = True)
            result['status'] = 'ok'
        except Exception as e:
            logging.error("获取火币杠杆账户借款详情异常，msg:%s" % traceback.format_exc())
        return result
                
    def get_plan_balance(self):
        """
        计算预计的资产量
        """
        result = {'status' : 'failed', 'data' : {}}
        report = self.get_balance_repay()
        report1 = self.get_balance_repay(type='today', days = 1)
        report2 = self.get_balance_repay(type='today', days = 2)

        for coin, rep in report.items():
            if coin in ['date',]:
                continue
            bl = rep['balance']
            mn = 1 if rep['move_num'] == 0 else rep['move_num']
            bl1 = report1.get(coin, {}).get('balance', 0)
            mn1 = report1.get(coin, {}).get('move_num', 0)
            bl2 = report2.get(coin, {}).get('balance', 0)
            mn2 = report2.get(coin, {}).get('move_num', 0)
            if mn1 + mn2 == 0:
                logging.debug("还币，%s过去两天没有交易，mn1:%s mn2:%s" % (coin, mn1, mn2))
                continue
            bl_level = (bl1 + bl2) / (mn1 + mn2) * mn
            logging.debug("还币，计算资产标准,coin:%s bl_level[%s] = (bl1[%s] + bl2[%s]) / (mn1[%s] + mn2[%s]) * mn[%s]" % (coin, bl_level, bl1, bl2, mn1, mn2, mn))
            result['data'][coin] = bl_level
        result['status'] = 'ok'
        return result
    
    def get_idel_balance(self, home = 'huobi', coins = []):
        """
        返回交易所的闲置资金
        return{
            "home":{'xrp':{'address':'','tag':'xxx','fee':'xxx'},...},
            "data":{
                "bithumb":{
                    "balance":"xxxx",   #当前现货资产
                    "move_num":"xxx",   #24小时交易次数
                    "move_num_12":"xxx",   #12小时交易次数
                    "move_num_6":"xxx",   #6小时交易次数
                    "move_num_3":"xxx",   #3小时交易次数
                    "coin_info":{'address':'','tag':'xxx','fee':'xxx'}  #地址信息
                },...}
        }
        """
        try:
            result = {'status' : 'failed', 'data' : {}, 'home' : {}}
            balance_all = self.get_balance()
            trader_info = self.db['trader_new'].find_one({'updatetime' : {'$gte' : time.time() - 5 * 60}}, sort = [('_id', -1)])

            #获取每个交易所的基本信息
            exchange_info = {}
            cursors = self.db['exchange_info'].find({})
            for cursor in cursors:
                exchange_info[cursor['name']] = {}
                for coin in cursor.get('coins', []):
                    exchange_info[cursor['name']][coin['name']] = coin
                    if home == cursor['name']:
                        result['home'][coin['name']] = coin

            #查询24小时交易量为0的交易所币种信息
            if trader_info is None:
                result['data'] = None
                return result
            for pm, balance in balance_all.items():
                if pm in ['update_time', '_id', 'time', 'op', home]:
                    continue
                if pm not in result['data']:
                    result['data'][pm] = {}
                for coin in balance['spot']:
                    if len(coins) > 0 and coin not in coins:
                        continue
                    if coin in ["krw", "usd","jpy","eur","rub","idr","sgd","pln","uah","cad","aud","php","hkd","thb"]:
                        continue
                    if balance['spot'][coin] <= 0:
                        continue
                    if coin not in result['data'][pm]:
                        result['data'][pm][coin] = {
                            'balance' : balance['spot'][coin], 
                            'move_num' : 0,
                            'coin_info' : exchange_info.get(pm, {}).get(coin, {})
                        }
                    paths = [x for x in trader_info['paths'].keys() if (pm + '_') in x]
                    num = 0
                    for path in paths:
                        num += sum([y.get('last_24hour', {}).get('succ_count', 0) for x,y in trader_info['paths'][path].items() if ('_%s_' % coin) in x])
                    result['data'][pm][coin]['move_num'] += num
            #计算12，6，3小时内的交易量
            end = time.time()
            bg = end - 12 * 3600
            for pm in result['data'].keys():
                for coin in result['data'][pm]:
                    result['data'][pm][coin]['move_num_12'] = 0
                    result['data'][pm][coin]['move_num_6'] = 0
                    result['data'][pm][coin]['move_num_3'] = 0
                    if result['data'][pm][coin]['move_num'] == 0:
                        continue
                    cursors = self.db['move_result'].find({'market':pm,'coin':coin, 'time':{'$gte':bg,'$lte':end}})
                    for cursor in cursors:
                        if cursor['time'] >= (end - 3 * 3600):
                            result['data'][pm][coin]['move_num_12'] += 1
                            result['data'][pm][coin]['move_num_6'] += 1
                            result['data'][pm][coin]['move_num_3'] += 1
                        elif cursor['time'] >= (end - 6 * 3600):
                            result['data'][pm][coin]['move_num_12'] += 1
                            result['data'][pm][coin]['move_num_6'] += 1
                        else:
                            result['data'][pm][coin]['move_num_12'] += 1
            result['status'] = 'ok'
        except Exception as e:
            logging.warning("获取交易所闲置资金失败，msg:%s" % traceback.format_exc())
        return result
        

    def get_balance_margin_new(self):
        """
        自动开仓，根据move_info生成
        """
        result = {'status' : 'failed', 'data' : []}
        cursors = self.db['hedge_conf'].find({'status':1})
        paths = []
        for cursor in cursors:
            pma, pmb = cursor['hedge_path'].split('-')
            move_info_a2b = 'move_info_%s-%s' % (pma, pmb)
            move_info_b2a = 'move_info_%s-%s' % (pmb, pma)
            paths.append({
                'path_a2b' : {'move_info' : move_info_a2b, 'path' : '%s-%s' % (pma, pmb)},
                'path_b2a' : {'move_info' : move_info_b2a, 'path' : '%s-%s' % (pmb, pma)},
                'coin' : cursor['coins']
            })
        endtime = time.time()
        starttime = endtime - 60
        for path in paths:
            #a2b
            res = {}
            cursors = self.db[path['path_a2b']].find({'time' : {'gte' : starttime, 'lte' : endtime}})
            for cursor in cursors:
                sort_list = []
                for coin, move_info in cursor['coin'].items():
                    if move_info['pct'] >=  move_info['pct_td']:
                        sort_list.append({'coin' : coin, 'pct' : move_info['pct']})
                sort_list.sort(key = lambda x:x['pct'], reverse = True)
                for coin in sort_list[:3]:
                    if coin not in res:
                        res[coin] = {'num' : 0, 'w' : 0}
                    res[coin]['num'] += 1
                    res[coin]['w'] += coin['pct']
            items = sorted([{'coin': x, 'w': y['w'], 'path': path['path_a2b']} for x,y in res.items() if y['num'] > 2], key = lambda x:x['w'], reverse = True)[:3]
            result['data'].extend(items)

            #b2a
            res = {}
            cursors = self.db[path['path_b2a']].find({'time' : {'gte' : starttime, 'lte' : endtime}})
            for cursor in cursors:
                sort_list = []
                for coin, move_info in cursor['coin'].items():
                    if move_info['pct'] >=  move_info['pct_td']:
                        sort_list.append({'coin' : coin, 'pct' : move_info['pct']})
                sort_list.sort(key = lambda x:x['pct'], reverse = True)
                for coin in sort_list[:3]:
                    if coin not in res:
                        res[coin] = {'num' : 0, 'w' : 0}
                    res[coin]['num'] += 1
                    res[coin]['w'] += coin['pct']
            items = sorted([{'coin': x, 'w': y['w'], 'path': path['path_b2a']} for x,y in res.items() if y['num'] > 2], key = lambda x:x['w'], reverse = True)[:3]
            result['data'].extend(items)
        result['status'] = 'ok'
        return result


    def withdraw_wait(self, pm_a, pm_b, coin, **kvarg):
        """
        通过交易记录及转账记录判断转账是否到账
        """
        logging.debug("通过交易记录及转账记录判断转账是否到账,pm_a:%s pm_b:%s coin:%s" % (pm_a, pm_b, coin))
        time_trans = None
        balance_trans = None
        amount = None
        if 'balance' in kvarg and 'time' in kvarg and 'amount' in kvarg:
             time_trans = float(kvarg['time'])
             balance_trans = float(kvarg['balance'])
             amount = float(kvarg['amount'])
        else:
            record = self.db['withdraw_wait'].find_one({'from' : pm_a, 'to' : pm_b, 'coin' : coin})
            if record is None:
                logging.debug("通过交易记录及转账记录判断转账是否到账,暂无转账记录,pm_a:%s pm_b:%s coin:%s" % (pm_a, pm_b, coin))
                return True
            time_trans = record['time']
            balance_trans = record['balance']
        time_new = time.time()

        balance_all = self.get_balance()
        balance = balance_all[pm_b]['spot'][coin] + balance_all[pm_b]['spot_frozen'][coin] + sum([y[coin] for x, y in balance_all[pm_b]['margin'].items() if coin in x]) + sum([y[coin] for x, y in balance_all[pm_b]['margin_frozen'].items() if coin in x])
        logging.debug("平台%s现在有币[%s]%s个" % (pm_b, coin, balance))

        #计算交易的记录
        move_results = self.db['move_result'].find({'market' : pm_b, 'coin' : coin, 'status' : 'ok', 'time' : {'$gte' : time_trans, '$lte' : time_new}})
        move_num = 0
        for move_result in move_results:
            move_num += 1
            if move_result['side'] == 'sell':
                balance += float(move_result['amount'])
            else:
                balance -= float(move_result['amount'])
        if move_num == 0:
            logging.info("转账后无交易，不再发币")
            return False
        
        #计算借还记录
        margin_results = self.db['margin_order'].find({'name' : pm_b, 'coin' : coin, 'type' : {'$in' : ['margin', 'repay']}, 'status' : 3, 'time' : {'$gte' : time_trans, '$lte' : time_new}})
        for margin_result in margin_results:
            if margin_result['type'] == 'margin':
                balance -= float(margin_result['amount'])
            else:
                balance += float(margin_result['amount'])
        
        #计算转账记录
        trans_results = self.db['position_cmds'].find({'from' : pm_b, 'coin' : coin, 'status' : 3, 'time' : {'$gte' : time_trans, '$lte' : time_new}})
        for trans_result in trans_results:
            balance += float(trans_result['amount'])
        """
        trans_results = self.db['position_cmds'].find({'to' : pm_b, 'coin' : coin, 'status' : 3, 'time' : {'$gte' : time_trans, '$lte' : time_new}})
        for trans_result in trans_results:
            balance -= float(trans_result['amount'])
        """
        
        logging.debug("通过交易记录及转账记录判断转账是否到账,pm_a:%s pm_b:%s coin:%s balance_trans_pre:%s balance_new:%s" % (pm_a, pm_b, coin, balance_trans, balance))
        #转账到达
        if balance - balance_trans > 0.8 * amount:
            #self.db['withdraw_wait'].remove({'_id' : record['_id']})
            return True
        #更新等待cmd,防止多次操作
        try:
            pass
            #self.db['withdraw_wait'].update({'_id' : record['_id']}, {'$set' : {'time' : time_new, 'balance' : balance}})
        except Exception as e:
            logging.debug("更新确认币到账指令失败，msg:%s" % e.message)
        return False
            
    
    def has_withdraw_record(self, coin, pm_a = None, pm_b = None,end_time = None, interval = 24 * 3600):
        """
        获取交易所是否有转账记录
        """
        result = {'coin' : coin, 'status' : 'failed', 'num' : 0}
        try:
            where = {'status' : 3, 'coin' : coin}
            if pm_a is not None:
                where['from'] = pm_a
            if pm_b is not None:
                where['to'] = pm_b
            if end_time is None:
                end_time = time.time()
            start_time = end_time - interval
            where['create_time'] = {'$gte' : start_time, '$lte' : end_time}
            num = self.db['position_cmds'].count(where)
            result['num'] = num
            result['status'] = 'ok'
        except Exception as e:
            logging.error("获取交易所转账记录异常，msg:%s" % e.message)
        return result
    
    def get_last_withdraw_record(self, coin, pm_a = None, pm_b = None,end_time = None, interval = 24 * 3600):
        """
        获取最后的交易记录
        """
        try:
            where = {'status' : 3, 'coin' : coin}
            if pm_a is not None:
                where['from'] = pm_a
            if pm_b is not None:
                where['to'] = pm_b
            if end_time is None:
                end_time = time.time()
            start_time = end_time - interval
            where['create_time'] = {'$gte' : start_time, '$lte' : end_time}
            return self.db['position_cmds'].find_one(where, sort=[('create_time',-1)])
        except Exception as e:
            logging.error("获取最后的交易记录,msg:%s" % e.message)

    def get_last_margin_record(self, coin, pm_a = None, type = 'auto_margin',end_time = None, interval = 24 * 3600):
        """
        获取最后的开仓记录
        """
        try:
            where = {'status' : 3, 'coin' : coin, 'type' : type}
            if pm_a is not None:
                where['name'] = pm_a
            if end_time is None:
                end_time = time.time()
            start_time = end_time - interval
            where['create_time'] = {'$gte' : start_time, '$lte' : end_time}
            return self.db['margin_order'].find_one(where, sort=[('create_time',-1)])
        except Exception as e:
            logging.error("获取最后的开仓记录,msg:%s" % e.message)

    def auto_margin_report(self, interval = 1, move_interval = 2):
        """
        自动开仓统计
        return {
            'date' : 2018-10-31,
            'total' : 0,    #开仓次数
            'success' : 0,  #开仓成功次数
            'move_num' : 0, #转账后2小时有交易的仓数
            'enough' : 0,   #转账时提示结款超过14万美元的数量
            'precision' : 0,#开仓成功率 success/total
            'recall' : 0    #执行率 move_num/success
        }
        """
        t = time.localtime(time.time())
        time1 = time.mktime(time.strptime(time.strftime('%Y-%m-%d 00:00:00', t),'%Y-%m-%d %H:%M:%S'))
        endtime = float(time1) - (interval - 1) * 24 * 3600 
        starttime = endtime - 24 * 3600
        result = {
            '_id' : time.strftime('%Y-%m-%d', time.localtime(starttime)),
            'total' : 0,
            'success' : 0,
            'move_num' : 0,
            'enough' : 0,
            'precision' : 0,
            'recall' : 0,
            'create_time' : time.time()
        }
        try:
            for order in self.db['margin_order'].find({'type' : 'auto_margin', 'create_time' : {'$gte':starttime,'$lte':endtime}}):
                result['total'] += 1
                if order['status'] in [3, '3']:
                    result['success'] += 1
                    move_info = self.db['move_result'].find_one({'market':order['name'],'coin':order['coin'],'time':{'$gte':order['create_time'],'$lte':order['create_time'] + move_interval * 3600}})
                    if move_info is not None:
                        result['move_num'] += 1
                if u'开仓取消' in order.get('msg',''):
                    result['enough'] += 1
            result['precision'] = round((float(result['success']) / float(result['total']) * 100), 2) if result['total'] > 0 else 100
            result['recall'] = round(float(result['move_num']) / float(result['success']) * 100, 2) if result['success'] > 0 else 100
            self.db['report_auto_margin'].update({'_id':result['_id']}, result, True)
            return result
        except Exception as e:
            logging.error("自动开仓统计异常，%s" % traceback.format_exc())
            return None
    
            return False

    def get_auto_repay_cmds(self, endtime = None, interval = 3600):
        """
        获取近一小时到期的借款订单
        """
        cmds = []
        try:
            where = {'type':'margin','loan-left':{'$gt':0}}
            cursors = self.db['margin_order'].find(where)
            for cursor in cursors:
                t = (time.time() - cursor['create_time']) % (24 * 3600)
                if t >= (24 * 3600) - interval:
                    cmds.append(cursor)
        except Exception as e:
            logging.error("获取还款订单异常，msg:%s" % traceback.format_exc())
        return cmds

