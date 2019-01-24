#encoding=utf-8
import time, logging
import traceback
import importlib
from collections import deque
from publib.market import DbMarket
from clients.apiclientlib import SIDE_BUY, SIDE_SELL, CODE_OK, CODE_FAILED, ORDER_STATUS_OPEN
import threading
import random
import json, hashlib
import cPickle

class SuperDealer():
    """
    记录dealer初始状态和交易记录
    """
    market = None
    coin = None
    quote = None
    #交割线
    conf_price = None
    #交易数量
    conf_amount = None
    #每次交易量
    conf_amount_per = None
    #已成交量
    amount_traded = 0
    #挂单量
    amout_frozen = 0
    #最后一次的下单价
    last_price = None
    #操作，buy,sell
    side = None
    #加价量
    price_per = None
    #最小交易量
    amount_min = None
    #最小交易额
    dealer_min = None
    #缓存交易记录（frozen, traded）
    trade_queue = deque([])
    #记录有问题的订单 {'order_id':xxx,'amount':xxx,'price':xxx}
    error_orders = []
    #记录下单记录 {'order_id':xxx,'amount':xxx,'price':xxx}
    history_orders = []
    #成交记录{'order_id':xxx,'traded':xxx,'price':xxx}
    traded_orders = []

    def __init__(self, market, coin, quote, **kvarg):
        """
        初始化
        """
        self.market = market
        self.coin = coin
        self.quote = quote
        self.conf_price = kvarg['conf_price']
        self.conf_amount = kvarg['conf_amount']
        self.conf_amount_per = kvarg['conf_amount_per']
        self.side = kvarg['side']
        #maxlen = int(round((self.conf_amount - self.amount_traded)/self.conf_amount_per, 2))
        self.queue = deque([])

        #实例化clientapi
        module = importlib.import_module('clients.%s' % self.market)
        clientapi = getattr(module, self.market)
        self.client = clientapi(self.coin, self.quote)

        #获取交易所配置文件
        module_conf = importlib.import_module('clients.conf.%s' % self.market)
        self.config_client = getattr(module_conf, 'config_%s' % self.market)
        if getattr(clientapi, 'get_precision', None) is not None and callable(clientapi.get_precision):
            self.price_per = clientapi.get_precision
        else:
            self.price_per = self.config_client['precision'][self.client.symbol].get('price_per', None) or (1.0 / 10 ** self.config_client['precision'][self.client.symbol]['price'])
        self.amount_min = self.config_client['precision'][self.client.symbol].get('amount_min', None) or (1.0 / 10 ** self.config_client['precision'][self.client.symbol]['amount'])
        self.dealer_min = self.config_client['precision'][self.client.symbol].get('min', 0)

        if self.conf_amount_per is None:
            self.conf_amount_per = self.amount_min * 10
        
        logging.info("初始化dealer,pm:%s coin:%s quote:%s  conf_amount:%s conf_amount_per:%s conf_price:%s price_per:%s amount_min:%s dealer_min:%s" % (market, coin, quote, self.conf_amount, self.conf_amount_per, self.conf_price, self.price_per, self.amount_min, self.dealer_min))

    def trade(self):
        """
        开始交易，每个1s下一个订单
        开启多线程查询订单执行状态，通过队列保存查询结果
        """
        result = {'status' : CODE_FAILED, 'orders' : [], 'trade_history' : [], 'error_history' : [], 'msg' : 'empty'}
        self.dbmarket = DbMarket(self.market, self.quote)
        failed_num = 0
        while True:
            logging.info("当前执行情况，frozen:%s traded:%s" % (self.amout_frozen, self.amount_traded))
            #交易量是否满足
            self.query_dealer()
            self.get_dealer_status()
            #保存记录
            self.calu_status(result)
            if failed_num >= 3 and self.amout_frozen > 0:
                time.sleep(0.1)
                continue
            elif failed_num >= 3 and self.amout_frozen <= 0:
                result['msg'] = "失败次数超过三次"
                logging.warning("dealer交易异常，失败次数大于三次，failed_num:%s" % failed_num)
                break
            #计算下单价格，下单量
            price = self.get_dealer_price()
            amount = min(self.conf_amount_per, self.conf_amount - self.amount_traded - self.amout_frozen)
            if price is None:
                logging.info("价格不满足预期")
                time.sleep(0.05)
                continue
            #交易量满足初始值，结束交易
            if (self.amount_traded >= self.conf_amount or abs(self.amount_traded - self.conf_amount) < self.amount_min) and self.amout_frozen == 0:
                logging.info("dealer完成，amount_traded:%s conf_amount:%s" % (self.amount_traded, self.conf_amount))
                result['status'] = CODE_OK
                break
            #交易量小于最小交易额,最小下单量，结束交易
            elif (price * amount < self.dealer_min or amount < self.amount_min) and self.amout_frozen == 0:
                logging.info("dealer完成，下单数量小于最小交易额，price:%s amount:%s dealer_min:%s" % (price, amount, self.dealer_min))
                result['status'] = CODE_OK
                break
            #交易量+冻结量满足初始值，等待交易完成
            elif self.amout_frozen + self.amount_traded >= self.conf_amount or abs(self.amount_traded + self.amout_frozen - self.conf_amount) < self.amount_min:
                logging.info("dealer完成，amount_traded:%s conf_amount:%s" % (self.amount_traded, self.conf_amount))
                time.sleep(0.05)
                continue
            #下单数量少于最小
            elif amount < self.conf_amount_per and amount + self.amout_frozen > self.conf_amount_per:
                time.sleep(0.05)
                continue
            #有错误订单时
            if len(self.error_orders) >= 3 and self.amout_frozen == 0:
                break
            elif len(self.error_orders) >= 3:
                logging.debug("存在错误查询订单，不再进行dealer交易")
                time.sleep(0.05)
                continue
            elif len(self.history_orders) - len(self.traded_orders) >= 5:
                print '*' * 20
                logging.info("查询订单线程过多，等待订单查询，thread_num:%s" % (len(self.history_orders) - len(self.traded_orders)))
                time.sleep(0.05)
                continue
            
            logging.debug("获取dealer下单价格，下单量，price:%s last_price:%s amount:%s" % (price, self.last_price, amount))
            #价格跟上次一样不再下单
            if (price is None or price == self.last_price) and self.amout_frozen <= 0:
                time.sleep(0.05)
                continue
            res = {'status':CODE_FAILED, 'msg':'empty'}
            try:
                res = self.client.create_limit_order(self.side, amount, price)
                logging.debug("dealer下单结果，result:%s" % res)
            except Exception as e:
                res["msg"] = "dealer下单异常，msg:%s" % e.message
                logging.error("dealer下单异常，msg:%s" % traceback.format_exc())
                
            if res['status'] == CODE_OK:
                self.last_price = price
                self.history_orders.append({
                    'order_id' : res['order_id'],
                    'price' : price,
                    'amount' : amount
                })
                self.trade_queue.append({
                    'order_id' : res['order_id'],
                    'frozen' : amount,
                    'traded' : 0
                })
                self.queue.append({
                    'order_id' : res['order_id'],
                    'coin' : self.coin,
                    'quote' : self.quote,
                    'price' : price,
                    'amount' : amount
                })
            else:
                failed_num += 1
                logging.warning("dealer下单失败，msg:%s" % res['msg'])
                time.sleep(0.99)

        # #计算买入数量和平均价格
        # amount = sum([x['amount'] for x in self.traded_orders])
        # cost = sum([x['amount'] * x['price'] for x in self.traded_orders])
        # price = round(cost/amount, 8) if cost > 0 else 0
        # result['orders'] = self.history_orders
        # result['trade_history'] = self.traded_orders
        # result['error_history'] = self.error_orders
        # result['price'] = price
        # result['amount'] = amount
        # result['cost'] = cost
        self.calu_status(result)
        if len(self.error_orders) > 0:
            result['msg'] = "交易存在错误订单，orders:%s" % self.error_orders
            logging.warning(result['msg'])
        logging.info("交易完成，result:%s" % result)
        return result

    def get_dealer_price(self):
        """
        获取下单价格，价格为买一，卖一加减最低价格单位
        """
        try:
            endtime = time.time() * 1000
            starttime = endtime - 5 * 1000
            depth = self.dbmarket.get_market(self.coin, starttime, endtime)
            if depth is None:
                return None
            if self.side == SIDE_BUY:
                if depth['asks'][0]['price'] < 0.995 * self.conf_price:
                    logging.info("ask价格小于1.1倍conf_price, ask:%s conf_price:%s" % (depth['asks'][0]['price'], self.conf_price))
                    return depth['asks'][0]['price']
                price = depth['asks'][0]['price'] - (self.price_per(depth['asks'][0]['price']) if callable(self.price_per) else self.price_per)
                if self.conf_price is not None and price > self.conf_price:
                    logging.info("价格不满足预期，price:%s conf_price:%s" % (price, self.conf_price))
                    return None
                return price
            elif self.side == SIDE_SELL:
                if depth['bids'][0]['price'] < 1.005 * self.conf_price:
                    logging.info("bid价格小于1.1倍conf_price, bid:%s conf_price:%s" % (depth['bids'][0]['price'], self.conf_price))
                    return depth['bids'][0]['price']
                price = depth['bids'][0]['price'] + (self.price_per(depth['bids'][0]['price']) if callable(self.price_per) else self.price_per)
                if self.conf_price is not None and price < self.conf_price:
                    logging.info("价格不满足预期，price:%s conf_price:%s" % (price, self.conf_price))
                    return None
                return price
        except:
            logging.debug("计算dealer下单价格异常，\n%s" % traceback.format_exc())

    def get_dealer_status(self):
        """
        通过trade交易记录统计当前交易状态
        """
        while True:
            order_info = None
            try:
                order_info = self.trade_queue.popleft()
                self.amout_frozen += order_info['frozen']
                self.amount_traded += order_info['traded']
                #发现查询错误订单保存到error_orders
                if order_info.get('status', None) == -1:
                    self.error_orders.append({
                        'order_id' : order_info['order_id'],
                        'price' : order_info['price'],
                        'amount' : order_info['amount']
                    })
                    logging.warning("发现异常查询订单，lock:%s" % order_info)
                    continue
                #正常订单保存到traded_orders
                elif order_info.get('status', None) == 3:
                    self.traded_orders.append({
                        'order_id' : order_info['order_id'],
                        'price' : order_info['price'],
                        'amount' : order_info['traded']
                    })

            except IndexError as e:
                logging.info("没有交易完成结果")
                break
            except:
                logging.error("交易完成结果,order_info:%s\n%s" % (order_info, traceback.format_exc()))

    def query_dealer(self):
        """
        开启线程查询订单交易量
        """
        order_info = None
        try:
            order_info = self.queue.popleft()
            logging.info("开启线程查询订单状态，order_info:%s" % order_info)
            proc = threading.Thread(target= self.query_dealer_thread, args=(self.client, order_info, self.trade_queue, self.queue))
            #proc.daemon = False
            proc.start()
        except IndexError as e:
            logging.info("没有需要查询的交易")
        except:
            logging.error("查询交易状态异常,order_info:%s\n%s" % (order_info, traceback.format_exc()))

    def calu_status(self, result):
        #计算买入数量和平均价格
        #cp_res = cPickle.loads(cPickle.dumps(result))
        amount = sum([x['amount'] for x in self.traded_orders])
        cost = sum([x['amount'] * x['price'] for x in self.traded_orders])
        price = round(cost/amount, 8) if cost > 0 else 0
        result['orders'] = self.history_orders
        result['trade_history'] = self.traded_orders
        result['error_history'] = self.error_orders
        result['price'] = price
        result['amount'] = amount
        result['cost'] = cost
        return result

    @staticmethod
    def query_dealer_thread(clientapi, order_info, trade_queue, queue):
        #查询订单状态并保存到队列，和对冲交易逻辑相似
        status = 0
        closed = False
        bg = time.time()
        while True:
            #查询超时，锁住系统
            if time.time() - bg > 5 * 60:
                logging.warning("查询订单超时,order_info:%s" % order_info)
                order_info['status'] = -1
                order_info['msg'] = "查询订单超时"
                order_info['frozen'] = -order_info['amount']
                order_info['traded'] = 0
                trade_queue.append(order_info)
                break
            try:
                time.sleep(3)
                res = clientapi.fetch_order_one(order_info['order_id'])
                logging.debug("查询订单状态 order_id:%s status:%s：%s" % (order_info['order_id'], status, res))
                if res['status'] == CODE_OK and res['order_status'] != ORDER_STATUS_OPEN:
                    order_info['frozen'] = -order_info['amount']
                    order_info['traded'] = res['amount']
                    order_info['status'] = 3
                    trade_queue.append(order_info)
                    break
                else:
                    time.sleep(1)
                    status += 1
                if time.time() - bg > 60 and closed == False:
                    res = clientapi.cancel_order_temp(order_info['order_id'])
                    logging.debug("取消订单返回结果:order_id:%s status:%s：%s" % (order_info['order_id'], status, res))
                    if res['status'] == CODE_OK:
                        time.sleep(2)
                        closed = True
            except:
                logging.error("查询订单状态异常，msg:%s" % traceback.format_exc())
