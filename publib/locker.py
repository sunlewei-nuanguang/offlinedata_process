#encoding=utf-8
#created bu gxc at 2018/12/5
import redis
import hashlib
import logging
import cPickle as pickle
import time,json,re,copy,os,sys
from conf.publib_conf import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from conf.publib_conf import LOCK_WAIT, SERVER_NAME

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, password=REDIS_PASSWORD)

class LockWait():
    """
    平台锁
    """
    def add_lock(self, pm, value = None, timeout = None, wait = True, trade = False):
        """
        lock.wait改为redis锁
        params timeout 超时时间，毫秒
        return sec, value
        """
        sec = False
        key = '%s_%s_%s' % (SERVER_NAME, LOCK_WAIT, pm)
        if value is None:
            value = time.time() * 1000
        value = str(value)
        
        while True:
            #判断当前是否有交易
            keys = redis_client.keys("%s_trade_%s*" % (SERVER_NAME, pm))
            #是否交易添加锁
            if trade == False:
                if len(keys) > 0 and wait == False:
                    break
                elif len(keys) > 0:
                    time.sleep(0.1)
                    continue
            
            if redis_client.setnx(key, value):
                if timeout is not None:
                    redis_client.pexpire(key, int(timeout))
                sec = True
                break
            elif wait == False:
                break
            time.sleep(0.1)
        if sec:
            logging.debug("加锁成功，key:%s value:%s" % (key, value))
        else:
            logging.debug("加锁失败，key:%s value:%s" % (key, value))
        return sec, value
    
    def remove_lock(self, pm, value):
        """
        删除lock.wait
        """
        key = '%s_%s_%s' % (SERVER_NAME, LOCK_WAIT, pm)
        value = str(value)
        tmp_value = redis_client.get(key)
        if tmp_value == value:
            redis_client.delete(key)

locker_wait = LockWait()

class LockTrade():
    """
    交易锁
    """
    def add_lock(self, pm, coin, value, max_trade_num = 3):
        """
        添加交易记录
        """
        md5 = hashlib.md5()
        md5.update(value)
        sec_trade = False
        key = '%s_trade_%s_%s_%s' % (SERVER_NAME, pm, coin, md5.hexdigest())
        value = str(value)
        #删除平台锁，在代码中添加
        """
        #平台锁采用乐观锁，1秒后失效
        sec, tmp_value = locker_wait.add_lock(pm, timeout=1000, wait=False, trade=True)
        if sec == False:
            locker_wait.remove_lock(pm, tmp_value)
            return False
        """
        #判断当前是否有交易
        trades = redis_client.keys("%s_trade_%s_%s*" % (SERVER_NAME, pm, coin))
        if len(trades) < max_trade_num:
            redis_client.set(key, value)
            #设置超时时间
            redis_client.expire(key, 20 * 60)
            sec_trade = True
        else:
            logging.debug("交易锁添加失败，正在交易数量超过%s" % max_trade_num)
        #locker_wait.remove_lock(pm, tmp_value)
        return sec_trade
    
    def get_trading_num(self, pm, coin, max_trade_num = 3):
        """
        判断正在交易的订单数量是否大于最大订单量
        """
        if max_trade_num == 1:
            trades = redis_client.keys("%s_trade_%s*" % (SERVER_NAME, pm))
        else:
            trades = redis_client.keys("%s_trade_%s_%s*" % (SERVER_NAME, pm, coin))
        if len(trades) < max_trade_num:
            return True
        return False

    def remove_lock(self, pm, coin, value):
        md5 = hashlib.md5()
        md5.update(value)
        key = '%s_trade_%s_%s_%s' % (SERVER_NAME, pm, coin, md5.hexdigest())
        value = str(value)
        tmp_value = redis_client.get(key)
        if tmp_value == value:
            redis_client.delete(key)

locker_trade = LockTrade()