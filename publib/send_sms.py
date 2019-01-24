#! /usr/bin/env python2
# encoding:utf-8

import sys, logging, time, json
import requests

from conf.publib_conf import TITLE, TELS_REPORT, URL_DINGDING, URL_TRADE, URL_PCT_MONITOR, URL_LACK_COIN, URL_FOREXHEDGE
import multiprocessing
time_last_send= time.time() - 2000

def notice(msg_report, must = False):
    '''
    汇报账户信息，多人
    '''
    notice_by_dingding(msg_report, must = must)

def notice_by_dingding(msg_report, at_tels = [], must = False,title=None,sleepTime = 3, is_trade = False, is_pct = False, is_lack_coin = False, is_forexhedge = False):
    """
    通过钉钉发送消息
    """
    if len(at_tels) == 0:
        at_tels = TELS_REPORT
    if not title:
        title=TITLE
    msg_report = u"[%s] %s [%s]" % (title, msg_report, time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()))
    logging.info(u"向[%s] 推送消息：%s" % (at_tels, msg_report))
    print(msg_report)
    global time_last_send
    url = URL_DINGDING
    if is_trade:
        url = URL_TRADE
    if is_pct:
        url = URL_PCT_MONITOR 
    if is_lack_coin:
        url =  URL_LACK_COIN     
    if is_forexhedge:
        url =  URL_FOREXHEDGE
    if (time.time() - time_last_send >= 30) or (must and time.time() - time_last_send >= 0):
        try:
            data = {
                "msgtype": "text",
                "text": {
                   "content" : msg_report
                },
                "at": {
                    "atMobiles": at_tels,
                    "isAtAll": False
                }
            }
            resp = requests.post(url,
                data = json.dumps(data),
                headers = {'Content-Type': 'application/json'},
                timeout = 5)
            # print resp.content
            time.sleep(sleepTime)
        except Exception,e:
            logging.error(u"发送钉钉消息失败，%s" % e.message)
        time_last_send = time.time()

def hedging_notice_process(notice_result_info):
    """
    通过进程启动交易进程
    """
    msg = notice_result_info['msg']
    title = notice_result_info['title']
    _type = int(notice_result_info['type'])
    if _type == 1:
        notice_by_dingding(msg, title = title, must = True, sleepTime= 0.2, is_trade = True)
    elif _type == 2:
        notice_by_dingding(msg, title = title, must = True, sleepTime= 0.2)
    else:
        notice_by_dingding(msg, title = title, must = True, sleepTime= 0.2, is_lack_coin =True)

def hedging_notice(*args):
    '''
     解析交易参数：
        1.msg信息
        2.报警title
        3.type， 即给哪个钉钉发报警
    '''
    notice_result_info = {}
    notice_result_info['msg'] = args[0]
    notice_result_info['title'] = args[1]
    notice_result_info['type'] = args[2]
    process = multiprocessing.Process(target = hedging_notice_process,  args= (notice_result_info,))
    process.start()


