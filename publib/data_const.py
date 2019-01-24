import datetime

ABNORMAL_TIMES = {
    'bitbank': [(datetime.datetime(2018, 9, 21, 19), datetime.datetime(2018, 9, 22, 17)), 
                (datetime.datetime(2018, 11, 15, 0), datetime.datetime(2018, 11, 15, 10)), 
                (datetime.datetime(2018, 11, 21, 4), datetime.datetime(2018, 11, 21, 10)),],
    'huobi':[(datetime.datetime(2018, 10, 11, 8), datetime.datetime(2018, 10, 11, 12))],}
WITH_DRAW_LIMIT = {
    'huobi': {'bch': [(datetime.datetime(2018, 11, 14), datetime.datetime(2018, 12, 25)),]},
    'bitbank': {'bch':[(datetime.datetime(2018,11, 14), datetime.datetime(2018,12,25)),],},
    'btcbox': {'bch': [(datetime.datetime(2018, 11, 14), datetime.datetime(2018,12, 25)),],},
    'bithumb': {'eos':[(datetime.datetime(2018, 8, 1), datetime.datetime(2018, 8, 24)), 
                       (datetime.datetime(2018, 9, 11), datetime.datetime(2018, 10, 12)),],
                'btc':[(datetime.datetime(2018, 8, 1),datetime.datetime(2018, 8, 5)),],
                'bch':[(datetime.datetime(2018, 8, 1),datetime.datetime(2018, 8, 5)),
                        (datetime.datetime(2018, 11, 14),datetime.datetime(2018,12, 25)),], 
                'etc':[(datetime.datetime(2018, 8, 1),datetime.datetime(2018, 8, 5)),], 
                'eth':[(datetime.datetime(2018, 8, 1),datetime.datetime(2018, 8, 5)),],  
                'ltc':[(datetime.datetime(2018, 8, 1),datetime.datetime(2018, 8, 5)),],  
                'qtum':[(datetime.datetime(2018, 8, 1),datetime.datetime(2018, 8, 5)),], 
                'xrp':[(datetime.datetime(2018, 8, 1),datetime.datetime(2018, 8, 5, 12)),], 
                'ctxc':[(datetime.datetime(2018, 8, 1),datetime.datetime(2018, 8, 25)),], 
                'elf':[(datetime.datetime(2018, 8, 1),datetime.datetime(2018, 8, 29)),],
                'zil':[(datetime.datetime(2018, 8, 1),datetime.datetime(2018, 8, 29)),],                
                'omg':[(datetime.datetime(2018, 8, 1),datetime.datetime(2018, 8, 29)),],  
                'trx':[(datetime.datetime(2018, 8, 1),datetime.datetime(2018, 9, 21)),], }}
