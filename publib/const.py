#coding:utf-8

#交易参数统一变量
ORDER_TYPE_MARKET = 'market' #市价单交易
ORDER_TYPE_LIMIT = 'limit'   #限价单交易

TRADE_TYPE_MARGIN = 'margin' #交易方式--杠杆交易
TRADE_TYPE_SPOT = 'spot'     #交易方式--现货交易
TRADE_TYPE_FUTURE = 'future' #交易方式--合约交易

SIDE_BUY = 'buy'             #买入
SIDE_SELL = 'sell'           #卖出
SIDE_EXIT_LONG = 'exit_long' #平多
SIDE_EXIT_SHORT = 'exit_short' #平空
TRADE_DICT = {
    '1': SIDE_BUY,
    '2': SIDE_SELL,
    '3': SIDE_EXIT_LONG,
    '4': SIDE_EXIT_SHORT
}
COMMISSION_RATE = {'huobi': 0.0005, 'bitbank': 0.0012, 'bithumb': 0.0008}
EXCHANGE_QUOTE = {'huobi': 'usdt', 'bithumb': 'krw', 'bitbank': 'jpy', 'btcbox': 'jpy'}
