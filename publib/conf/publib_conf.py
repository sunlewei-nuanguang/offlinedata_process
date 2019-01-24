#coding:utf-8
QuoteSort = ['usd', 'usdt', 'aud', 'jpy', 'krw']
SERVER_NAME = 'server1'
OTC_TYPE = False
if SERVER_NAME in ['server4','server1','server2']:
	OTC_TYPE = True
LOCK_WAIT_INFO = "lock.wait_info"
LOCK_FILE = "lock.file"
LOCK_FATAL = "lock.fatal"
LOCK_ERROR = "lock.error"
LOCK_WAIT = "lock.wait"
CMD_LOG_FILE = "./logs/cmds_result.log"
CMD_LOG_FILE_FATAL = "./logs/cmds.log"
CMD_RESULT = "./logs/cmds_result.json" #存放指令的收益效果：火币和网关指令双方预期价格，双方的最终成交价格，双方消耗的时间，预期的价差。供风控模块判断最近的交易盈亏情况，并进行相应的风控处理

CONN_ADDR_AWS = "172.31.140.205:27777"#"47.75.149.49:27777"
USERNAME_AWS = "adminall"
PWD_AWS = "moveall"
CONN_ADDR_FUNDINFO = "127.0.0.1:27777"#"47.75.149.49:27777"
USERNAME_FUNDINFO = "adminall"
PWD_FUNDINFO = "moveall"
KARKEN_USDT_ADDR = "172.31.140.84:27777"
CONN_ADDR_AWS_TRADE="172.31.140.86:28888"
USERNAME_TRADE = "adminall"
PWD_TRADE = "moveall"

#REDIS_HOST = '172.31.140.86'
REDIS_HOST = 'nuanguang.redis.rds.aliyuncs.com'
REDIS_PORT = 6379
REDIS_PASSWORD = 'nuanguang@2018&0720'

DEPTH_LOG_DIR = '/data1/depth_log'
#交易参数统一变量
ORDER_TYPE_MARKET = 'market' #市价单交易
ORDER_TYPE_LIMIT = 'limit'   #限价单交易
#下单类型 key:交易所 key：market limit
ORDER_TYPE_DICT = {
	"bitbank":ORDER_TYPE_MARKET,
	#"bithumb":ORDER_TYPE_MARKET,
	"coinlink":ORDER_TYPE_MARKET,
	"abcc":ORDER_TYPE_MARKET
}
#使用模拟器来交易的交易所
MARKETS_SIMULATOR = [
	'abcc',
	'coinlink',
]

WHITE_LIST=['huobi-usdt_omg_bithumb-krw','btcbox-jpy_ltc_huobi-usdt','huobi-usdt_bch_btcbox-jpy','huobi-usdt_btc_btcbox-jpy','huobi-usdt_eth_btcbox-jpy','huobi-usdt_ltc_btcbox-jpy','btcbox-jpy_bch_huobi-usdt','btcbox-jpy_btc_huobi-usdt','btcbox-jpy_eth_huobi-usdt']
TITLE = "test"
URL_DINGDING = "https://oapi.dingtalk.com/robot/send?access_token=9f504d3673c3fa2c68961b0ee6c5fb723bb7d814912c145acf3e2493dad3887e"
URL_TRADE = "https://oapi.dingtalk.com/robot/send?access_token=ecceadc04d6537b3d623a0cf1248b40b56dfe41eb60e1a5665ae40b5d3d3d665"
URL_PCT_MONITOR = "https://oapi.dingtalk.com/robot/send?access_token=1a4e0ffcbc4936cd42f5c59d53e945a4a1569a0ec0ca986f1d878567f536d9dc"
URL_LACK_COIN = "https://oapi.dingtalk.com/robot/send?access_token=605eaa4db0204079c13eedb84b292068364378f661d5c77cc9bd2493900bdb4a"
URL_FOREXHEDGE = "https://oapi.dingtalk.com/robot/send?access_token=cd3f87ed7a1c97f09274a38f81d1f7a95a097d3f3f994efcbde20348e7adbc3e"
TELS_REPORT = []
