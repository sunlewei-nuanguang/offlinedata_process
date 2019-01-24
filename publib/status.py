#encoding=utf-8

from publib.utils import truncate_minute


class Status(object):
    '''
    某一个 token 某一时间的状态
    '''

    def __init__(self):
        '''
        默认初始化生成的是空仓的状态
        '''
        self.offset = 0
        self.cost = 0
        self.bid = 1
        self.ask = 1
        self.bid_ask = 0
        self.commission_rate = 0
        self.token = None

    def __lt__(self, other):
        return self.time < other.time

    @classmethod
    def create_by_future_spot_offset(cls, time, bid, ask, token, offset, commission_rate, spot_quote):
        '''
        根据现货、期货数据生成 token 状态
        '''
        s = cls()
        s.time = time
        s.token = token
        s.bid = bid
        s.ask = ask
        s.bid_ask = ask - bid
        s.commission_rate = commission_rate
        s.offset = offset
        s.cost = s.bid_ask / 2 / spot_quote + commission_rate
        return s

    @classmethod
    def create_by_future_spot(cls, time, bid, ask, token, ma, commission_rate, spot_quote):
        '''
        根据现货、期货数据生成 token 状态
        '''
        return cls.create_by_future_spot_offset(time, bid, ask, token, (bid + ask) / 2 / spot_quote - ma, commission_rate, spot_quote)

    @classmethod
    def create_by_future_spot_with_all_ma(cls, time, bid, ask, token, ma, commission_rate, spot_quote_ma):
        '''
        根据现货、期货数据生成 token 状态
        '''
        minute = truncate_minute(time)
        return cls.create_by_future_spot(time, bid, ask, token, ma[minute], commission_rate, spot_quote_ma[minute])

    @classmethod
    def create_by_route(cls, time, bid, ask, mid, token, ma, commission_rate):
        '''
        根据现货、期货数据生成 token 状态
        '''
        s = cls()
        s.time = time
        s.token = token
        s.bid = bid
        s.ask = ask
        s.bid_ask = ask - bid
        s.offset = mid - ma[truncate_minute(time)]
        s.commission_rate = commission_rate
        s.cost = max(0.00001, s.bid_ask / (s.bid + s.ask) + commission_rate)
        return s

    def rising_signal(self):
        return Status().short_signal(self)

    def dropping_signal(self):
        return self.short_signal(Status())

    def shorter_than(self, other, trader, disable_long, disable_short):
        # 如果被替换的不是空仓并且该 token 当前没有仓位，则不能被替换，返回 0
        if disable_long and trader.pos(other.token) * trader.real_price[other.token] > -trader.PER_ORDER and other.token is not None:
            return 0

        if disable_short and trader.pos(self.token) * trader.real_price[self.token] < trader.PER_ORDER and self.token is not None:
            return 0
        return self.short_signal(other)

    def shorter_than_with_thres(self, other, trader, close_thres, disable_long, disable_short):
        # 如果被替换的不是空仓并且该 token 当前没有仓位，则不能被替换，返回 0
        if disable_long and trader.pos(other.token) > -trader.PER_ORDER and other.token is not None:
            return 0

        if disable_short and trader.pos(self.token) < trader.PER_ORDER and self.token is not None:
            return 0
        return self.short_signal_with_thres(other, close_thres)

    def short_signal(self, other):
        '''
        计算两个 status 的替换收益
        '''
        return (self.offset - other.offset) / (self.cost + other.cost)

    def short_signal_with_thres(self, other, close_thres):
        return self.offset - other.offset - (self.cost + other.cost) * close_thres

    def debug_string(self):
        return "%s bid:%.5f/%.5f:ask offset:%.5f cost:%.5f" % (self.token, self.bid, self.ask, self.offset, self.cost)
