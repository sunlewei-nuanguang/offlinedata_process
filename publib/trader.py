# coding:utf-8
# author:Kebin

import numpy
import pandas
import time
import datetime
import logging
import plotly.graph_objs as go
from publib.initializer import initializer
from collections import defaultdict
from scipy.stats import norm

initializer.add_argument("--disable_check", action='store_true', help='禁用现金平衡检查')


EPS = 1e-8
TOTAL = 'total'
SECONDS_PER_DAY = 24 * 3600
DAY1 = datetime.timedelta(days=1)


class Signal(object):
    '''交易信号'''

    def __init__(self, bar, price, symbol, is_long, is_open):
        self.bar = bar
        self.symbol = symbol
        self.price = price
        self.is_long = is_long
        self.is_open = is_open

    def __lt__(self, y):
        return self.time < y.time


def describe_string(desc):
    return "mean %.2f median %.2f min %.2f max %.2f std: %.2f" % (
        desc['mean'], desc['50%'], desc['min'], desc['max'], desc['std'])


def prune_timeline(time_list, time_begin, time_end):
    if isinstance(time_list[0][0], datetime.datetime):
        time_begin = datetime.datetime.combine(time_begin, datetime.time())
        time_end = datetime.datetime.combine(time_end, datetime.time())
    else:
        assert(isinstance(time_list[0][0], datetime.date))
    return [(t, v) for t, v in time_list if t >= time_begin and t < time_end]


class Trader(object):
    '''
    交易统计基类

    派生类通常会重载 commission 函数来实现不同的手续费逻辑
    基本用法参考下面的 sample_trader 函数
    '''

    @classmethod
    def calculate_drawdown(cls, history):
        history_max_time, history_max = history[0]
        init_asset = history_max
        max_drawdown_time = datetime.timedelta(0)
        max_drawdown_begin = None
        max_drawdown = 0
        for time, val in history:
            if val > history_max:
                history_max = val
                history_max_time = time
            else:
                max_drawdown = max(max_drawdown, 1 - val / history_max)
                if time - history_max_time > max_drawdown_time:
                    max_drawdown_time = time - history_max_time
                    max_drawdown_begin = history_max_time
        profit = ((1 + history[-1][1]) / (1 + history[0][1]) - 1)
        sharpe = profit / (max_drawdown + EPS)

        asset_list = [p for t, p in history]
        weekly = pandas.Series((after / before - 1) * 100 for before, after in zip(asset_list, asset_list[1:])).rolling(window=7).sum().describe()

        info = "max_down:%.4f time:%s from:%s sharpe_ratio:%s week:[%s]" % (
            max_drawdown, max_drawdown_time, max_drawdown_begin, sharpe, describe_string(weekly))
        return {"max_drawdown": max_drawdown, "max_drawdown_time": max_drawdown_time,
            "max_drawdown_begin": max_drawdown_begin, "sharpe": sharpe, "profit": profit,
            "info": info, "weekly": weekly}

    def __init__(self, init_cash=None):
        self.init(init_cash)

    def init(self, init_cash):
        '''
        初始化函数，会根据交易对数量配置资金。如果不需要对爆仓进行模拟则不必关注资金量的问题。
        '''
        self.cash = init_cash
        self.init_cash = init_cash
        self.cost = defaultdict(float)
        self.symbol_commission = defaultdict(float)
        self.portfolio = defaultdict(int)
        self.win_cnt = defaultdict(int)
        self.lose_cnt = defaultdict(int)
        self.trade_cnt = defaultdict(int)
        self.last_price = defaultdict(float)
        self.real_price = defaultdict(float)
        self.profit = defaultdict(float)
        self.pos_expect = [defaultdict(float), defaultdict(float)]
        self.neg_expect = [defaultdict(float), defaultdict(float)]
        self.symbol_day_profit = defaultdict(lambda: defaultdict(float))
        self.trade_amount = defaultdict(lambda: defaultdict(float))

        self.daily_history = []
        self.day_cnt = 0

        self.symbol_trade_date = defaultdict(set)
        self._pending = defaultdict(list)

        self._portfolio_history = defaultdict(list)
        self._portfolio_history_cash = defaultdict(list)
        self._profit_history = defaultdict(list)

    def __lt__(self, other):
        return sum(self.profit.values()) < sum(other.profit.values())

    @classmethod
    def trade_date_signal(cls, date_signal_list):
        trader = cls()
        for date, signal_list in sorted(date_signal_list.items()):
            trader.day_begin(date)
            for signal in sorted(signal_list):
                trader.trade_by_signal(signal)
            trader.day_end(date)
        trader.show()

    def pos(self, symbol):
        '''
        请求当前指定 symbol 的仓位
        '''
        return self.portfolio[symbol]

    def trade_per_day(self):
        '''
        平均每天交易次数
        '''
        return float(sum(self.trade_cnt.values())) / self.day_cnt
    
    @classmethod
    def turnover_rate(cls, trade_amount_list, begin_cash):
        '''
        计算平均换手率, key: 'min', 'max', '25%', '75%', '50%', 'mean'
        '''
        return pandas.Series(trade_amount_list).describe()/begin_cash
    
    @classmethod
    def profit_daliy_info(cls, daily_history):
        '''
        计算盈利的天数，亏损的天数，利润横盘的天数，以天为单位的利润平均值、标准差
        '''
        day_profit = [(t0, p1 - p0) for (t0, p0), (t1, p1) in zip(daily_history, daily_history[1:])]
        mean, std = norm.fit([x for _, x in day_profit])
        win_day = len([x for _, x in day_profit if x > 0])
        loss_day = len([x for _, x in day_profit if x < 0])
        zero_day = len([x for _, x in day_profit if x == 0])
        return 'win_day:loss_day:zero %s:%s:%s mean:%.2f std:%.2f' % (win_day, loss_day, zero_day, mean, std)

    def show(self):
        '''
        交易结果简报
        '''
        total_win = sum(self.win_cnt.values())
        total_lose = sum(self.lose_cnt.values())
        total_trade = sum(self.trade_cnt.values())
        result = [
            "\ncash:%s cost:%s symbol_commission:%s" % (self.cash, sum(self.cost.values()), sum(self.symbol_commission.values())),
            "profit:%.6f expect:long:%.6f:%.6f: short:%.6f:%.6f" % (
                (sum(self.profit.values())) / self.init_cash,
                sum(self.pos_expect[1].values()), sum(self.neg_expect[1].values()),
                sum(self.pos_expect[0].values()), sum(self.neg_expect[0].values())),
            "win:loss %s:%s %.2f trades per day %d total" % (
                total_win, total_lose, total_trade / float(self.day_cnt), total_trade),
            "turnover rate %s" % describe_string(self.turnover_rate(self.trade_amount[TOTAL].values(), self.init_cash)),
            self.calculate_drawdown(self.daily_history)["info"],
            self.profit_daliy_info(self.daily_history),
            " ".join("%s:%.2f:%.2f:%.2f" % (k, v, numpy.mean(self.trade_amount[k].values())/self.init_cash, self.portfolio.get(k, 0))
                    for k, v in self.profit.items()),
            " ".join("%s:%.2f" % (k, v) for k, v in self.occupy_percentage().items())]

        if self.daily_history:
            result.append("%s-%s" % (self.daily_history[0][0], self.daily_history[-1][0]))

        return "\n".join(result)
    
    def interval_show(self, time_begin, time_end):
        '''
        某段时间内交易结果简报
        '''
        daily_history_itv = prune_timeline(self.daily_history, time_begin, time_end + DAY1)
        profit_cash = daily_history_itv[-1][1] - daily_history_itv[0][1]
        portfolio_history_cash_itv = prune_timeline(self._portfolio_history_cash[TOTAL], time_begin, time_end)
        trade_amount_itv = prune_timeline(list(self.trade_amount[TOTAL].items()), time_begin, time_end)

        result = [
            "\n%s-%s" % (time_begin, time_end),
            "cash:%.2f profit:%.4f profit_cash:%.2f" % (daily_history_itv[-1][1], profit_cash/daily_history_itv[0][1], profit_cash), 
            "%.2f trades per day %d total" %(len(portfolio_history_cash_itv) / ((time_end - time_begin).total_seconds() / SECONDS_PER_DAY), len(portfolio_history_cash_itv)),
            "turnover rate %s" % describe_string(self.turnover_rate([v for _, v in trade_amount_itv], daily_history_itv[0][1])),
            self.calculate_drawdown(daily_history_itv)["info"],
            self.profit_daliy_info(daily_history_itv),
            " ".join("%s:%.2f" % (k, v) for k, v in self.occupy_percentage(time_begin, time_end).items())]
        
        return "\n".join(result)
    
    def thisweek_show(self):
        '''
        当周收益
        '''
        today = datetime.date.today()
        time_begin_week, time_show_end = today - datetime.timedelta(today.weekday()), today + DAY1
        return self.interval_show(time_begin_week, time_show_end)

    def thismonth_show(self, start_date=6):
        '''
        当月收益，每月按照6日开始计算盈亏
        '''
        today = datetime.date.today()
        time_begin_month, time_show_end = datetime.date(today.year, today.month, start_date), today + DAY1
        return self.interval_show(time_begin_month, time_show_end)

    def trade_by_signal(self, signal):
        full_pos = int(self.init_cash / signal.price) * (1 if signal.is_long else -1)
        if signal.is_open:
            pos = full_pos - self.portfolio[signal.symbol]
        else:
            pos = -self.portfolio[signal.symbol]

        if pos == 0 or (pos > 0) != signal.is_long:
            return

        gap = signal.price - signal.bar.close
        if not signal.is_long:
            gap = - gap

        self.trade(signal.bar.time, signal.symbol, signal.price, gap, pos)
        logging.debug("signal trade %s %s %s:%s %s %s:%s:%s %s",
            signal.bar.time, signal.symbol, signal.price, pos, self.portfolio[signal.symbol],
            signal.bar.close_bid, signal.bar.close, signal.bar.close_ask, self.profit[signal.symbol])

    def real_trade(self, symbol, is_long, abs_pos, price, last, bid, ask):
        pos = abs_pos if is_long else -abs_pos
        self.trade(datetime.datetime.now(), symbol, price, 0, pos)
        logging.info("real trade %s %s:%s %s %s:%s:%s %s",
            symbol, price, pos, self.portfolio[symbol], bid, last, ask, self.profit[symbol])

    def update_price_and_profit(self, time, symbol, price):
        profit = (price - self.real_price[symbol]) * self.portfolio.get(symbol, 0)
        self.profit[symbol] += profit
        self.symbol_day_profit[time.date()][symbol] += profit
        self.real_price[symbol] = price
    
    def occupy_percentage(self, time_begin=None, time_end=None):
        '''
        计算每个symbol的满仓率
        '''
        if len(self._portfolio_history_cash[TOTAL]) == 0:
            return {}
        time_begin = self._portfolio_history_cash[TOTAL][0][0] if time_begin is None else datetime.datetime.combine(time_begin, datetime.time())
        time_end = self._portfolio_history_cash[TOTAL][-1][0] if time_end is None else datetime.datetime.combine(time_end, datetime.time())

        if time_end <= time_begin:
            return {k: 0 for k, v in self._portfolio_history_cash.items()}

        full_posi = (time_end - time_begin).total_seconds() * self.init_cash
        full_position_rate_info = defaultdict(float)

        for symbol, portfolio_history in self._portfolio_history_cash.items():
            if symbol == TOTAL:
                continue
            last_time, full_posi_amount = time_begin, 0

            for t, port_cash in portfolio_history:
                if t >= time_begin and t < time_end:
                    full_posi_amount += ((t - last_time).total_seconds()) * port_cash
                    last_time = t
            full_position_rate_info[symbol] = full_posi_amount / full_posi
        full_position_rate_info[TOTAL] = sum(full_position_rate_info.values())

        return full_position_rate_info

    def trade(self, time, symbol, price, bid_ask, pos, expect=0, amount_commission=None, volume_commission=None):
        '''
        交易上报
        需要在每次发生交易的时候调用

        time 时间
        symbol 交易代码
        price 价格
        bid_ask 交易瞬间的买卖价差（用于计算交易成本）
        pos 仓位变化（正数为买入，负数为卖出）
        expect 期望收益（仅用于模型计算）
        amount_commission 针对交易额收取手续费
        volume_commission 针对交易量收取手续费，二者不能同时使用
        '''
        if pos == 0:
            return

        # 记录手续费
        assert(amount_commission is None or volume_commission is None)
        if amount_commission is None and volume_commission is None:
            amount_commission = 0
        if amount_commission is not None:
            commission = amount_commission * abs(pos) * price
        if volume_commission is not None:
            commission = amount_commission * abs(pos)
        self.symbol_commission[symbol] += commission

        # 记录胜负
        gross_profit = self.portfolio.get(symbol, 0) * (
            price - self.last_price.get(symbol, price))
        if gross_profit > EPS:
            self.win_cnt[symbol] += 1
        if gross_profit < -EPS:
            self.lose_cnt[symbol] += 1

        # 更新价格利润
        self.update_price_and_profit(time, symbol, price)
        self.profit[symbol] -= commission
        self.symbol_day_profit[time.date()][symbol] -= commission

        # 记录现金持仓变化
        self.portfolio[symbol] += pos
        self.cash -= price * pos + commission
        self.cost[symbol] += bid_ask * abs(pos) / 2 + commission

        # 记录交易
        self.last_price[symbol] = price
        self.trade_cnt[symbol] += 1
        self.trade_amount[symbol][time.date()] += abs(pos) * price
        self.trade_amount[TOTAL][time.date()] += abs(pos) * price

        # 检查剩余资金
        if not initializer.args.disable_check:
            total = -sum(self.profit.values()) + self.cash + sum(
                v * self.real_price[k] for k, v in self.portfolio.items())
            if abs(total - self.init_cash) > 1:
                logging.fatal("total: %s, cash run out\n", total)
                self.show()
                assert(False)

        if expect > 0:
            self.pos_expect[pos > 0][symbol] += expect
        else:
            self.neg_expect[pos > 0][symbol] += -expect

        self.symbol_trade_date[symbol].add(time.date())
        self.symbol_trade_date[TOTAL].add(time.date())
        self._portfolio_history[symbol].append((time, self.portfolio[symbol]))
        self._portfolio_history[TOTAL].append((time, sum(self.portfolio.values())))
        self._portfolio_history_cash[symbol].append((time, self.portfolio[symbol] * price))
        self._portfolio_history_cash[TOTAL].append((time, sum([v[-1][1] for k, v in self._portfolio_history_cash.items() if k != TOTAL])))
        self._profit_history[symbol].append((time, self.profit[symbol]))
        self._profit_history[TOTAL].append((time, self.profit_sum()))

    def scatters(self):
        '''
        画仓位和收益图
        '''
        scatters = []
        for symbol, pf in self._portfolio_history.items():
            scatters.append(go.Scatter(x=numpy.array([t for t, _ in pf]), y=numpy.array([p for _, p in pf]), mode="lines+markers", name="%s_pos" % symbol, yaxis='y2', line={'shape': 'hv'}))
        for symbol, profit in self._profit_history.items():
            scatters.append(go.Scatter(x=numpy.array([t for t, _ in profit]), y=numpy.array([1 + p / self.init_cash for _, p in profit]), mode="lines", name="%s_profit" % symbol))
        return scatters

    def delayed_trade(self, *args):
        self._pending[args[0].date()].append(args)

    def settle(self):
        for date, trades in sorted(self._pending.items()):
            self.day_begin(date)
            for r in sorted(trades):
                self.trade(*r)
                time, symbol, price, bid_ask, pos, expect = r
                logging.info("delayed trade: %s %s %s*%s exp:%s", time, symbol, pos, price, expect)
            for v in self.portfolio.values():
                if v != 0:
                    logging.info(v)
                assert(v == 0)
            self.day_end(date)
        self._pending.clear()

    def day_begin(self, time):
        '''
        每个交易日开始的时候调用
        '''
        self.day_cnt += 1
        if len(self.daily_history) == 0:
            self.daily_history.append((time, self.cash))

    def day_end(self, time):
        '''
        每个交易日结束的时候调用
        '''
        time += datetime.timedelta(days=1)
        assert(time > self.daily_history[-1][0])
        self.daily_history.append((time, sum(self.profit.values()) + self.init_cash))
        for symbol, profit in self.profit.items():
            self._profit_history[symbol].append((time, profit))
        self._profit_history[TOTAL].append((time, self.profit_sum()))

    def close(self, time, symbol, price):
        self.show_and_trade(time, symbol, price, -self.portfolio[symbol], None)

    def non_zero_num(self):
        '''
        持仓交易对象的数量
        '''
        return sum(1 for v in self.portfolio.values() if v)

    def profit_sum(self):
        '''
        总收益
        '''
        return sum(self.profit.values())

    @classmethod
    def interval_profit(cls, time_begin, time_end, daily_history):
        '''
        一段时间内的总收益
        '''
        daily_history_itv = prune_timeline(daily_history, time_begin, time_end + DAY1)
        return daily_history_itv[-1][1] - daily_history_itv[0][1]

    def portfolio_sum(self):
        '''
        持仓总价值
        '''
        return sum(self.real_price.get(k, 0) * v for k, v in self.portfolio.items())

    def profit_rate(self):
        '''
        利润率
        '''
        return self.profit_sum() / self.init_cash


def sample_trader():
    '''
    使用样例
    '''
    import plotly

    trader = Trader()

    trader.day_begin(datetime.date(2018, 11, 4))
    trader.trade(datetime.datetime(2018, 11, 4, 10, 20), "btc", 100.5, 0.1, 1300.53)  # btc 开仓
    trader.trade(datetime.datetime(2018, 11, 4, 10, 21), "xrp", 1.02, 0.01, 32768)  # xrp 开仓
    trader.trade(datetime.datetime(2018, 11, 4, 15, 5), "btc", 113.6, 0.05, -700)  # btc 关仓
    trader.day_end(datetime.date(2018, 11, 4))

    trader.day_begin(datetime.date(2018, 11, 5))
    trader.trade(datetime.datetime(2018, 11, 5, 21, 25), "btc", 113.6, 0.05, -600.53)  # btc 关仓
    trader.day_end(datetime.date(2018, 11, 5))

    logging.info(trader.show())
    layout = go.Layout(title='sample', yaxis={'title': 'rate'}, yaxis2={'title': 'portfolio', 'overlaying': 'y', 'side': 'right'})
    plotly.offline.plot({'data': trader.scatters(), 'layout': layout}, filename='sample.html')


if __name__ == "__main__":
    from publib.initializer import initializer
    initializer.init()
    sample_trader()
