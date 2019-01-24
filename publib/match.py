# coding:utf-8
import logging
import json
import time 
import copy
class MakingMatch:
    '''
    撮合引擎
    '''
    def __init__(self, quote_a = None, quote_b = None, money_move_loop = None):
        self.quote_a = quote_a
        self.quote_b = quote_b
        self.money_move_loop = money_move_loop
        

    def match_loop_step(self, idx_asks, idx_bids, amount_coin_current, price_asks, price_bids, amount_move_loop, amount_cum, cum_asks_loop, cum_bids_loop, asks_left_loop, bids_left_loop, asks_list, bids_list):
        '''
            环路撮合的单个步骤
            params:
                amount_coin_current :  当前asks_list中某项币数量
                amount_money_current ： 当前asks_list某项钱的数量
                price_asks, 
                price_bids, 
                money_move_loop : 一次环路所需要的钱
                money_cum  : 累计消耗的钱 
                cum_asks_loop : 环路累计的买单 
                cum_bids_loop : 环路累计的卖单
                asks_left_loop : 剩余的卖单
                bids_left_loop : 剩余的买单
                asks_list :
                bids_list : 
            return:
                exit_loop 是否结束loop的撮合
        '''
        exit_loop = False
        #交易额满足money_move_loop
        amount_left_loop = amount_move_loop - amount_cum
        if amount_left_loop == amount_coin_current:
            logging.debug("本次idx_bids[%s] idx_asks[%s]的撮合单量正好满足loop余量[%s]" % (idx_bids, idx_asks, amount_coin_current))
            exit_loop = True
            #吃掉的单
            cum_asks_loop.append({'size':amount_coin_current, 'price':price_asks})
            cum_bids_loop.append({'size':amount_coin_current, 'price':price_bids})
            #剩余的单
            asks_left_loop = copy.deepcopy(asks_list)
            bids_left_loop = copy.deepcopy(bids_list)
        elif amount_left_loop > amount_coin_current:
            logging.debug("本次idx_bids[%s] idx_asks[%s]的撮合单量[%s]仍然不满足loop余量[%s]" % (idx_bids, idx_asks, amount_coin_current, amount_left_loop))
            #吃掉的单
            cum_asks_loop.append({'size':amount_coin_current, 'price':price_asks})
            cum_bids_loop.append({'size':amount_coin_current, 'price':price_bids})
        else:
            logging.debug("本次idx_bids[%s] idx_asks[%s]的撮合单量[%s]超过loop余量[%s]" % (idx_bids, idx_asks, amount_coin_current, amount_left_loop))
            exit_loop = True
            #吃掉的单
            cum_asks_loop.append({'size':amount_left_loop, 'price':price_asks})
            cum_bids_loop.append({'size':amount_left_loop, 'price':price_bids})
            #剩余的单
            asks_left_loop = copy.deepcopy(asks_list)
            asks_left_loop[idx_asks]['size'] += amount_coin_current - amount_left_loop
            bids_left_loop = copy.deepcopy(bids_list)
            bids_left_loop[idx_bids]['size'] += amount_coin_current - amount_left_loop
        return exit_loop, cum_asks_loop, cum_bids_loop, asks_left_loop, bids_left_loop
        

    def match_pct_step(self,idx_asks, idx_bids, amount_coin_current, price_asks, price_bids, amount_coin, amount_cum, amount_money_max, money_cum, huilv_asks, cum_asks_pct, cum_bids_pct, asks_left_pct, bids_left_pct, asks_list, bids_list):
        '''
        按照指定pct阈值进行撮合的单个步骤
        params:
            amount_coin_current : 当前asks_list某项钱的数量
            price_asks, 
            price_bids, 
            amount_coin : pmb中可消耗的币的数量
            amount_money : pma中可消耗的钱的数量
            amount_money_max : 非环路一次对冲的最大的钱的数量
            money_cum : 累计的钱的数量
            huilv_asks, 
            cum_asks_pct, 
            cum_bids_pct, 
            asks_left_pct, 
            bids_left_pct, 
            asks_list, 
            bids_list
        return:

        '''
        exit_pct = False
        #通过钱的数量和币的数量确定交易量
        size_take = min(amount_coin_current, (amount_coin - amount_cum), (amount_money_max - money_cum) * huilv_asks / price_asks)
        #吃单
        cum_asks_pct.append({'size':size_take, 'price':price_asks})
        cum_bids_pct.append({'size':size_take, 'price':price_bids})
        '''
        (amount_coin-amount_cum) 为剩余的单 如果当前的单大于剩余的单则可停 即 amount_coin_current -(amount_coin-amount_cum) > 0
        残余订单
            钱足 币足
            amount_delta > 0 . amount_coin_current < (amount_coin-amount_cum) , amount_coin_current < (amount_money_max-money_cum)/huilv_asks/item_asks['price']
            钱不足 币足
            amount_delta = amount_coin_current - (amount_money_max-money_cum)/huilv_asks/item_asks['price']. amount_coin_current < (amount_coin-amount_cum)
            钱足 币不足
            amount_delta = amount_coin_current - (amount_coin-amount_cum).  amount_coin_current < (amount_money_max-money_cum)/huilv_asks/item_asks['price']
            钱 币 都不足
            amount_delta = max(amount_coin_current - (amount_coin-amount_cum), amount_coin_current - (amount_money_max-money_cum)/huilv_asks/item_asks['price'])
        '''
        amount_delta = max(0, amount_coin_current - (amount_coin-amount_cum), amount_coin_current - (amount_money_max-money_cum) * huilv_asks / price_asks)
        if amount_delta > 0:
            exit_pct = True
            asks_left_pct = copy.deepcopy(asks_list)
            bids_left_pct = copy.deepcopy(bids_list)
            asks_left_pct[idx_asks]['size'] += amount_delta
            bids_left_pct[idx_bids]['size'] += amount_delta
            return exit_pct, cum_asks_pct, cum_bids_pct, asks_left_pct, bids_left_pct
        else:
            return exit_pct, cum_asks_pct, cum_bids_pct, asks_left_pct, bids_left_pct

    def match_normal(self, coin, bids_list, asks_list, amount_coin, amount_money_max, amount_move_loop,  min_pct_move, side, huilv_asks = None, huilv_bids = None):
        '''
        供搬砖用
        交易撮合: 对冲量为amount_one的买卖单撮合及其价差比例
        param:
             bids_list, 
             asks_list, 
             amount_coin : pmb中可以使用的币的数量
             amount_money : min(pma中可以使用的钱的数量,一次最大交易量)
             money_move_loop : 环路一次对冲的钱
             min_pct_move :需要满足非环路的最小的pct
             huilv_asks = None, 
             huilv_bids = None
        return:
            match返回结果：
                {
                    "status":"ok",
                    "pct_limit":{
                        "pct_td":pec_new
                        "pct_last":最后一单撮合的pct,
                        "pct":综合pct
                        "income":本次对冲收入
                        "amount_coin":满足了pct_limit时的coin数量
                        "amount_money":满足了pct_limit时的money数量
                        "price_asks":最后一单卖出价
                        "price_bids":最后一单的买入价
                        "price_asks_uni":最后一单卖出价 -- 原始平台价格
                        "price_bids_uni":最后一单的买入价 -- 原始平台价格
                        "asks":[],#吃的asks单
                        "bids":[],#吃的bids单
                        "asks_left":#吃剩的asks单 
                        "bids_left":#吃剩的bids单,
                        "msg_deal":"coin[%s] go单方向利差:%.3f 阀值[%s] income:%.4f price-go:asks[%s %s]-[%s %s]bids.  at[%s] 第[%d]次"
                    },
                    "loop_limit":{ #固定1200美元对冲量
                        "pct_last":最后一单撮合的pct,
                        "pct":综合pct 
                        "income":本次对冲收入
                        "amount_coin":满足了1200美元对冲量时的coin数量
                        "amount_money":1200
                        "price_asks":最后一单卖出价
                        "price_bids":最后一单的买入价
                        "price_asks_uni":最后一单卖出价 -- 原始平台价格
                        "price_bids_uni":最后一单的买入价 -- 原始平台价格
                        "asks":[],#吃的asks单
                        "bids":[],#吃的bids单
                        "asks_left":#吃剩的asks单 
                        "bids_left":#吃剩的bids单
                        "msg_deal":"coin[%s] go单方向利差:%.3f 阀值[%s] income:%.4f price-go:asks[%s %s]-[%s %s]bids.  at[%s] 第[%d]次"
                    }
                }
        ''' 
        ret_match = { "status":"ok", "pct_limit": {}, "loop_limit": {} }
        #吃单的数量和钱的数量
        amount_cum = money_cum = 0.
        exit_loop = False; exit_pct = False
        #记录吃单明细
        cum_bids_loop = []; cum_asks_loop = []
        cum_bids_pct = []; cum_asks_pct = []
        #记录剩余单量
        bids_left_loop = []; asks_left_loop = []
        bids_left_pct = []; asks_left_pct = []
        #利差情况
        msg_deal_loop = None; msg_deal_pct = None
        idx_bids = idx_asks = 0    
        price_bids = price_asks = 0.
        if side =='a2b' :
            quote_a = self.quote_a
            quote_b =self.quote_b
        else:
            quote_a = self.quote_b
            quote_b =self.quote_a
        while idx_bids < len(bids_list) and idx_asks < len(asks_list):
            #debug 把买卖单信息打印出来
            if idx_bids == 0 and idx_asks == 0:
                logging.debug("bids单: %s" % json.dumps(bids_list[:8]))
                logging.debug("asks单: %s" % json.dumps(asks_list[:8]))
            item_bids = bids_list[idx_bids]
            size_bids = item_bids['size']; price_bids = item_bids['price']
            item_asks = asks_list[idx_asks]
            size_asks = item_asks['size']; price_asks = item_asks['price']
            logging.debug("吃单 idx[%d] size_bids[%s] price_bids[%s]" % (idx_bids, size_bids, price_bids))
            logging.debug("卖单 idx[%d] size_asks[%s] price_asks[%s] " % (idx_asks, size_asks, price_asks))
            #当前撮合步骤的币量和钱量
            amount_coin_current = min(size_asks, size_bids)
            amount_money_current = amount_coin_current * price_asks / huilv_asks
            pct_current = (item_bids['price']/huilv_bids - item_asks['price']/huilv_asks) * 100.0 / (item_asks['price']/huilv_asks)
            logging.debug("amount_coin_current[%s] amount_money_current[%s] pct_current[%s]" % (amount_coin_current, amount_money_current, pct_current))
            #退出控制
            if exit_loop and exit_pct:
                logging.debug("退出撮合")
                break
                
            #开始撮合
            if size_asks < size_bids:
                bids_list[idx_bids]['size'] = size_bids - size_asks
                asks_list[idx_asks]['size'] = 0
                logging.debug("撮合 买入单量[%s]大于卖出单量[%s] 撮合吃单量并将吃单量更新到[%s] 下一步用下一个[%d]卖单来撮合吃单余量 当前撮合步骤的币量[%s]和钱量[%s]" % (size_bids, size_asks, bids_list[idx_bids]['size'], idx_asks+1, amount_coin_current, amount_money_current))
                #通过满足loop交易量限制的撮合
                if not exit_loop:
                    exit_loop, cum_asks_loop, cum_bids_loop, asks_left_loop, bids_left_loop = self.match_loop_step(idx_asks, idx_bids, amount_coin_current, price_asks, price_bids, amount_move_loop, amount_cum, cum_asks_loop, cum_bids_loop, asks_left_loop, bids_left_loop, asks_list, bids_list)           
                #通过TD_GO限制pct的撮合
                if not exit_pct:
                    if pct_current < min_pct_move:
                        exit_pct = True
                    else:                                                                                           
                        exit_pct, cum_asks_pct, cum_bids_pct, asks_left_pct, bids_left_pct = self.match_pct_step(idx_asks, idx_bids, amount_coin_current, price_asks, price_bids, amount_coin, amount_cum, amount_money_max, money_cum, huilv_asks, cum_asks_pct, cum_bids_pct, asks_left_pct, bids_left_pct, asks_list, bids_list)
                idx_asks += 1
            elif size_asks > size_bids:
                asks_list[idx_asks]['size'] = size_asks - size_bids
                bids_list[idx_bids]['size'] = 0
                logging.debug("撮合 卖出单量[%s]大于买入单量[%s] 撮合卖单量并将卖单量更新到[%s] 下一步用下一个[%d]吃单来撮合卖单余量 当前撮合步骤的币量[%s]和钱量[%s]" % (size_asks, size_bids, asks_list[idx_asks]['size'], idx_bids+1, amount_coin_current, amount_money_current))
                #通过满足loop交易量限制的撮合
                if not exit_loop:
                    exit_loop, cum_asks_loop, cum_bids_loop, asks_left_loop, bids_left_loop = self.match_loop_step(idx_asks, idx_bids, amount_coin_current, price_asks, price_bids, amount_move_loop, amount_cum, cum_asks_loop, cum_bids_loop, asks_left_loop, bids_left_loop, asks_list, bids_list)           
                if not exit_pct:
                    if pct_current < min_pct_move:
                        exit_pct = True
                    else:                                                                                           
                        exit_pct, cum_asks_pct, cum_bids_pct, asks_left_pct, bids_left_pct = self.match_pct_step(idx_asks, idx_bids, amount_coin_current, price_asks, price_bids, amount_coin, amount_cum, amount_money_max, money_cum, huilv_asks, cum_asks_pct, cum_bids_pct, asks_left_pct, bids_left_pct, asks_list, bids_list)
                idx_bids += 1
            else:
                asks_list[idx_asks]['size'] = 0
                bids_list[idx_bids]['size'] = 0
                logging.debug("撮合 卖出买入单相同，都是[%s] 下一步用下一个吃单[%d]和卖单[%d] 当前撮合步骤的币量[%s]和钱量[%s]" % (size_bids, idx_bids+1, idx_asks+1, amount_coin_current, amount_money_current))
                #通过满足loop交易量限制的撮合
                if not exit_loop:
                    exit_loop, cum_asks_loop, cum_bids_loop, asks_left_loop, bids_left_loop = self.match_loop_step(idx_asks, idx_bids, amount_coin_current, price_asks, price_bids, amount_move_loop, amount_cum, cum_asks_loop, cum_bids_loop, asks_left_loop, bids_left_loop, asks_list, bids_list)           
                #通过TD_GO限制pct的撮合
                if not exit_pct:
                    if pct_current < min_pct_move:
                        exit_pct = True
                    else:                                                                                           
                        exit_pct, cum_asks_pct, cum_bids_pct, asks_left_pct, bids_left_pct = self.match_pct_step(idx_asks, idx_bids, amount_coin_current, price_asks, price_bids, amount_coin, amount_cum, amount_money_max, money_cum, huilv_asks, cum_asks_pct, cum_bids_pct, asks_left_pct, bids_left_pct, asks_list, bids_list)
                idx_bids += 1
                idx_asks += 1
            #记录撮合总量
            money_cum += amount_money_current
            amount_cum += amount_coin_current
            if idx_bids >= len(bids_list) or idx_asks >= len(asks_list):
                exit_loop = True
                exit_pct = True

            if exit_loop and msg_deal_loop == None:
                income = pct_current * self.money_move_loop / 100.
                msg_deal_loop = "coin[%s] go单方向利差:%.3f 交易额度[%s]  income:%.4f price-go:asks[%s %s]-[%s %s]bids.  at[%s] " % \
                (coin, pct_current, self.money_move_loop, income, price_asks, quote_a, price_bids, quote_b, time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time())))
                logging.info("exit_loop的msg_deal_loop结果为%s" % msg_deal_loop)
                ret_match['loop_limit'] = {
                    'side' : side,
                    'type':'loop_limit',
                    "coin": coin,
                    "pct":pct_current,
                    "income":income,
                    "amount_coin":round(sum([size_t['size'] for size_t in cum_asks_loop]), 6),
                    "amount_money":self.money_move_loop,
                    "price_asks":price_asks,
                    "price_bids":price_bids,
                    "price_asks_uni":price_asks/huilv_asks,
                    "price_bids_uni":price_bids/huilv_bids,
                    "asks":cum_asks_loop,#吃的asks单
                    "bids":cum_bids_loop,#吃的bids单
                    "asks_left":asks_left_loop,#吃剩的asks单 
                    "bids_left":bids_left_loop,#吃剩的bids单
                    "msg_deal":msg_deal_loop
                }
            if exit_pct and msg_deal_pct == None:
                if len(cum_asks_pct) < 1:
                    ret_match['pct_limit'] = {
                        'side' : side,
                        'type':'pct_limit',
                        "coin": coin,
                        "pct_td":min_pct_move,
                        "pct":pct_current,
                        "income":0,
                        "amount_coin":0,
                        "amount_money":0,
                        "price_asks":price_asks,
                        "price_bids":price_bids,
                        "price_asks_uni":price_asks/huilv_asks,
                        "price_bids_uni":price_bids/huilv_bids,
                        "asks":[],#吃的asks单
                        "bids":[],#吃的bids单
                        "asks_left":asks_left_pct,
                        "bids_left":bids_left_pct,#吃剩的bids单,
                        "msg_deal":"pct[%s]不符合阈值要求[%s]" % (pct_current, min_pct_move)
                    }
                else:
                    amount_money = round(sum([p['price'] / huilv_asks * p['size'] for p in cum_asks_pct]), 6)
                    income = pct_current * amount_money / 100.
                    msg_deal_pct = "coin[%s] go单方向利差:%.3f 交易额度[%s] 阀值[%s] income:%.4f price-go:asks[%s %s]-[%s %s]bids.  at[%s] " % \
                    (coin, pct_current, amount_money, min_pct_move, income, price_asks, quote_a, price_bids, quote_b, time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time())))
                    logging.debug("exit_pct的msg_deal_pct结果为%s" % msg_deal_pct)
                    ret_match['pct_limit'] = {
                        'side' : side,
                        'type':'pct_limit',
                        "coin": coin,
                        "pct_td":min_pct_move,
                        "pct":pct_current,
                        "income":income,
                        "amount_coin":round(sum([p['size'] for p in cum_bids_pct]), 6),
                        "amount_money":amount_money,
                        "price_asks":price_asks,
                        "price_bids":price_bids,
                        "price_asks_uni":price_asks/huilv_asks,
                        "price_bids_uni":price_bids/huilv_bids,
                        "asks":cum_asks_pct,#吃的asks单
                        "bids":cum_bids_pct,#吃的bids单
                        "asks_left":asks_left_pct,
                        "bids_left":bids_left_pct,#吃剩的bids单,
                        "msg_deal":msg_deal_pct
                    }
        
        return ret_match

    def match_contract(self, bids_list, asks_list, amount_one, huilv_asks = None, huilv_bids = None):
        '''
        交易撮合: 对冲量为amount_one的买卖单撮合及其价差比例
        param:
             bids_list, 
             asks_list, 
             amount_one:必须达到的对冲量
        return:
            ret_a2b = {
                    "status":"ok/failed",
                    "amount":可搬砖量,
                    "pct":综合利差率，按市价成交,
                    "price_asks":最高卖单价，不高于这个价搬砖买入即符合利差要求,
                    "price_bids":最低吃单价，不低于这个价搬砖卖出即符合利差要求,
                    "cum_bids":[吃单列表],
                    "cum_asks":[卖单列表]，
                }
        ''' 
        amount = 0.
        cum_bids = []; cum_asks = []
        idx_bids = idx_asks = 0
        price_bids = price_asks = 0.
        while idx_bids < len(bids_list) and idx_asks < len(asks_list):
            item_bids = bids_list[idx_bids]
            size_bids = item_bids['size']; price_bids = item_bids['price']
            item_asks = asks_list[idx_asks]
            size_asks = item_asks['size']; price_asks = item_asks['price']
            logging.debug("吃单 idx[%d] size_bids[%s] price_bids[%s]" % (idx_bids, size_bids, price_bids))
            logging.debug("卖单 idx[%d] size_asks[%s] price_asks[%s] " % (idx_asks, size_asks, price_asks))
            #退出判断：达到了最大交易量
            if amount > amount_one:
                logging.warning("撮合退出: 超过了一次指定交易量. amount[%s] amount_one[%s] idx_bids[%d] idx_asks[%d]" % (amount, amount_one, idx_bids, idx_asks))
                break
            if amount == amount_one:
                logging.info("撮合退出: 达到了最大交易量. amount[%s] amount_one[%s] idx_bids[%d] idx_asks[%d]" % (amount, amount_one, idx_bids, idx_asks))
                break
            #debug 把买卖单信息打印出来
            if idx_bids == 0 and idx_asks == 0:
                logging.debug("bids单: %s" % json.dumps(bids_list[:8]))
                logging.debug("asks单: %s" % json.dumps(asks_list[:8]))
            amount_left = amount_one - amount
            if size_asks >= amount_left and size_bids >= amount_left:
                cum_bids.append( { "size":amount_left, "price": price_bids } )
                cum_asks.append( { "size":amount_left, "price": price_asks } )
                asks_list[idx_asks]['size'] -= amount_left
                bids_list[idx_bids]['size'] -= amount_left
                amount += amount_left
                logging.debug("撮合 卖单[%s]和吃单[%s] 都大于amount_max_left[%s] 下一步退出" % (size_asks, size_bids, amount_left))
            elif size_asks < size_bids:
                cum_bids.append( { "size":size_asks, "price": price_bids } )
                cum_asks.append( { "size":size_asks, "price": price_asks } )
                bids_list[idx_bids]['size'] = size_bids - size_asks
                asks_list[idx_asks]['size'] = 0
                idx_asks += 1
                amount += size_asks
                logging.debug("撮合 吃单量[%s]大于卖单量[%s] 撮合吃单量并将吃单量更新到[%s] 下一步用下一个[%d]卖单来撮合吃单余量" % (size_bids, size_asks, bids_list[idx_bids]['size'], idx_asks))
            elif size_asks > size_bids:
                cum_bids.append( { "size":size_bids, "price":price_bids } )
                cum_asks.append( { "size":size_bids, "price":price_asks } )
                asks_list[idx_asks]['size'] = size_asks - size_bids
                bids_list[idx_bids]['size'] = 0
                idx_bids += 1
                amount += size_bids
                logging.debug("撮合 卖单量[%s]大于吃单量[%s] 撮合卖单量并将卖单量更新到[%s] 下一步用下一个[%d]吃单来撮合卖单余量" % (size_asks, size_bids, asks_list[idx_asks]['size'], idx_bids))
            else:
                cum_bids.append( { "size":size_bids, "price":price_bids } )
                cum_asks.append( { "size":size_bids, "price":price_asks } )
                asks_list[idx_asks]['size'] = 0
                bids_list[idx_bids]['size'] = 0
                idx_bids += 1
                idx_asks += 1
                amount += size_asks
                logging.debug("撮合 吃单卖单相同，都是[%s] 下一步用下一个吃单[%d]和卖单[%d]" % (size_bids, idx_bids, idx_asks))
            logging.debug("当前cum_bids:%s.  cum_asks:%s" % (cum_bids, cum_asks))
        ret_match = { "status":"ok", "price_asks":price_asks, "price_bids":price_bids, "cum_bids":cum_bids, "cum_asks":cum_asks }
        
        #统一汇率后，计算统一quote下的价格和利差
        ret_match['price_asks_uni'] = price_asks / huilv_asks
        ret_match['price_bids_uni'] = price_bids / huilv_bids
        ret_match['bids'] = bids_list
        ret_match['asks'] = asks_list
        ret_match['pct'] = (ret_match['price_bids_uni'] - ret_match['price_asks_uni']) * 100. / ret_match['price_asks_uni']
        return ret_match