from blueshift.api import order, symbol, order_target
from functools import partial
from blueshift.api import symbol, order_target, get_datetime, terminate, on_data, on_trade, off_data, off_trade
import talib
import numpy as np
 
 
def initialize(context):
    context.traded = False
    context.entry_price = {}
    context.order_monitors = {}
    context.order_data = {}
    context.data_monitors = {}
    context.stoploss_price = {}
    context.stock = [symbol('ETSY'), symbol('AMD'), symbol('TSLA'), symbol(
        'TWLO'), symbol('ZIOP'), symbol('ROKU'), symbol('NKE'), symbol('DIS')]
    context.i = 0
    context.invested = False
    context.candle_rankings = {'CDL3LINESTRIKEBull': 1,
        'CDL3LINESTRIKEBear': 2, 'CDLEVENINGSTARBull': 4,
        'CDLEVENINGSTARBear': 4, 'CDLTASUKIGAPBull': 5, 'CDLTASUKIGAPBear':
        5, 'CDLINVERTEDHAMMERBull': 6, 'CDLINVERTEDHAMMERBear': 6,
        'CDLMATCHINGLOWBull': 7, 'CDLMATCHINGLOWBear': 7,
        'CDLABANDONEDBABYBull': 8, 'CDLABANDONEDBABYBear': 8,
        'CDLBREAKAWAYBull': 10, 'CDLBREAKAWAYBear': 10,
        'CDLMORNINGSTARBull': 12, 'CDLMORNINGSTARBear': 12,
        'CDLPIERCINGBull': 13, 'CDLPIERCINGBear': 13,
        'CDLSTICKSANDWICHBull': 14, 'CDLSTICKSANDWICHBear': 14,
        'CDLTHRUSTINGBull': 15, 'CDLTHRUSTINGBear': 15, 'CDLINNECKBull': 17,
        'CDLINNECKBear': 17, 'CDLENGULFINGBear': 18, 'CDLENGULFINGBull': 18}
    context.candle_names = ['CDL3LINESTRIKE', 'CDLEVENINGSTAR',
        'CDLTASUKIGAP', 'CDLINVERTEDHAMMER', 'CDLMATCHINGLOW',
        'CDLABANDONEDBABY', 'CDLBREAKAWAY', 'CDLMORNINGSTAR', 'CDLPIERCING',
        'CDLSTICKSANDWICH', 'CDLTHRUSTING', 'CDLINNECK', 'CDLENGULFING']
    context.candle_days = {'CDL3LINESTRIKEBull': 4, 'CDL3LINESTRIKEBear': 4,
        'CDLEVENINGSTARBull': 3, 'CDLEVENINGSTARBear': 3,
        'CDLTASUKIGAPBull': 3, 'CDLTASUKIGAPBear': 3,
        'CDLINVERTEDHAMMERBull': 2, 'CDLINVERTEDHAMMERBear': 2,
        'CDLMATCHINGLOWBull': 2, 'CDLMATCHINGLOWBear': 2,
        'CDLABANDONEDBABYBull': 3, 'CDLABANDONEDBABYBear': 3,
        'CDLBREAKAWAYBull': 5, 'CDLBREAKAWAYBear': 5, 'CDLMORNINGSTARBull':
        3, 'CDLMORNINGSTARBear': 3, 'CDLPIERCINGBull': 2, 'CDLPIERCINGBear':
        2, 'CDLSTICKSANDWICHBull': 3, 'CDLSTICKSANDWICHBear': 3,
        'CDLTHRUSTINGBull': 2, 'CDLTHRUSTINGBear': 2, 'CDLINNECKBull': 2,
        'CDLINNECKBear': 2, 'CDLENGULFINGBear': 2, 'CDLENGULFINGBull': 2}
 
 
def handle_data(context, data):
    for stocki in context.stock:
        df = data.history(stocki, ['open', 'high', 'low', 'close'], 6, '1m')
        op = df['open']
        hi = df['high']
        lo = df['low']
        cl = df['close']
        op = np.array(op, dtype=float)
        hi = np.array(hi, dtype=float)
        lo = np.array(lo, dtype=float)
        cl = np.array(cl, dtype=float)
        df['CDL3LINESTRIKE'] = talib.CDL3LINESTRIKE(op, hi, lo, cl)
        df['CDLEVENINGSTAR'] = talib.CDLEVENINGSTAR(op, hi, lo, cl)
        df['CDLTASUKIGAP'] = talib.CDLTASUKIGAP(op, hi, lo, cl)
        df['CDLINVERTEDHAMMER'] = talib.CDLINVERTEDHAMMER(op, hi, lo, cl)
        df['CDLMATCHINGLOW'] = talib.CDLMATCHINGLOW(op, hi, lo, cl)
        df['CDLABANDONEDBABY'] = talib.CDLABANDONEDBABY(op, hi, lo, cl)
        df['CDLBREAKAWAY'] = talib.CDLBREAKAWAY(op, hi, lo, cl)
        df['CDLMORNINGSTAR'] = talib.CDLMORNINGSTAR(op, hi, lo, cl)
        df['CDLPIERCING'] = talib.CDLPIERCING(op, hi, lo, cl)
        df['CDLSTICKSANDWICH'] = talib.CDLSTICKSANDWICH(op, hi, lo, cl)
        df['CDLTHRUSTING'] = talib.CDLTHRUSTING(op, hi, lo, cl)
        df['CDLINNECK'] = talib.CDLINNECK(op, hi, lo, cl)
        df['CDLENGULFING'] = talib.CDLENGULFING(op, hi, lo, cl)
        df['candlestick_pattern'] = np.nan
        df['candlestick_match_count'] = np.nan
        for index, row in df.iterrows():
            if len(row[context.candle_names]) - sum(row[context.
                candle_names] == 0) == 0:
                df.loc[index, 'candlestick_pattern'] = 'NO_PATTERN'
                df.loc[index, 'candlestick_match_count'] = 0
            elif len(row[context.candle_names]) - sum(row[context.
                candle_names] == 0) == 1:
                patterns = []
                for i in context.candle_names:
                    if row[i] != 0:
                        patterns.append(i)
                        break
                if any(row[context.candle_names].values > 0):
                    df.loc[index, 'candlestick_pattern'] = patterns[0] + 'Bull'
                    df.loc[index, 'candlestick_match_count'] = 1
                else:
                    df.loc[index, 'candlestick_pattern'] = patterns[0] + 'Bear'
                    df.loc[index, 'candlestick_match_count'] = 1
            else:
                patterns = []
                for i in context.candle_names:
                    if row[i] != 0:
                        patterns.append(i)
                container = []
                for pattern in patterns:
                    if row[pattern] > 0:
                        container.append(pattern + 'Bull')
                    else:
                        container.append(pattern + 'Bear')
                rank_list = [context.candle_rankings[p] for p in container]
                if len(rank_list) == len(container):
                    rank_index_best = rank_list.index(min(rank_list))
                    df.loc[index, 'candlestick_pattern'] = container[
                        rank_index_best]
                    df.loc[index, 'candlestick_match_count'] = len(container)
        df.drop(context.candle_names, axis=1, inplace=True)
        pattern_name = df.iloc[5]['candlestick_pattern']
        if pattern_name != 'NO_PATTERN':
            k = context.candle_days[pattern_name]
        stoplossprice = 0
        if pattern_name[-4:] == 'Bull':
            stoplossprice = df.iloc[-k:]['low'].min()
            order_id = order_target(stocki, 20)
            f = partial(check_order, order_id, stocki, stoplossprice, 'Bull')
            context.order_monitors[stocki] = f
            on_trade(f)
        if pattern_name[-4:] == 'Bear':
            stoplossprice = df.iloc[-k:]['high'].max()
            order_id = order_target(stocki, -20)
            f = partial(check_order, order_id, stocki, stoplossprice, 'Bear')
            context.order_monitors[stocki] = f
            on_trade(f)
 
 
def check_order(order_id, asset, stoplossprice, trend, context, data):
    orders = context.orders
    if order_id in orders:
        order = orders[order_id]
        if order.pending > 0:
            return
        context.entry_price[asset] = order.average_price
        context.stoploss_price[asset] = stoplossprice
        g = partial(check_exit, asset, trend)
        context.order_data[asset] = g
        on_data(g)
        off_trade(context.order_monitors[asset])
 
 
def check_exit(asset, trend, context, data):
    px = data.current(asset, 'close')
    if context.stoploss_price[asset] > px and trend == 'BULL':
        order_target(asset, 0)
        off_data(context.order_data[asset])
    elif context.stoploss_price[asset] < px and trend == 'Bear':
        order_target(asset, 0)
        off_data(context.order_data[asset])
 
