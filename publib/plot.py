#encoding=utf-8

import os
import plotly
import datetime
import logging
import numpy
import plotly.graph_objs as go

from publib.initializer import initializer


initializer.add_argument("--plot_sample", default=300, type=int)


def timeline_sample(timeline):
    ''' 时间线抽样 '''
    sampled = []
    step = datetime.timedelta(seconds=initializer.args.plot_sample)
    for t, v in timeline:
        if len(sampled) == 0 or t - sampled[-1][0] >= step:
            sampled.append((t, v))
    return sampled


def plot_timeline(timeline, title):
    ''' 用时间线抽样生成 scatter '''
    return plot_pair_list(timeline_sample(timeline), title)


def plot_pair_list(pair_list, title):
    ''' 用时间线生成 scatter '''
    return go.Scatter(x=numpy.array([t for t, _ in pair_list]), y=numpy.array([mid for t, mid in pair_list]), mode="lines", name=title)


def easy_plot(scatters, title="untitled", store_path="/data1/tmp/"):
    ''' 一行代码打印一张图，如果在 44 上，可以直接从互联网查看 '''
    filename = title + datetime.datetime.now().strftime("_%Y%m%d%H%M%S.html")
    full_path = os.path.join(store_path, filename)
    plotly.offline.plot({"data": scatters}, filename=full_path)
    logging.info("plot to %s", full_path)

    if store_path == "/data1/tmp/":
        logging.info("if on 44 http://47.75.204.44:9991/view/public/%s", filename)
