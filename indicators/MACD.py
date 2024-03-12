import pandas as pd
import numpy as np
from pandas import DataFrame
from datetime import datetime
import asyncio
from datetime import datetime, timezone
import logging

import tortoise

from db_app.models import KlineData, AggregatedTradeData
from db_app.config import db_config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

file_handler = logging.FileHandler('macd.log')
file_handler.setLevel(logging.INFO)

file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

logger.addHandler(file_handler)

pd.set_option("display.max_rows", 10000)
pd.set_option("display.max_columns", 10000)
pd.set_option("display.width", 1000)


def search_macd(klins_data: list, short_period: int = 12, long_period: int = 26, k_period: int = 12, d_period: int = 3):
    df = pd.DataFrame(klins_data).iloc[:, :5]
    df.columns = ["open_time", "open", "high", "low", "close"]

    # MACD Calculation
    df["ma_fast"] = df["close"].ewm(span=short_period, adjust=False).mean()
    df["ma_slow"] = df["close"].ewm(span=long_period, adjust=False).mean()
    df["macD"] = df["ma_fast"] - df["ma_slow"]
    df["signal"] = df["macD"].ewm(span=9, adjust=False).mean()
    df['n_high'] = df['high'].rolling(k_period).max()
    df['n_low'] = df['low'].rolling(k_period).min()
    df['%K'] = (df['close'] - df['n_low']) * 100 / (df['n_high'] - df['n_low'])
    df['%D'] = df['%K'].rolling(d_period).mean()

    # Crossover calculation for MACD
    df['macd_crossover'] = 0
    df.loc[(df['macD'] > df['signal']) & (df['macD'].shift(1) <= df['signal'].shift(1)), 'macd_crossover'] = 1
    df.loc[(df['macD'] < df['signal']) & (df['macD'].shift(1) >= df['signal'].shift(1)), 'macd_crossover'] = -1
    # Crossover calculation for Stochastic
    df['stochastic_crossover'] = 0
    df.loc[(df['%K'] > df['%D']) & (df['%K'].shift(1) <= df['%D'].shift(1)), 'stochastic_crossover'] = 1
    df.loc[(df['%K'] < df['%D']) & (df['%K'].shift(1) >= df['%D'].shift(1)), 'stochastic_crossover'] = -1

    last_row = df.iloc[-1]
    # print(df.tail(1000))

    return last_row


async def find_profit(
        signal_date_to_increase: datetime,
        signal_date_to_decrease: datetime,
) -> tuple:
    await tortoise.Tortoise.init(db_config)
    klines_for_certain_period = await KlineData.filter(
        open_time__gte=signal_date_to_increase,
        close_time__lte=signal_date_to_decrease
    ).order_by("open_time")

    await tortoise.Tortoise.close_connections()

    signal_kline = klines_for_certain_period[0]

    logger.info(f"klines_for_certain_period: {klines_for_certain_period}")

    if len(klines_for_certain_period) < 2:
        return signal_kline.open_time, signal_kline.high_price, signal_kline.high_price, -0.99999

    max_price = max(kline.high_price for kline in klines_for_certain_period[1:])
    logger.info(f"Max price: {max_price} for {signal_kline.open_time}")
    on_signal_bought_price = signal_kline.open_price
    profit = max_price - on_signal_bought_price
    logger.info(f"Result for {signal_kline.open_time}: {signal_kline.open_time, on_signal_bought_price, max_price, profit}")
    return signal_kline.open_time, on_signal_bought_price, max_price, profit


async def analyse_ginals(signals_list: list[dict]):
    if signals_list[0].get("crossover") == -1:
        logger.info(f"Deleted first signal: {signals_list[0]}")
        signals_list.pop(0)

    results = []

    while len(signals_list) >= 2:
        increase_signal = signals_list.pop(0)
        decrease_signal = signals_list.pop(0)
        result = await find_profit(
            signal_date_to_increase=increase_signal["open_time"],
            signal_date_to_decrease=decrease_signal["open_time"],
        )
        results.append(result)
        logger.info(f"Sending request for {increase_signal['open_time']} and {decrease_signal['open_time']}")

    df = pd.DataFrame(results).iloc[:, :4]
    df.columns = ["signal_time", "on_signal_bought_price", "max_price", "profit"]

    profit_rows_for_more_than_15 = df[df['profit'] >= 15]
    logger.info(f"Length of profit_rows_for_more_than_10: {len(profit_rows_for_more_than_15)}")
    logger.info(f"Profit rows: \n{profit_rows_for_more_than_15}")
    profit_rows_for_more_than_10 = df[df['profit'] >= 10]
    logger.info(f"Length of profit_rows_for_more_than_10: {len(profit_rows_for_more_than_10)}")
    logger.info(f"Profit rows: \n{profit_rows_for_more_than_10}")
    profit_rows_for_more_than_5 = df[df['profit'] >= 5]
    logger.info(f"Length of profit_rows_for_more_than_5: {len(profit_rows_for_more_than_5)}")
    logger.info(f"Profit rows: \n{profit_rows_for_more_than_5}")
    profit_rows_for_more_than_3 = df[df['profit'] >= 3]
    logger.info(f"Length of profit_rows_for_more_than_3: {len(profit_rows_for_more_than_3)}")
    logger.info(f"Profit rows: \n{profit_rows_for_more_than_3}")
