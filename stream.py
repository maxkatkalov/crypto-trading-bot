import asyncio
from datetime import datetime, timezone, timedelta
import logging

import aiohttp
import tortoise
import websocket
import threading
import ssl
import json

from db_app.config import db_config
from db_app.models import KlineData
from indicators.MACD import search_macd
from signal_notificator_bot import send_message_to_user


BASE_API_URL = "https://api.binance.com/api/v3/"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

file_handler = logging.FileHandler('stream.log')
file_handler.setLevel(logging.INFO)

file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

logger.addHandler(file_handler)


class SocketConn:
    def __init__(self, url, kline_data, search_for_trend: str = "decrease"):
        self.url = url
        self.started = True
        self.kline_data = kline_data
        self.current_monitor_signal = self.monitor_decrease_stochastic
        self.count_for_report = 0
        self.symbol = "ETHUSDT"
        self.interval = "1h"
        self.last_hour = datetime.utcnow().replace(tzinfo=timezone.utc)
        self.last_report_minute = datetime.utcnow().replace(tzinfo=timezone.utc)
        self.search_for_trend = search_for_trend
        self.last_kline = {
            "open_time": datetime(2024, 2, 27, hour=9, tzinfo=timezone.utc),
            "open_price": 3219.98,
            "close_price": 3254.82,
            "high_price": 3254.82,
            "low_price": 3219.98,
        }

    async def __call__(self, *args, **kwargs):
        if self.search_for_trend == "increase":
            self.current_monitor_signal = self.monitor_increase_stochastic
        proxy_url = "http://18.192.126.104:8888"
        async with aiohttp.ClientSession(timeout=5) as session:
            async with session.ws_connect(self.url, ssl=False) as ws: #TODO: return proxy
                while True:
                    msg = await ws.receive()
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        open_time = datetime.utcfromtimestamp(data["k"].get("t") / 1000)
                        open_price = float(data["k"].get("o"))
                        high_price = float(data["k"].get("h"))
                        low_price = float(data["k"].get("l"))
                        close_price = float(data["k"].get("c"))
                        current_kline = [
                            open_time,
                            open_price,
                            high_price,
                            low_price,
                            close_price,
                        ]
                        self.kline_data.append(current_kline)
                        res = search_macd(self.kline_data, short_period=1, long_period=6)
                        logger.info(
                            (
                                f"Current MACD: {float(format(res['macD'], '.2f'))}, "
                                f"SIGNAL: {float(format(res['signal'], '.2f'))}, "
                                f"%K: {float(format(res['%K'], '.2f'))}, "
                                f"%D: {float(format(res['%D'], '.2f'))}, "
                            )
                        )
                        await self.current_monitor_signal(res)
                        self.kline_data.pop(-1)

                        if self.started:
                            await self.on_open(res)
                            self.started = False

                        if self.last_kline["open_price"] is None:
                            self.last_kline["open_price"] = current_kline[1]
                            self.last_kline["open_time"] = current_kline[0]

                        if (
                                datetime.utcnow().replace(tzinfo=timezone.utc) - self.last_report_minute
                        ) >= timedelta(minutes=3):
                            await self.just_monitor(res)
                            self.last_report_minute = datetime.utcnow().replace(tzinfo=timezone.utc)

                        await self.generate_last_kline(current_kline)

                        if self.last_hour.hour != datetime.utcnow().hour:
                            self.kline_data.append(
                                [
                                    self.last_kline["open_time"],
                                    self.last_kline["open_price"],
                                    self.last_kline["high_price"],
                                    self.last_kline["low_price"],
                                    self.last_kline["close_price"],
                                ]
                            )
                            await send_message_to_user(
                                message_text=(
                                    f"🆕 Adding last hour kline for period 🆕\n"
                                    f"<em>{self.last_hour.date()}</em>, {self.last_hour.hour}:00 "
                                    f"to {self.last_hour.hour}:59\n"
                                    f"\n"
                                    f"<b>Kline added:</b> \n"
                                    f"{self.last_kline}"
                                ),
                            )
                            await send_message_to_user(
                                message_text=(
                                    f"🆕 New hour kline details 🆕\n"
                                    f"<b>New period:</b> {self.last_hour.date()}, "
                                    f"{self.last_hour.hour}:00–{self.last_hour.hour}:59\n"
                                    f"\n"
                                    f"New hour open price: {current_kline[1]}"
                                )
                            )
                            self.last_kline = {
                                "open_price": None,
                                "close_price": None,
                                "high_price": None,
                                "low_price": None
                            }

                            self.last_hour = datetime.utcnow()

    async def on_open(self, kline):
        await send_message_to_user(
            (
                f"👀 <b>Connection established, starting monitor</b> 👀\n"
                f"<b>Looking for</b> {self.search_for_trend} trend\n"
                f"<b>Current symbol:</b> {self.symbol}\n"
                f"<b>Current {self.symbol} price:</b> {kline['close']}\n"
                f"<b>Current %K:</b> {kline['%K']}\n"
                f"<b>Current %D:</b> {kline['%D']}\n"
                f"<b>Current MACD:</b> {kline['macD']}\n"
                f"<b>Current MACD SIGNAL:</b> {kline['signal']}\n"
            )
        )

    async def generate_last_kline(self, kline):
        self.last_kline["close_price"] = kline[4]
        if self.last_kline["high_price"] is None or self.last_kline["high_price"] < kline[2]:
            self.last_kline["high_price"] = kline[2]
        if self.last_kline["low_price"] is None or self.last_kline["low_price"] > kline[3]:
            self.last_kline["low_price"] = kline[3]

    async def just_monitor(self, last_macd_result):
        await send_message_to_user(
            (
                f"⏳ Current situation: ⏳\n"
                f"<ins>Current price</ins>: <b>{float(format(last_macd_result['close'], '.2f'))}</b>\n"
                f"<ins>Current stochastic %K</ins>: <b>{float(format(last_macd_result['%K'], '.2f'))}</b>\n"
                f"<ins>Current stochastic %D</ins>: <b>{float(format(last_macd_result['%D'], '.2f'))}</b>\n"
                f"<ins>Current MACD</ins>: <b>{float(format(last_macd_result['macD'], '.2f'))}</b>\n"
                f"<ins>Current MACD signal</ins>: <b>{float(format(last_macd_result['signal'], '.2f'))}</b>\n"
                f"{self.last_kline}"
            )
        )

    async def monitor_increase_stochastic(self, last_macd_result):
        stoch_k_value = float(format(last_macd_result["%K"], '.2f'))
        stock_d_value = float(format(last_macd_result["%D"], '.2f'))
        diff = stoch_k_value - stock_d_value
        if diff >= 5:
            await send_message_to_user(
                (
                    f"⚡️ <b>Stochastic increase</b> ⚡️\n"
                    f"Current stochastic %K: {stoch_k_value}\n"
                    f"Current stochastic %D: {stock_d_value}\n"
                    f"Current price: {last_macd_result['close']}\n"
                )
            )
            self.current_monitor_signal = self.monitor_increase_macd

    async def monitor_decrease_stochastic(self, last_macd_result):
        stoch_k_value = float(format(last_macd_result["%K"], '.2f'))
        stock_d_value = float(format(last_macd_result["%D"], '.2f'))
        diff = stoch_k_value - stock_d_value
        if diff <= 1:
            await send_message_to_user(
                (
                    f"❌ <b>Stochastic decrease</b> ❌\n"
                    f"Current stochastic %K: {stoch_k_value}\n"
                    f"Current stochastic %D: {stock_d_value}\n"
                    f"Current price: {last_macd_result['close']}\n"
                )
            )
            self.current_monitor_signal = self.monitor_increase_stochastic

    async def monitor_increase_macd(self, last_macd_result):
        macd_value = float(format(last_macd_result["macD"], '.2f'))
        signal_value = float(format(last_macd_result["signal"], '.2f'))
        stoch_k_value = float(format(last_macd_result["%K"], '.2f'))
        stock_d_value = float(format(last_macd_result["%D"], '.2f'))
        diff = macd_value - signal_value
        stoch_diff = stoch_k_value - stock_d_value
        if diff >= 2 and stoch_diff >= 5:
            await send_message_to_user(
                (
                    f"⚠️ <b>SIGNAL</b> ⚠️\n"
                    f"Current MACD: {macd_value}\n"
                    f"Current MACD signal: {signal_value}\n"
                    f"Current stochastic %K: {stoch_k_value}\n"
                    f"Current stochastic %D: {stock_d_value}\n"
                    f"Current price: {last_macd_result['close']}\n"
                    f"\n"
                    f"<b>BUY NOW!!!</b>"
                )
            )
            self.current_monitor_signal = self.monitor_decrease_stochastic
        elif diff >= 1.5:
            await send_message_to_user(
                (
                    f"⚡️ <b>MACD increase</b> ⚡️\n"
                    f"Current MACD: {macd_value}\n"
                    f"Current MACD signal: {signal_value}\n"
                    f"But signal not confirmed by stochastic\n"
                )
            )

    async def monitor_decrease_macd(self, last_macd_result):
        macd_value = float(format(last_macd_result["macD"], '.2f'))
        signal_value = float(format(last_macd_result["signal"], '.2f'))
        diff = macd_value - signal_value
        stoch_k_value = float(format(last_macd_result["%K"], '.2f'))
        stock_d_value = float(format(last_macd_result["%D"], '.2f'))
        stoch_diff = stoch_k_value - stock_d_value
        if diff <= 1 and stoch_diff <= 1:
            await send_message_to_user(
                (
                    f"❌ <b>SELL NOW!!!</b> ❌\n"
                    f"Current MACD: {macd_value}\n"
                    f"Current MACD signal: {signal_value}\n"
                    f"Current stochastic %K: {stoch_k_value}\n"
                    f"Current stochastic %D: {stock_d_value}\n"
                    f"Current price: {last_macd_result['close']}\n"
                )
            )
            self.current_monitor_signal = self.monitor_increase_stochastic

        if diff <= 1:
            await send_message_to_user(
                (
                    f"❌ <b>MACD decrease</b> ❌\n"
                    f"Current MACD: {macd_value}\n"
                    f"Current MACD signal: {signal_value}\n"
                    f"Current price: {last_macd_result['close']}\n"
                )
            )


async def main():
    await tortoise.Tortoise.init(db_config)
    open_time = datetime(2023, 12, 1, tzinfo=timezone.utc)
    all_klines = await (
            KlineData
            .filter(
                open_time__gte=open_time,
                symbmol="ETHUSDT",
            )
            .order_by("open_time")
        )
    kline_data = []
    for kline in all_klines:
        kline_data.append(
            [
                kline.open_time,
                kline.open_price,
                kline.high_price,
                kline.low_price,
                kline.close_price,
            ]
        )
    socket_conn = SocketConn(
        "wss://127.0.0.1:5555",
        kline_data,
        search_for_trend="increase",)
    await socket_conn()

if __name__ == "__main__":
    asyncio.run(main())
