import asyncio
from datetime import datetime, timezone, timedelta
import logging

import tortoise

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
    def __init__(
            self,
            kline_data,
            new_kline_data: list,
            macd_increase_target: float,
            macd_decrease_target: float,
            price_target: float,
            last_hour: datetime,
            budget: int = 800,
            search_for_trend: str = "decrease",
    ):
        self.max_macd = None
        self.bought_crypto_amount = None
        self.bought_crypto_price = None
        self.need_to_sell_for_price = None
        self.started = True
        self.kline_data = kline_data
        self.macd_increase_target = macd_increase_target
        self.macd_decrease_target = macd_decrease_target
        self.current_monitor_signal = self.monitor_increase_macd
        self.count_for_report = 0
        self.symbol = "ETHUSDT"
        self.interval = "1h"
        self.last_hour = last_hour
        self.last_report_minute = datetime.utcnow().replace(tzinfo=timezone.utc)
        self.search_for_trend = search_for_trend
        self.last_kline = {
            "open_time": None,
            "open_price": None,
            "close_price": None,
            "high_price": None,
            "low_price": None,
        }
        self.new_kline_data = new_kline_data
        self.budget = budget
        self.profit = price_target
        self.start_budget_for_current_day = None
        self.buy_trades = 0
        self.sell_trades = 0
        self.urgent_trades = 0
        self.porfit_for_current_day = 0
        self.bought_time = None
        self.sold_last_buy = True

    async def __call__(self, *args, **kwargs):
        if self.search_for_trend == "increase":
            self.current_monitor_signal = self.monitor_increase_stochastic

        for new_kline in self.new_kline_data:
            if self.start_budget_for_current_day is None:
                self.start_budget_for_current_day = self.budget

            self.kline_data.append(new_kline)
            res = search_macd(self.kline_data)
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
                self.last_kline["open_price"] = new_kline[1]
                self.last_kline["open_time"] = new_kline[0]
            if (
                    datetime.utcnow().replace(tzinfo=timezone.utc) - self.last_report_minute
            ) >= timedelta(minutes=3):
                await self.just_monitor(res)
                self.last_report_minute = datetime.utcnow().replace(tzinfo=timezone.utc)

            await self.generate_last_kline(new_kline)

            if self.last_hour.hour != new_kline[0].hour:
                lastKline = await KlineData.get(open_time=self.last_kline['open_time'], interval="1h", symbmol=self.symbol)
                self.kline_data.append(
                    [
                        lastKline.open_time,
                        lastKline.open_price,
                        lastKline.high_price,
                        lastKline.low_price,
                        lastKline.close_price,
                    ]
                )
                res = search_macd(self.kline_data)
                new_kline[0].replace(tzinfo=None)
                await send_message_to_user(
                    message_text=(
                        f"🆕 Adding last hour kline for period 🆕\n"
                        f"<em>{self.last_hour.date()}</em>, {self.last_hour.hour}:00 "
                        f"to {self.last_hour.hour}:59\n"
                        f"\n"
                        f"<b>Kline added:</b> \n"
                        f"Open time: {lastKline.open_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"Open price: {lastKline.open_price}\n"
                        f"High price: {lastKline.high_price}\n"
                        f"Low price: {lastKline.low_price}\n"
                        f"Close price: {lastKline.close_price}\n"
                        f"Stochastic %K: {float(format(res['%K'], '.2f'))}\n"
                        f"Stochastic %D: {float(format(res['%D'], '.2f'))}\n"
                        f"MACD: {float(format(res['macD'], '.2f'))}\n"
                        f"Signal: {float(format(res['signal'], '.2f'))}\n"
                    ),
                )
                self.last_hour = self.last_kline["open_time"] + timedelta(hours=1)

                self.last_kline = {
                    "open_price": None,
                    "close_price": None,
                    "high_price": None,
                    "low_price": None
                }

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

    @staticmethod
    async def just_monitor(last_macd_result):
        await send_message_to_user(
            (
                f"⏳ Current situation: ⏳\n"
                f"Current time: {last_macd_result[0].strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"<ins>Current price</ins>: <b>{float(format(last_macd_result['close'], '.2f'))}</b>\n"
                f"<ins>Current stochastic %K</ins>: <b>{float(format(last_macd_result['%K'], '.2f'))}</b>\n"
                f"<ins>Current stochastic %D</ins>: <b>{float(format(last_macd_result['%D'], '.2f'))}</b>\n"
                f"<ins>Current MACD</ins>: <b>{float(format(last_macd_result['macD'], '.2f'))}</b>\n"
                f"<ins>Current MACD signal</ins>: <b>{float(format(last_macd_result['signal'], '.2f'))}</b>\n"
            )
        )

    async def monitor_increase_stochastic(self, last_macd_result):
        stoch_k_value = float(format(last_macd_result["%K"], '.2f'))
        stock_d_value = float(format(last_macd_result["%D"], '.2f'))
        diff = stoch_k_value - stock_d_value
        if diff >= 3.5:
            await send_message_to_user(
                (
                    f"⚡️ <b>Stochastic increase</b> ⚡️\n"
                    f"Current time: {last_macd_result[0].strftime('%Y-%m-%d %H:%M')}\n"
                    f"Current stochastic %K: {stoch_k_value}\n"
                    f"Current stochastic %D: {stock_d_value}\n"
                    f"Current price: {last_macd_result['close']}\n"
                )
            )
            self.current_monitor_signal = self.monitor_increase_macd

    async def monitor_decrease_stochastic(self, last_macd_result):
        stoch_k_value = float(format(last_macd_result["%K"], '.2f'))
        stock_d_value = float(format(last_macd_result["%D"], '.2f'))

        if stock_d_value <= 25 and stoch_k_value <= 25:
            await send_message_to_user(
                (
                    f"❌ <b>Stochastic decrease</b> ❌\n"
                    f"Current time: {last_macd_result['open_time'].strftime('%Y-%m-%d %H:%M')}\n"
                    f"Current stochastic %K: {stoch_k_value}\n"
                    f"Current stochastic %D: {stock_d_value}\n"
                    f"Current price: {last_macd_result['close']}\n"
                )
            )
            self.current_monitor_signal = self.monitor_increase_macd

    async def monitor_increase_macd(self, last_macd_result):
        macd_value = float(format(last_macd_result["macD"], '.2f'))
        signal_value = float(format(last_macd_result["signal"], '.2f'))
        stoch_k_value = float(format(last_macd_result["%K"], '.2f'))
        stock_d_value = float(format(last_macd_result["%D"], '.2f'))
        diff = macd_value - signal_value
        stoch_diff = stoch_k_value - stock_d_value
        if diff >= self.macd_increase_target and stoch_diff >= 0.5:
            await send_message_to_user(
                (
                    f"⚠️ <b>SIGNAL</b> ⚠️\n"
                    f"Current time: {last_macd_result[0].strftime('%Y-%m-%d %H:%M')}\n"
                    f"Current MACD: {macd_value}\n"
                    f"Current MACD signal: {signal_value}\n"
                    f"Current stochastic %K: {stoch_k_value}\n"
                    f"Current stochastic %D: {stock_d_value}\n"
                    f"Current price: {last_macd_result['close']}\n"
                    f"DIFF: {stoch_diff}\n"
                    f"\n"
                    f"<b>BUY NOW!!!</b>"
                )
            )
            self.current_monitor_signal = self.buy_crypto
            self.max_macd = macd_value

    async def monitor_sell(self, last_macd_result):
        macd_value = float(format(last_macd_result["macD"], '.2f'))
        signal_value = float(format(last_macd_result["signal"], '.2f'))

        if macd_value > self.max_macd:
            self.max_macd = macd_value

        if (last_macd_result["close"] - self.bought_crypto_price) >= (self.bought_crypto_price * 1.015) - self.bought_crypto_price:
            await send_message_to_user(
                (
                    f"🤑 <b>Profit</b> 🤑\n"
                    f"Profit time: {last_macd_result[0].strftime('%Y-%m-%d %H:%M')}\n"
                    f"Current price: {last_macd_result['close']}\n"
                    f"Current profit: {last_macd_result['close'] - self.bought_crypto_price}\n"
                    f"Current budget: {self.budget}\n"
                    f"Current crypto amount: {self.bought_crypto_amount}\n"
                )
            )
            self.current_monitor_signal = self.sell_crypto
        if self.bought_time.hour != last_macd_result.iloc[0].hour:
            if macd_value < signal_value:
                await send_message_to_user(
                    (
                        f"🤑 <b>MACD decrease for sell</b> 🤑\n"
                        f"Decrease time: {last_macd_result[0].strftime('%Y-%m-%d %H:%M')}\n"
                        f"Current MACD: {macd_value}\n"
                        f"Current MACD signal: {signal_value}\n"
                        f"Current price: {last_macd_result['close']}\n"
                    )
                )
                self.current_monitor_signal = self.sell_crypto

    async def monitor_decrease_macd(self, last_macd_result):
        macd_value = float(format(last_macd_result["macD"], '.2f'))
        signal_value = float(format(last_macd_result["signal"], '.2f'))

        if self.bought_time.hour != last_macd_result.iloc[0].hour:
            if macd_value < signal_value:
                await send_message_to_user(
                    (
                        f"❌ <b>MACD decrease</b> ❌\n"
                        f"Decrease time: {last_macd_result[0].strftime('%Y-%m-%d %H:%M')}\n"
                        f"Current MACD: {macd_value}\n"
                        f"Current MACD signal: {signal_value}\n"
                        f"Current price: {last_macd_result['close']}\n"
                        f"{self.bought_time.hour != last_macd_result.iloc[0].hour}"
                    )
                )
                self.current_monitor_signal = self.monitor_increase_macd

    async def buy_crypto(self, last_macd_result):
        self.bought_crypto_price = last_macd_result["close"] + 0.10
        self.bought_crypto_amount = self.budget / self.bought_crypto_price
        self.need_to_sell_for_price = last_macd_result["close"] + self.profit + 0.07
        self.bought_time = last_macd_result[0]
        self.sold_last_buy = False
        self.buy_trades += 1

        self.current_monitor_signal = self.monitor_sell

        await send_message_to_user(
            (
                f"🔥 <b>Buy</b> 🔥\n"
                f"Buy time: {last_macd_result[0].strftime('%Y-%m-%d %H:%M')}\n"
                f"Bought for price: {self.bought_crypto_price}\n"
                f"Bought crypto amount: {self.bought_crypto_amount}\n"
                f"\n"
                f"Sell target: {self.bought_crypto_price * 1.015}\n"
            )
        )

    async def sell_crypto(self, last_macd_result):
        clean_profit = ((self.bought_crypto_amount * last_macd_result["close"]) - self.budget) - 0.38
        self.budget += clean_profit
        self.porfit_for_current_day += clean_profit
        await send_message_to_user(
            (
                f"🔥 <b>Sold</b> 🔥\n"
                f"Sell time: {last_macd_result[0].strftime('%Y-%m-%d %H:%M')}\n"
                f"Sold for price: {last_macd_result['close']}\n"
                f"\n"
                f"Current budget: {self.budget}"
            )
        )

        self.sell_trades += 1
        self.sold_last_buy = True
        self.current_monitor_signal = self.monitor_decrease_macd
        self.max_macd = None

    async def urgent_sell_crypto(self, last_macd_result):
        clean_profit = ((self.bought_crypto_amount * last_macd_result["close"]) - self.budget) - 0.38
        self.budget += clean_profit
        await send_message_to_user(
            (
                f"😖 <b>Sold</b> 😖\n"
                f"Sell time: {last_macd_result[0].strftime('%Y-%m-%d %H:%M')}\n"
                f"Sold for price: {last_macd_result['close']}\n"
                f"\n"
                f"Current budget: {self.budget}"
            )
        )
        self.current_monitor_signal = self.monitor_decrease_stochastic
        self.urgent_trades += 1

    async def generate_day_report(self):
        await send_message_to_user(
            (
                f"📊 <b>Day report</b> 📊\n"
                f"Day {self.last_hour.date()} start budget: {self.start_budget_for_current_day}\n"
                f"Total buy trades: {self.buy_trades}\n"
                f"Total sell trades: {self.sell_trades}\n"
                f"Total urgent trades: {self.urgent_trades}\n"
                f"Profited: {self.porfit_for_current_day}\n"
                f"\n"
                f"Total budget: {self.budget}"
            )
        )
        self.buy_trades = 0
        self.sell_trades = 0
        self.urgent_trades = 0
        self.porfit_for_current_day = 0
        self.start_budget_for_current_day = self.budget


async def main(start_stream: datetime):
    await tortoise.Tortoise.init(db_config)
    open_time = datetime(2023, 6, 1, tzinfo=timezone.utc)
    to_analyze_klines = await (
        KlineData
        .filter(
            open_time__gte=open_time,
            close_time__lte=start_stream - timedelta(microseconds=1),
            symbmol="ETHUSDT",
            interval="1h",
        )
        .order_by("open_time")
    )
    logger.info(f"{len(to_analyze_klines)} klines to analyze")

    kline_data_for_analyze = []
    for kline in to_analyze_klines:
        kline_data_for_analyze.append(
            [
                kline.open_time,
                kline.open_price,
                kline.high_price,
                kline.low_price,
                kline.close_price,
            ]
        )

    stream_klines = await (
        KlineData
        .filter(
            open_time__gte=start_stream,
            symbmol="ETHUSDT",
        )
        .order_by("open_time")
    )
    logger.info(f"{len(stream_klines)} klines in stream")

    new_stream_klines = []
    for kline in stream_klines:
        new_stream_klines.append(
            [
                kline.open_time,
                kline.open_price,
                kline.high_price,
                kline.low_price,
                kline.close_price,
            ]
        )

    socket_conn = SocketConn(
        kline_data=kline_data_for_analyze,
        new_kline_data=new_stream_klines,
        macd_increase_target=0.75,
        macd_decrease_target=0.20,
        price_target=0.20,
        budget=700,
        last_hour=start_stream,
    )
    await socket_conn()
    print(f"Last budget is: {socket_conn.budget}")
    await tortoise.Tortoise.close_connections()


if __name__ == "__main__":
    asyncio.run(main(
        start_stream=datetime(
            2024, 1, 1, hour=0, tzinfo=timezone.utc,
        )
    ))
