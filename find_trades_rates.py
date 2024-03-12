import requests
from datetime import datetime, timedelta, timezone
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

file_handler = logging.FileHandler('find_trades.log')
file_handler.setLevel(logging.INFO)

file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

logger.addHandler(file_handler)


BASE_API_URL = "https://api.binance.com/api/v3/"


def make_request_to_klines(start_time: datetime, end_time: datetime):
    resp = requests.get(
        f"{BASE_API_URL}klines",
        params={
            "symbol": "ETHUSDT",
            "interval": "1m",
            "limit": 1000,
            "startTime": int(start_time),
            "endTime": int(end_time),
        },
    )

    kline = resp.json()[0]
    open_price = float(kline[1])
    high_price = float(kline[2])
    low_price = float(kline[3])
    close_price = float(kline[4])
    logger.info(
        (
            f"*Open price*: {open_price}, "
            f"*High price*: {high_price}, "
            f"*Low price*: {low_price}, "
            f"*Close price*: {close_price} "
        )
    )


def anylyze_price(trades: list[dict]):
    total_buy_trades = 0
    total_buy_in_eth = 0
    total_buy_in_usdt = 0

    total_sell_trades = 0
    total_sell_in_eth = 0
    total_sell_in_usdt = 0

    for trade in trades:
        if trade.get("m"):
            total_buy_trades += 1
            total_buy_in_usdt += float(trade.get("p"))
            total_buy_in_eth += float(trade.get("q"))
        else:
            total_sell_trades += 1
            total_sell_in_usdt += float(trade.get("p"))
            total_sell_in_eth += float(trade.get("q"))

    total_trade_operations = total_buy_trades + total_sell_trades
    defference_in_usdt = total_buy_in_usdt - total_sell_in_usdt
    defference_in_eth = total_buy_in_eth - total_sell_in_eth

    logger.info(
        (
            f"**Total trades**: {total_trade_operations}, "
            f"**Total buy trades**: {total_buy_trades}, "
            f"**Total sell trades**: {total_sell_trades}, "
            f"**Defference in usdt**: {defference_in_usdt}, "
            f"**Defference in eth**: {defference_in_eth} "
            f"**Total buy in usdt**: {total_buy_in_usdt}, "
            f"**Total buy in eth**: {total_buy_in_eth}, "
            f"**Total sell in usdt**: {total_sell_in_usdt}, "
            f"**Total sell in eth**: {total_sell_in_eth} "
        )
    )


def make_request_to_aggtrades(start_time: datetime, end_time: datetime):
    resp = requests.get(
        f"{BASE_API_URL}aggTrades",
        params={
            "symbol": "ETHUSDT",
            "startTime": int(start_time),
            "endTime": int(end_time),
            "limit": 1000,
        },
    )

    logger.info(f"Response status code: {resp.status_code}")

    anylyze_price(resp.json())


def start_requests():
    start_time = datetime(2024, 2, 10, 0, 0, )
    end_time = datetime(2024, 2, 10, 0, 1, 0)

    logger.info(
            f"{'=' * 7}START REQUESTS FOR {start_time.date()}, {start_time.time()} - {end_time.time()}{'=' * 7}"
    )

    while True:
        make_request_to_aggtrades(
            start_time.replace(tzinfo=timezone.utc).timestamp() * 1000,
            end_time.replace(tzinfo=timezone.utc).timestamp() * 1000,
        )

        make_request_to_klines(
            start_time.replace(tzinfo=timezone.utc).timestamp() * 1000,
            end_time.replace(tzinfo=timezone.utc).timestamp() * 1000,
        )

        logger.info(
            f"{'=' * 7}END REQUESTS FOR {start_time.date()}, {start_time.time()} - {end_time.time()}{'=' * 7}"
        )

        start_time = end_time
        end_time = end_time + timedelta(minutes=1)


if __name__ == "__main__":
    start_requests()
