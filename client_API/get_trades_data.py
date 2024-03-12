import asyncio
from datetime import datetime, timedelta, timezone
import logging

import aiohttp
import tortoise

from aws_ssh_app.aws_ec2_accessor import get_all_ec2_dns_names
from aws_ssh_app.ssh_proxies import connect_all_servers_and_run_cmd
from db_app.models import AggregatedTradeData, Kline1mData
from db_app.config import db_config

BASE_API_URL = "https://api.binance.com/api/v3/"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

file_handler = logging.FileHandler("get_trades_data.log")
file_handler.setLevel(logging.INFO)

file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

logger.addHandler(file_handler)


async def init_tortoise():
    await tortoise.Tortoise.init(db_config)


async def close_tortoise():
    await tortoise.Tortoise.close_connections()


class TradeData:
    def __init__(
            self,
            symbol: str,
            limit: int,
            start_period: datetime,
            stop_period: datetime = datetime.utcnow(),
    ) -> None:
        self.symbol = symbol
        self.limit = limit
        self.trades = []
        self.start_period = start_period
        self.end_period = stop_period.replace(microsecond=999000)
        self.date_list = None
        self.request_to_be_done = None

    async def __call__(self):
        self.generate_minute_range()
        chunks = self.generate_chunks(self.date_list)
        for index in range(len(chunks)):

            logging.info(f"Sleeping before starting requests for chunk #{index + 1} out of {len(chunks)}...")
            await asyncio.sleep(20)

            logging.info(f"Preparing proxies to start requests...")
            await connect_all_servers_and_run_cmd(
                get_all_ec2_dns_names()
            )

            await self.start_requests(chunks[index])

    @staticmethod
    def rotate_proxies(
            list_of_proxies: list[dict] = get_all_ec2_dns_names()
    ):
        proxies = [proxy for proxy in list_of_proxies]
        for proxy in proxies:
            yield proxy

    def generate_minute_range(self):
        self.date_list = []
        current_date = self.start_period
        while current_date <= self.end_period:
            splitted_minute = []
            for i in range(2):
                if i == 0:
                    splitted_minute.append(current_date)
                    current_date += timedelta(seconds=30)
                else:
                    splitted_minute.append(current_date - timedelta(microseconds=1))
                    current_date += timedelta(seconds=30)
            self.date_list.append(splitted_minute)
        self.request_to_be_done = len(self.date_list)
        logging.info(f"Total requests should be done: {self.request_to_be_done}")

    @staticmethod
    def generate_chunks(array_of_elements, chunk_size=1440):
        chunks = []
        while True:
            if array_of_elements and len(array_of_elements) > chunk_size:
                chunks.append(list(array_of_elements[:chunk_size]))
                array_of_elements = array_of_elements[chunk_size:]
            else:
                chunks.append(list(array_of_elements))
                break

        logging.info(f"Total chunks: {len(chunks)} with {len(chunks[0])} elements, and last element: {len(chunks[-1])}")
        return chunks

    async def find_kline_for_trade(self):
        await tortoise.Tortoise.init(db_config)
        klines_for_certain_period = await Kline1mData.filter(
            open_time__gte=self.start_period,
            close_time__lte=self.end_period,
        )
        klines_for_certain_period = {kline.open_time: kline for kline in klines_for_certain_period}

        for trade_dict in self.trades:
            date = (
                trade_dict["trade_time"].replace(second=0, microsecond=0)
            )
            logging.info(f"Date: {date}")
            trade_dict["kline"] = klines_for_certain_period.get(
                trade_dict["trade_time"]
                .replace(tzinfo=timezone.utc)
                .replace(second=0, microsecond=0)
            )
            if trade_dict.get("kline"):
                logging.info(f"Found kline: {trade_dict.get('trade_time')} - {trade_dict['kline']}")
            else:
                logging.info(f"{trade_dict.get('trade_time')} - no kline found")

        await tortoise.Tortoise.close_connections()

    async def make_request_to_agg_trades(
            self,
            minute: list,
            proxy_str: str,
            session: aiohttp.ClientSession,
            requests_count: int,

    ) -> dict:
        try:
            minute_data = []
            for time in minute:
                async with session.get(
                        f"{BASE_API_URL}aggTrades",
                        proxy=f"http://{proxy_str}",
                        ssl=False,
                        params={
                            "symbol": self.symbol,
                            "limit": self.limit,
                            "startTime": int(time.replace(tzinfo=timezone.utc).timestamp() * 1000),
                            "endTime": int(
                                (time + timedelta(seconds=30)).replace(
                                    tzinfo=timezone.utc
                                ).timestamp() * 1000
                            ),
                        }
                ) as resp:
                    data = await resp.json()
                    logging.info(
                        f"Request #{requests_count}: for periond: "
                        f"{time.replace(tzinfo=timezone.utc).date()} - "
                        f"{time.replace(tzinfo=timezone.utc).time()} - "
                        f"{(time + timedelta(seconds=30)).replace(tzinfo=timezone.utc).time()}, "
                        f"response: {resp.status} "
                    )
                    if data:
                        for trade in data:
                            trade_dict = {
                                "aggregated_trade_id": trade.get("a"),
                                "price": float(trade.get("p")),
                                "quantity": float(trade.get("q")),
                                "trade_time": datetime.utcfromtimestamp(trade.get("T") / 1000),
                                "first_trade_id": trade.get("f"),
                                "last_trade_id": trade.get("l"),
                                "buyer_market_maker": trade.get("m"),
                                "best_price_match": trade.get("M"),
                            }

                            minute_data.append(trade_dict)

            self.trades.extend(minute_data)
            logging.info(f"Current total of trades: {len(self.trades)}")
        except (aiohttp.ClientOSError, aiohttp.ServerDisconnectedError, asyncio.TimeoutError) as e:
            logging.error(e)
            await asyncio.sleep(20)
            logging.info(f"Trying again to make request #{requests_count} with proxy: {proxy_str}..., after 20 sec")
            await self.make_request_to_agg_trades(
                minute,
                proxy_str,
                session,
                requests_count,
            )
        except Exception as e:
            logging.error(f"{e} for response: {resp.status}, {resp.text}, response: {resp}, response: {data}")
            await asyncio.sleep(20)
            logging.info(f"Trying again to make request #{requests_count} with proxy: {proxy_str}..., after 20 sec")
            await self.make_request_to_agg_trades(
                minute,
                proxy_str,
                session,
                requests_count,
            )

    async def start_requests(self, chunk: list[list[datetime, datetime]]):
        logging.info(f"Starting requests...")
        local_proxy_list = self.rotate_proxies()
        current_proxy = next(local_proxy_list)

        requests_count_per_proxy = 0
        total_request_count = 0

        async with aiohttp.ClientSession() as session:
            async with asyncio.TaskGroup() as tg:
                for date in chunk:
                    tg.create_task(
                        self.make_request_to_agg_trades(
                            date,
                            proxy_str=f"{current_proxy.get('dns_name')}:8888",
                            session=session,
                            requests_count=total_request_count,
                        )
                    )

                    requests_count_per_proxy += 1
                    total_request_count += 1

                    if requests_count_per_proxy == 1:
                        try:
                            current_proxy = next(local_proxy_list)
                            requests_count_per_proxy = 0
                            logging.info(f"Next proxy: {current_proxy.get('dns_name')}")
                        except StopIteration:
                            logging.info(f"Sleeping for 20 sec before restarting...")
                            await asyncio.sleep(20)
                            local_proxy_list = self.rotate_proxies()
                            logging.info(f"Rotating proxies...")

        await self.find_kline_for_trade()
        list_of_instances = [
            AggregatedTradeData(
                aggregated_trade_id=data.get("aggregated_trade_id"),
                price=data.get("price"),
                quantity=data.get("quantity"),
                trade_time=data.get("trade_time"),
                first_trade_id=data.get("first_trade_id"),
                last_trade_id=data.get("last_trade_id"),
                buyer_market_maker=data.get("buyer_market_maker"),
                best_price_match=data.get("best_price_match"),
                kline=data.get("kline"),
            )
            for data in self.trades
        ]

        await self.bulk_create_trades(list_of_instances)
        self.trades = []
        logging.info(f"Resetted trades: {len(self.trades)}")

    @staticmethod
    async def bulk_create_trades(list_of_instances: list):
        await init_tortoise()
        logging.info(f"Bulk creating {len(list_of_instances)} klines")
        await AggregatedTradeData.bulk_create(list_of_instances, batch_size=1000)
        await close_tortoise()


if __name__ == "__main__":
    trade_instance = TradeData(
        symbol="ETHUSDT",
        limit=1000,
        start_period=datetime(2024, 2, 1, 0, 0, 0),
        stop_period=datetime(2024, 2, 15, 23, 59, 59),
    )
    asyncio.run(trade_instance())
