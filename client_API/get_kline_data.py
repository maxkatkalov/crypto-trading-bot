import asyncio
from datetime import datetime, timedelta, timezone
import logging

import aiohttp
import tortoise

from aws_ssh_app.aws_ec2_accessor import get_all_ec2_dns_names
from aws_ssh_app.ssh_proxies import connect_all_servers_and_run_cmd
from db_app.models import KlineData
from db_app.config import db_config

BASE_API_URL = "https://api.binance.com/api/v3/"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

file_handler = logging.FileHandler('get_klines_data.log')
file_handler.setLevel(logging.INFO)

file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

logger.addHandler(file_handler)


class KlineDataClient:
    def __init__(
            self,
            symbol: str,
            interval: str,
            limit: int,
            start_period: datetime,
            stop_period: datetime = datetime.now()
    ):
        self.symbol = symbol
        self.interval = interval
        self.limit = limit
        self.kline_data = []
        self.start_period = start_period
        self.end_period = stop_period
        self.date_list = None
        self.request_to_be_done = None

    async def __call__(self):
        self.generate_minute_range()
        chunks = self.generate_chunks(self.date_list)
        for index in range(len(chunks)):
            logging.info(f"Sleeping before starting requests for chunk #{index+1} out of {len(chunks)}...")

            logging.info(f"Preparing proxies to start requests...")
            await connect_all_servers_and_run_cmd(
                get_all_ec2_dns_names()
            )

            await self.start_requests(chunks[index])

        logging.info(f"Preparing proxies to start requests...")

        logging.info(f"Sleeping before bulk creating klines...")
        await asyncio.sleep(10)
        await self.bulk_create_klines()

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
            self.date_list.append(current_date)
            current_date += timedelta(minutes=16)
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

        logging.info(f"Total chunks: {len(chunks)} with {len(chunks[0])} elements")
        return chunks

    async def make_request_to_kline(
            self,
            start_time: datetime,
            end_time: datetime,
            proxy_str: str,
            requests_count: int,
            session: aiohttp.ClientSession,
    ) -> dict:
        try:
            async with session.get(
                f"{BASE_API_URL}klines",
                proxy=f"http://{proxy_str}",
                ssl=False,
                params={
                    "symbol": self.symbol,
                    "interval": self.interval,
                    "limit": self.limit,
                    "startTime": int(start_time.replace(tzinfo=timezone.utc).timestamp() * 1000),
                    "endTime": int(end_time.replace(tzinfo=timezone.utc).timestamp() * 1000),
                }
            ) as resp:
                logging.info(
                    f"Request #{requests_count}: for periond: "
                    f"{start_time.date()} - {start_time.time()} - {end_time.time()}, "
                    f"response: {resp.status} "
                )
                data = await resp.json()
                if resp.status == 200:
                    for kline in data:
                        data_dict = {
                            "open_time": datetime.utcfromtimestamp(kline[0] / 1000),
                            "close_time": datetime.utcfromtimestamp(kline[6] / 1000),
                            "open_price": float(kline[1]),
                            "high_price": float(kline[2]),
                            "low_price": float(kline[3]),
                            "close_price": float(kline[4]),
                            "volume": float(kline[5]),
                            "trades": int(kline[8]),
                            "quote_asset_volume": float(kline[7]),
                            "taker_buy_base_asset_volume": float(kline[9]),
                            "taker_buy_quote_asset_volume": float(kline[10]),
                            "interval": self.interval,
                            "symbol": self.symbol,
                        }
                        self.kline_data.append(KlineData.create_new_instance(data_dict))
        except (aiohttp.ClientOSError, aiohttp.ServerDisconnectedError, asyncio.TimeoutError) as e:
            logging.error(e)
            await asyncio.sleep(20)
            logging.info(f"Trying again to make request #{requests_count} with proxy: {proxy_str}..., after 20 sec")
            await self.make_request_to_kline(
                start_time,
                end_time,
                proxy_str,
                requests_count,
                session=session,
            )
        except Exception as e:
            logging.error(f"{e} for response: {resp.status}, {resp.text}, response: {resp}, response: {data}")
            await asyncio.sleep(20)
            logging.info(f"Trying again to make request #{requests_count} with proxy: {proxy_str}..., after 20 sec")
            await self.make_request_to_kline(
                start_time,
                end_time,
                proxy_str,
                requests_count,
                session=session,
            )

    async def start_requests(self, chunk: list[datetime]):
        logging.info(f"Starting requests...")
        local_proxy_list = self.rotate_proxies()
        current_proxy = next(local_proxy_list)

        requests_count_per_proxy = 0
        count_to_proxy_restart = 0
        total_request_count = 0

        async with aiohttp.ClientSession() as session:
            async with asyncio.TaskGroup() as tg:
                for date in chunk:
                    tg.create_task(
                        self.make_request_to_kline(
                            start_time=date,
                            end_time=date + timedelta(minutes=15, seconds=59, microseconds=999999),
                            proxy_str=f"{current_proxy.get('dns_name')}:8888",
                            requests_count=total_request_count,
                            session=session,
                        )
                    )

                    requests_count_per_proxy += 1
                    total_request_count += 1
                    count_to_proxy_restart += 1

                    if requests_count_per_proxy == 3:
                        try:
                            current_proxy = next(local_proxy_list)
                            requests_count_per_proxy = 0
                            logging.info(f"Next proxy: {current_proxy.get('dns_name')}")
                        except StopIteration:
                            logging.info(f"Sleeping for 20 sec before restarting...")
                            await asyncio.sleep(20)
                            local_proxy_list = self.rotate_proxies()
                            logging.info(f"Rotating proxies...")

    async def bulk_create_klines(self):
        logging.info(f"Bulk creating {len(self.kline_data)} klines")
        await tortoise.Tortoise.init(db_config)
        await KlineData.bulk_create(self.kline_data, batch_size=1_000_000)


if __name__ == "__main__":
    kline_instance = KlineDataClient(
        symbol="ETHUSDT",
        interval="1s",
        limit=1000,
        start_period=datetime(2023, 6, 1, 0, tzinfo=timezone.utc),
        stop_period=datetime(2023, 12, 31, 23, minute=59, microsecond=999999, tzinfo=timezone.utc),
    )
    asyncio.run(kline_instance())
