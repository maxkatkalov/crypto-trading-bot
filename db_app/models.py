import asyncio

from tortoise import Tortoise, fields
from tortoise.models import Model

from db_app.config import db_config


class KlineData(Model):
    open_time = fields.DatetimeField(index=True)
    close_time = fields.DatetimeField(index=True)
    open_price = fields.FloatField()
    high_price = fields.FloatField()
    low_price = fields.FloatField()
    close_price = fields.FloatField()
    volume = fields.FloatField()
    quote_asset_volume = fields.FloatField()
    buy_base_asset_volume = fields.FloatField()
    buy_quote_asset_volume = fields.FloatField()
    number_of_trades = fields.IntField()
    symbmol = fields.CharField(max_length=20)
    interval = fields.CharField(max_length=20, default="1m")

    aggregated_trades: fields.ReverseRelation["AggregatedTradeData"]

    class Meta:
        table = "KlineData"
        indexes = [("open_time", "symbmol"), ("close_time", "symbmol")]

    def __str__(self) -> str:
        return (
            f"Open time: {self.open_time}, "
            f"Close time: {self.close_time}, "
            f"Open price: {self.open_price}, "
            f"High price: {self.high_price}, "
            f"Low price: {self.low_price}, "
            f"Close price: {self.close_price}, "
            f"Volume: {self.volume}, "
            f"Quote asset volume: {self.quote_asset_volume}, "
            f"Buy base asset volume: {self.buy_base_asset_volume}, "
            f"Buy quote asset volume: {self.buy_quote_asset_volume}, "
            f"Number of trades: {self.number_of_trades}"
        )

    @classmethod
    def create_new_instance(cls, kline_data: dict) -> "KlineData":
        return cls(
            open_time=kline_data.get("open_time"),
            close_time=kline_data.get("close_time"),
            open_price=kline_data.get("open_price"),
            high_price=kline_data.get("high_price"),
            low_price=kline_data.get("low_price"),
            close_price=kline_data.get("close_price"),
            volume=kline_data.get("volume"),
            quote_asset_volume=kline_data.get("quote_asset_volume"),
            buy_base_asset_volume=kline_data.get("taker_buy_base_asset_volume"),
            buy_quote_asset_volume=kline_data.get("taker_buy_quote_asset_volume"),
            number_of_trades=kline_data.get("trades"),
            interval=kline_data.get("interval"),
            symbmol=kline_data.get("symbol"),
        )


class AggregatedTradeData(Model):
    aggregated_trade_id = fields.IntField()
    price = fields.FloatField()
    quantity = fields.FloatField()
    first_trade_id = fields.IntField()
    last_trade_id = fields.IntField()
    trade_time = fields.DatetimeField()
    buyer_market_maker = fields.BooleanField()
    best_price_match = fields.BooleanField()
    kline: fields.ForeignKeyRelation[KlineData] = fields.ForeignKeyField(
        "models.KlineData",
        related_name="aggregated_trades",
    )

    class Meta:
        table = "AggregatedTradeData"

    def __str__(self) -> str:
        return (
            f"Aggregated trade id: {self.aggregated_trade_id}, "
            f"Price: {self.price}, "
            f"Quantity: {self.quantity}, "
            f"Trade time: {self.trade_time}, "
            f"Buyer market maker: {self.buyer_market_maker},"
        )

    @classmethod
    def create_new_trade_instance(cls, trade_data: dict) -> "AggregatedTradeData":
        return cls.create(
            aggregated_trade_id=trade_data.get("aggregated_trade_id"),
            price=trade_data.get("price"),
            quantity=trade_data.get("quantity"),
            first_trade_id=trade_data.get("first_trade_id"),
            last_trade_id=trade_data.get("last_trade_id"),
            trade_time=trade_data.get("trade_time"),
            buyer_market_maker=trade_data.get("buyer_market_maker"),
            best_price_match=trade_data.get("best_price_match"),
            kline=trade_data.get("kline"),
        )


async def init_and_generate_schemas():
    await Tortoise.init(db_config)
    await Tortoise.generate_schemas()


if __name__ == "__main__":
    asyncio.run(init_and_generate_schemas())
