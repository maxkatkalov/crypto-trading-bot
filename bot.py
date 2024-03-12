import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.dispatcher import router
from aiogram.filters.command import Command


logging.basicConfig(level=logging.INFO)
bot = Bot(token="7084418916:AAEpAmbE0rrDI1TenMiQtTs0Uwn8LroAUxI", parse_mode="HTML")
dp = Dispatcher()


@dp.message()
async def cmd_start(message: types.Message):
    await message.reply(f"Hello, {message.from_user.id}!")


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
