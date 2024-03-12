import asyncio

from bot import bot


async def send_message_to_user(message_text: str, user_id: int = 382861786):
    await bot.send_message(chat_id=user_id, text=message_text)


async def main():
    await send_message_to_user(382861786, "Hello, World!")


if __name__ == "__main__":
    asyncio.run(main())
