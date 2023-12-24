from telethon.sync import TelegramClient
import configparser
import json
# для корректного переноса времени сообщений в json
from datetime import date, datetime
# класс для работы с сообщениями
from telethon.tl.functions.messages import GetHistoryRequest

config = configparser.ConfigParser()
config.read("tg_config.ini")

API_ID = config['Telegram']['api_id']
API_HASH = config['Telegram']['api_hash']
USERNAME = config['Telegram']['username']

telegram_client = TelegramClient(USERNAME, API_ID, API_HASH)
telegram_client.start()


async def DumpAllMessagesFromTelegramChannel(current_telegram_channel):
    """
    Writes a json-file with information about all channel/chat messages
    :arg:
        current_telegram_channel (Telegram Channel):
        Current chat/Telegram channel from which you need to parse messages.
    :return:
        messages (JSON-File): Messages received by parsing.
    """

    message_offset = 0  # номер записи, с которой начинается считывание
    limit_of_messages = 100  # максимальное число записей, передаваемых за один раз

    messages_list = []  # список всех сообщений
    total_messages = 0
    total_count_limit = 0  # поменять значение, если нужны не все сообщения

    class DateTimeEncoder(json.JSONEncoder):
        """Class for serializing dates in JSON"""

        def Default(self, obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            if isinstance(obj, bytes):
                return list(obj)
            return json.JSONEncoder.default(self, obj)

    while True:
        history = await telegram_client(GetHistoryRequest(
            peer=current_telegram_channel,
            offset_id=message_offset,
            offset_date=None, add_offset=0,
            limit=limit_of_messages, max_id=0, min_id=0,
            hash=0))
        if not history.messages:
            break
        messages = history.messages
        for message in messages:
            messages_list.append(message.to_dict())
        message_offset = messages[len(messages) - 1].id
        total_messages = len(messages_list)
        if total_count_limit != 0 and total_messages >= total_count_limit:
            break

    with open('channel_messages.json', 'w', encoding='utf8') as outfile:
        json.dump(messages_list, outfile, ensure_ascii=False, cls=DateTimeEncoder)


async def Main():
    telegram_channel_url = input("Enter the link to the Telegram channel or chat: ")
    current_telegram_channel = await telegram_client.get_entity(telegram_channel_url)

    await DumpAllMessagesFromTelegramChannel(current_telegram_channel)

with telegram_client:
    telegram_client.loop.run_until_complete(Main())
