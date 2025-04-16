import json
from datetime import datetime, timedelta
from typing import Union, AsyncGenerator, Any
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from collections import defaultdict
from pyrogram import Client, utils, raw
from pyrogram.types import Message


client = Client(
    'analyzer-bot',
    api_id=API_ID,
    api_hash=API_HASH,
    phone_number=PHONE,
    password=CLOUD_PASSWORD
)

chats = {}

async def get_chunk(
    *,
    client: "Client",
    chat_id: Union[int, str],
    limit: int = 0,
    offset: int = 0,
    from_message_id: int = 0,
    from_date: datetime = utils.zero_datetime()
):
    messages = await client.invoke(
        raw.functions.messages.GetHistory(
            peer=await client.resolve_peer(chat_id),
            offset_id=from_message_id,
            offset_date=utils.datetime_to_timestamp(from_date),
            add_offset=offset,
            limit=limit,
            max_id=0,
            min_id=0,
            hash=0
        ),
        sleep_threshold=60
    )

    return await utils.parse_messages(client, messages, replies=0)

class GetChatHistory:
    async def get_chat_history_reverse(
        self: "Client",
        chat_id: Union[int, str],
        limit: int = 0,
        offset: int = 0,
        offset_id: int = 0,
        offset_date: datetime = utils.zero_datetime()
    ) -> AsyncGenerator[Message, Any]:
        current = 0
        total = limit or (1 << 31) - 1
        limit = min(100, total)

        while True:
            messages = await get_chunk(
                client=self,
                chat_id=chat_id,
                limit=limit,
                offset=offset,
                from_message_id=offset_id,
                from_date=offset_date
            )

            if not messages:
                return

            offset_id = messages[-1].id

            for message in messages:
                yield message

                current += 1

                if current >= total:
                    return


# TODO
# async def update_chat_data(end_datetime: datetime | None = datetime.now() - timedelta(days=180), filename: str = '', chat_ids: list[int | str] | None = None):
#     start_datetime = datetime.now()
#     if not chat_ids:
#         dialogs = client.get_dialogs()
#         chats = {}
#         async for dialog in dialogs:
#             if getattr(dialog, 'chat', None):
#                 dialog = dialog.chat
#             chats[dialog.id] = []
#             messages = []
#             async for message in GetChatHistory.get_chat_history_reverse(self=client, chat_id=dialog.id):
#                 msg_datetime = message.date
#                 if start_datetime and msg_datetime > start_datetime:
#                     continue
#                 if end_datetime and msg_datetime < end_datetime:
#                     break
#                 messages.append(message)
#                 chats[dialog.id] = messages
#                 print(f'Message {message.id} updated')
#             print(f'\nChat {dialog.id} updated\n\n')
#         with open(f"chats_data/data_{filename}.json", "w", encoding="utf-8") as file:
#             json.dump(chats, file, ensure_ascii=False, indent=4)
#     else:
#         _chats = []
#         for chat_id in chat_ids:
#             _chats.append(await client.get_chat(chat_id))
#         dialogs = _chats
#         chats = {}
#         for dialog in dialogs:
#             chats[dialog.id] = []
#             messages = []
#             async for message in GetChatHistory.get_chat_history_reverse(self=client, chat_id=dialog.id):
#                 msg_datetime = message.date
#                 if start_datetime and msg_datetime > start_datetime:
#                     continue
#                 if end_datetime and msg_datetime < end_datetime:
#                     break
#                 messages.append(message)
#                 chats[dialog.id] = messages
#                 print(f'Message {message.id} updated')
#             print(f'\nChat {dialog.id} updated\n\n')
#         with open(f"chats_data/data_{filename}.json", "w", encoding="utf-8") as file:
#             json.dump(chats, file, ensure_ascii=False, indent=4)
#
# async def load_chat_data(filename: str = ''):
#     global chats
#     with open("data.json", "r", encoding="utf-8") as file:
#         chats = json.load(file)

async def count_and_plot(chat_id: str | int = "me", user_id: str | int = "me", start_date: datetime = None, end_date: datetime = None, interval: int = 3600):
    counts = defaultdict(int)
    if start_date:
        chat_history = GetChatHistory.get_chat_history_reverse(self=client, chat_id=chat_id, offset_date=start_date)
    else:
        chat_history = GetChatHistory.get_chat_history_reverse(self=client, chat_id=chat_id)
    async for message in chat_history:
        if (message.from_user and (message.from_user.username == user_id or message.from_user.id == user_id)) or user_id == "any":
            print(f'Подгружено {message.id} {message.date}')
            msg_date = message.date
            if start_date and msg_date > start_date:
                continue
            if end_date and msg_date < end_date:
                break
            timestamp_sec = msg_date.timestamp()
            rounded_timestamp_sec = (timestamp_sec // interval) * interval
            rounded_time = datetime.fromtimestamp(rounded_timestamp_sec)
            counts[rounded_time] += 1
    if not counts:
        print("Нет сообщений")
        return
    sorted_times = sorted(counts.keys())
    message_counts = [counts[time] for time in sorted_times]
    plt.figure(figsize=(12, 6))
    plt.plot(sorted_times, message_counts, marker='o', linestyle='-')
    caption = f"user: {user_id}, chat_id: {chat_id}, start_date: {start_date}, end_date: {end_date}, interval: {interval} sec"
    plt.xlabel("date & time")
    plt.ylabel("count")
    plt.title(caption)
    plt.grid(True)
    ax = plt.gca()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m %H:%M'))
    plt.gcf().autofmt_xdate()
    file_path = "graph.png"
    plt.savefig(file_path, bbox_inches="tight")
    plt.close()
    await client.send_document("me", document=file_path, caption=caption)
    return counts

async def start():
    await client.start()
    start_dt = datetime.now()
    end_dt = start_dt - timedelta(days=31)
    stats = await count_and_plot(user_id="any", chat_id="", interval=int(60*60*3), end_date=end_dt, start_date=start_dt)
    if stats:
        print("Статистика сообщений:", stats)
    # await update_chat_data(None, '', chat_ids=[])
    print(chats)

client.run(start())
