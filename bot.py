import asyncio
import os
import sys
from datetime import datetime, timedelta
from typing import List, Optional
from pyrogram.errors import InviteRequestSent, UserAlreadyParticipant, FloodWait

import pyrogram
from progress.bar import IncrementalBar
from pyrogram.enums import ChatType

from config import TELEGRAM_LINKS, my_channel_link, YEAR, MONTH, DAY

from pyrogram import Client, types, filters

from dotenv import load_dotenv

from pydantic import BaseModel

load_dotenv()

API_ID = os.environ.get('API_ID')
API_HASH = os.environ.get('API_HASH')
SESSION_NAME = os.environ.get('SESSION_NAME')

app = Client(SESSION_NAME, api_id=API_ID, api_hash=API_HASH)
target_date = datetime(YEAR, MONTH, DAY)
chat_ids = []

seen_messages = set()
seen_urls = set()
seen_captions = set()
seen_photos = set()


class Albom(BaseModel):
    messages: List[int]
    date: Optional[datetime] = None
    chat_id: Optional[str] = None


@app.on_message(filters.chat(chat_ids))
async def handle_new_message(client, message):
    if message.text:
        if message.text in seen_messages:
            return
        seen_messages.add(message.text)
    if message.web_page:
        if message.web_page.url in seen_urls:
            return
        seen_urls.add(message.web_page.url)
    if message.caption:
        if message.caption in seen_captions:
            return
        seen_captions.add(message.caption)
    if message.photo:
        if message.photo.file_unique_id in seen_photos:
            return
        seen_photos.add(message.photo.file_unique_id)
    await message.forward(my_channel)


async def main():
    await app.start()
    date_month_ago = target_date - timedelta(days=30)

    global my_channel
    my_channel = await app.get_chat(my_channel_link if my_channel_link.split('/')[-1].startswith('+') else my_channel_link.split('/')[-1])
    if my_channel.type == 'channel' and type(my_channel) == types.ChatPreview:
        try:
            my_channel = await app.join_chat(my_channel.id)
        except InviteRequestSent:
            print(f'Канал {my_channel_link} требует потверждения')
            sys.exit(1)
    elif my_channel.type != ChatType.CHANNEL:
        print('Ссылка на ваш чат не является ссылкой на группу')
        sys.exit(1)
    my_channel = my_channel.id
    try:
        await app.join_chat(my_channel)
    except UserAlreadyParticipant:
        pass

    async for message in app.get_chat_history(my_channel):
        if message.date < date_month_ago:
            break
        if message.text:
            seen_messages.add(message.text)
        if message.web_page:
            seen_urls.add(message.web_page.url)
        if message.caption:
            seen_captions.add(message.caption)

    bar = IncrementalBar('Обработка прошлых сообщений', max=len(TELEGRAM_LINKS), suffix='%(percent)d%% [%(eta)d сек]')
    bar.start()
    albums = []
    for chat in TELEGRAM_LINKS:
        source_channel = await app.get_chat(chat if chat.split('/')[-1].startswith('+') else chat.split('/')[-1])

        if source_channel.type == 'channel' and type(source_channel) == types.ChatPreview:
            try:
                source_channel = await app.join_chat(source_channel.id)
            except InviteRequestSent:
                print(f'Канал {chat} требует потверждения')
                continue
        elif source_channel.type != ChatType.CHANNEL:
            print(f'Ссылка на чат {chat} не является ссылкой на группу')
            continue

        source_channel = source_channel.id
        try:
            await app.join_chat(source_channel)
        except UserAlreadyParticipant:
            pass
        except FloodWait as e:
            print(f"Sleeping for {e.value} seconds")
            await asyncio.sleep(e.value)
            await app.join_chat(source_channel) 
        chat_ids.append(source_channel)
        
        
        current_album = Albom(messages=[])
        last_timestamp = None

        async for message in app.get_chat_history(source_channel):
            if message.date < target_date:
                break
            if message.text:
                if message.text in seen_messages:
                    continue
                else:
                    seen_messages.add(message.text)
            if message.web_page:
                if message.web_page.url in seen_urls:
                    continue
                else:
                    seen_urls.add(message.web_page.url)
            if message.caption:
                if message.caption in seen_captions:
                    continue
                else:
                    seen_captions.add(message.caption)
            if message.photo:
                if message.photo.file_unique_id in seen_photos:
                    continue
                else:
                    seen_photos.add(message.photo.file_unique_id)

            if not last_timestamp or abs((message.date - last_timestamp).total_seconds()) <= 1: # проверяем что сообщения отправлены с интервалом в 1 секунду
                current_album.messages.append(message.id)
            else:
                if current_album:
                    current_album.date = last_timestamp
                    current_album.chat_id = source_channel
                    albums.append(current_album)
                    current_album = Albom(messages=[message.id])
            last_timestamp = message.date
        if current_album.messages:  # добавляем последний альбом
            current_album.date = last_timestamp
            current_album.chat_id = source_channel
            albums.append(current_album)
        await asyncio.sleep(1)
        bar.next()
    bar.finish()
    if albums:
        bar = IncrementalBar('Отправка сообщений', max=len(albums))
        bar.start()
        for message in sorted(albums, key=lambda mes: mes.date):
            try:
                await app.forward_messages(my_channel, message.chat_id, message.messages)
            except FloodWait as e:
                await asyncio.sleep(e.value)
                await app.forward_messages(my_channel, message.chat_id, message.messages)
            bar.next()
        bar.finish()

        print('Все старые сообщения обработаны')
    else:
        print('Нет старых сообщений')

    await pyrogram.idle()
    await app.stop()
    
if __name__ == '__main__':
    app.run(main())