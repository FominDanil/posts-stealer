import asyncio
import os
import sys
from datetime import datetime, timedelta
from pyrogram.errors import InviteRequestSent, UserAlreadyParticipant, FloodWait

import pyrogram
from progress.bar import IncrementalBar
from pyrogram.enums import ChatType

from config import TELEGRAM_LINKS, my_channel_link, YEAR, MONTH, DAY

from pyrogram import Client, types, filters

from dotenv import load_dotenv

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


async def send_message(message):
    if message.text:
        await app.send_message(my_channel, message.text, entities=message.entities)
    elif message.photo:
        await app.send_photo(
            my_channel,
            message.photo.file_id,
            caption=message.caption
        )
    elif message.video:
        await app.send_video(
            my_channel,
            message.video.file_id,
            caption=message.caption
        )
    elif message.audio:
        await app.send_audio(
            my_channel,
            message.audio.file_id,
            caption=message.caption
        )
    elif message.document:
        await app.send_document(
            my_channel,
            message.document.file_id,
            caption=message.caption
        )


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
    await send_message(message)


async def main():
    await app.start()
    messages_to_forward = []
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
        chat_ids.append(source_channel)

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

            messages_to_forward.append(message)
        await asyncio.sleep(1)
        bar.next()
    bar.finish()
    if messages_to_forward:
        bar = IncrementalBar('Отправка сообщений', max=len(messages_to_forward))
        bar.start()
        for message in sorted(messages_to_forward, key=lambda mes: mes.date):
            try:
                await send_message(message)
            except FloodWait as e:
                await asyncio.sleep(e.x)
                await send_message(message)
            bar.next()
        bar.finish()

        print('Все старые сообщения обработаны')
    else:
        print('Нет старых сообщений')

    await pyrogram.idle()
    await app.stop()
app.run(main())