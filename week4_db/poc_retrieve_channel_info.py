## Proof of concept with config data hidden and data dictionary flattened and write to database

import asyncio
import sys

# Create and set event loop before any Telethon imports
if sys.platform == 'win32':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

from telethon.sync import TelegramClient
from telethon import functions
from config import app_name, api_id, api_hash, config
from db import insert_data_into_channel_metadata_table
from logic import extract_data_dictionary_from_channel_object

if __name__ == '__main__':
    # Input data
    channel_names = ["rybar", "mig41"]

    # Retrieve channel metadata from Telegram API
    with TelegramClient(app_name, api_id, api_hash) as client:
        for channel_name in channel_names:
            channel_object = client(
                functions.channels.GetFullChannelRequest(channel=channel_name)
            )

            if channel_object is not None:
                flattened_dictionary = extract_data_dictionary_from_channel_object(channel_object, channel_name)
                print(flattened_dictionary)
                insert_data_into_channel_metadata_table([flattened_dictionary])


                print('==============================================================')