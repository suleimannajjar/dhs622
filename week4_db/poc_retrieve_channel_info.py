## Proof of concept with config data hidden and data dictionary flattened and write to database

import asyncio
import sys

# Create and set event loop before any Telethon imports
if sys.platform == 'win32':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

from telethon.tl.types.messages import ChatFull
from telethon.sync import TelegramClient
from telethon import functions
from config import app_name, api_id, api_hash, config
from db import insert_data_into_channel_metadata_table

def extract_data_dictionary_from_channel_object(
    channel_object: ChatFull, channel_name: str
) -> dict:
    return {
        "channel_name": channel_name,
        "channel_id": channel_object.to_dict()["full_chat"]["id"],
        "channel_title": channel_object.to_dict()["chats"][0]["title"],
        "num_subscribers": channel_object.to_dict()["full_chat"]["participants_count"],
        "channel_bio": channel_object.to_dict()["full_chat"]["about"],
        "channel_birthdate": channel_object.to_dict()["chats"][0]["date"],
        "api_response": channel_object.to_json(),
    }


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