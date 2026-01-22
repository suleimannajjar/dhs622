## Proof of concept with config data hidden and data dictionary flattened

import asyncio
import sys

# Create and set event loop before any Telethon imports
if sys.platform in ('win32', 'darwin'):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

from telethon.tl.types.messages import ChatFull
from telethon.sync import TelegramClient
from telethon.sync import functions
from config import app_name, api_id, api_hash, OUTPUT_DIR, INPUT_DIR
import os
import csv
import pandas as pd
import time

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
        # "api_response": channel_object.to_json(),
    }


if __name__ == '__main__':
    # This is where the program really begins

    # Input data
    seed_df = pd.read_csv(os.path.join(INPUT_DIR, "russian_disinfo_channel_names.csv"))
    my_output_file_full_path = os.path.join(OUTPUT_DIR, "russian_disinfo_channels_info.csv")
    channel_names = list(seed_df['handle']) # ["rybar", "mig41"]

    # Retrieve channel metadata from Telegram API
    with TelegramClient(app_name, api_id, api_hash) as client:
        for channel_name in channel_names:
            channel_object = client(
                functions.channels.GetFullChannelRequest(channel=channel_name)
            )

            if channel_object is not None:
                # extract a simple dictionary of interesting data:
                data = extract_data_dictionary_from_channel_object(channel_object, channel_name)

                # save to disk as a CSV:
                df = pd.DataFrame.from_records([data])

                if not os.path.exists(my_output_file_full_path):
                    df.to_csv(my_output_file_full_path,
                              index=False,
                              encoding='utf-8-sig',
                              quoting=csv.QUOTE_NONNUMERIC)
                else:
                    df.to_csv(my_output_file_full_path,
                              index=False,
                              encoding='utf-8-sig',
                              quoting=csv.QUOTE_NONNUMERIC,
                              mode='a',
                              header=False)
                print('==============================================================')
            print(f"sleeping 30 seconds after obtaining data for @{channel_name} from Telegram API...")
            time.sleep(30)