import asyncio
import sys

# Create and set event loop before any Telethon imports
if sys.platform == 'win32':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

from telethon.sync import TelegramClient
from config import app_name, api_id, api_hash
import time
from db import insert_data_into_channel_messages_table
from logic import extract_data_from_message_object

if __name__ == "__main__":
    channel_names = ['rybar', 'mig41']

    for channel_name in channel_names:
        new_max_id = 0

        with TelegramClient(app_name, api_id, api_hash) as client:
            while True:
                message_iterator = client.iter_messages(channel_name,
                                                     min_id=0,
                                                     max_id=new_max_id,
                                                     limit=100)

                new_messages = list(message_iterator)

                if len(new_messages) == 0:
                    print(f"no new messages obtained for @{channel_name}")
                    break

                new_records = [
                    extract_data_from_message_object(message)
                    for message in new_messages
                    if message.to_dict()["_"] == "Message"
                ]

                # save these new records somewhere!
                insert_data_into_channel_messages_table(new_records)

                new_max_id = min([message.id for message in new_messages])

                if new_max_id == 0:
                    print(f"yay! we're done! we went thru all of @{channel_name}'s messages")
                    break

                print(f"the min message ID is now {new_max_id}")
                time.sleep(1)

    print(f"do cool stuff now...")