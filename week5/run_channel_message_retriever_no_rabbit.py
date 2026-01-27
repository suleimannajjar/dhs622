## Proof of concept with config data hidden and data dictionary flattened

import asyncio
import sys

# Create and set event loop before any Telethon imports
if sys.platform in ('win32', 'darwin'):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

from week5.utilities.logic import retrieve_and_save_channel_messages
from week5.config import app_name, api_id, api_hash, INPUT_DIR
import os
import pandas as pd

if __name__ == '__main__':
    seed_list_name = 'russian_disinfo'
    my_input_csv = os.path.join(INPUT_DIR, 'russian_disinfo.csv')
    channel_df = pd.read_csv(my_input_csv)
    channel_names = list(set([x.lower() for x in list(channel_df['handle'])]))

    bad_list = [
        'rian_ru',
        'bbbreaking',
        'SIL0VIKI',
        'boris_rozhin'
    ]
    for channel_name in channel_names:
        if channel_name not in bad_list:
            retrieve_and_save_channel_messages(channel_name, app_name, api_id, api_hash)