## Proof of concept with config data hidden and data dictionary flattened

import asyncio
import sys

# Create and set event loop before any Telethon imports
if sys.platform in ('win32', 'darwin'):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

from week7.utilities.logic2 import retrieve_and_save_channel_metadata, get_seed_list_preview
from week7.config import app_name, api_id, api_hash, INPUT_DIR
import os
import pandas as pd

if __name__ == '__main__':
    seed_list_name = 'russian_disinfo'

    # load all channels you're interested in:
    my_input_csv = os.path.join(INPUT_DIR, 'russian_disinfo.csv')
    channel_df = pd.read_csv(my_input_csv)
    channel_names = list(set([x.lower() for x in list(channel_df['handle'])]))

    # filter out handles you already retrieved from Telegram API:
    current_channels_df = pd.DataFrame.from_records(get_seed_list_preview([seed_list_name]))
    current_channel_names = list(current_channels_df['channel_name'])
    channel_names = [channel_name for channel_name in channel_names if channel_name not in current_channel_names]

    # for channel_name in channel_names:
    retrieve_and_save_channel_metadata(
        channel_names, app_name, api_id, api_hash, seed_list_name
    )