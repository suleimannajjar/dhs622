from config import INPUT_DIR, OUTPUT_DIR
from db_solved import insert_data_into_channel_metadata_table, insert_data_into_seed_table, insert_data_into_channel_messages_table
import pandas as pd
import os
from tqdm import tqdm

if __name__ == '__main__':
    seed_list_name = 'russian_disinfo'

    # load channel info data:
    channel_info_df = pd.read_csv(os.path.join(OUTPUT_DIR, 'russian_disinfo_channels_info.csv'))
    channel_info_df['seed_list'] = seed_list_name
    print(f"loaded a channel metadata frame of {channel_info_df.shape[0]} channels")

    # load channel messages data:
    channel_message_df = pd.read_csv(os.path.join(OUTPUT_DIR, 'russian_disinfo_channels_messages.csv'))
    print(f"loaded a channel messages frame of {channel_message_df.shape[0]} messages")

    # write the channel info data to database:
    insert_data_into_seed_table(channel_info_df[['channel_id', 'channel_name', 'seed_list']].to_dict('records'))
    insert_data_into_channel_metadata_table(channel_info_df.to_dict('records'))

    i = 0
    STEP_SIZE = 10
    while i < channel_message_df.shape[0]:
        print(f'i={i}')
        records = channel_message_df.loc[i:i+STEP_SIZE, :].to_dict('records')
        for record in records:
            if 'api_response' not in record.keys():
                record['api_response'] = {}
            if record['api_response'] is None:
                record['api_response'] = {}
            for corrupted_field in ('forwardee_message_id', 'forwardee_channel_id',):
                try:
                    record[corrupted_field] = int(record[corrupted_field])
                except:
                    record[corrupted_field] = None
        insert_data_into_channel_messages_table(records)
        i += STEP_SIZE
    print('done')

