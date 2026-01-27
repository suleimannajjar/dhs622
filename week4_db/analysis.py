from db import fetch_time_series_chart_data
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

if __name__ == "__main__":
    handles = ['anna_news']
    start_date = '2025-12-01'
    end_date = '2026-01-21'
    time_series_chart_unit = 'day'

    records = fetch_time_series_chart_data(handles, start_date, end_date, time_series_chart_unit)

    df = pd.DataFrame.from_records(records)

    if not df.empty:
        # Create the line plot
        plt.figure(figsize=(12, 6))
        sns.lineplot(data=df, x='message_dt', y='count')

        plt.xlabel('Date/Time')
        plt.ylabel('Count')
        plt.title(f"Messages (per {time_series_chart_unit}) from {start_date} to {end_date}")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
    print('done')