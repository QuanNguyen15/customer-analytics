import numpy as np
import pandas as pd
import os

# List input files (optional for your docker container, but kept here for visibility)
for dirname, _, filenames in os.walk('/opt/spark/data'):
    for filename in filenames:
        print(os.path.join(dirname, filename))

# Load the data for October and November
df_oct = pd.read_csv('/opt/spark/data/2019-Oct.csv')
df_nov = pd.read_csv('/opt/spark/data/2019-Nov.csv')

# Convert 'event_time' to datetime and remove 'UTC'
df_oct['event_time'] = pd.to_datetime(df_oct['event_time'].str.replace(' UTC', ''))

# Filter for 'purchase' event_type
purchase_df_oct = df_oct[df_oct['event_type'] == 'purchase']

# Extract hour from 'event_time'
purchase_df_oct['hour'] = purchase_df_oct['event_time'].dt.hour

# Count occurrences of each hour
hour_counts = purchase_df_oct['hour'].value_counts().sort_index()

# Get the hour that appears most often and its count
most_common_hour = hour_counts.idxmax()
count_most_common_hour = hour_counts.max()

print(f"Most common hour for purchases: {most_common_hour} with {count_most_common_hour} purchases.")

# Get the top 3 hours with the most purchases
top_3_hours = hour_counts.nlargest(3)
print(f"Top 3 hours with most purchases:\n{top_3_hours}")

# Extract day from 'event_time'
purchase_df_oct['day'] = purchase_df_oct['event_time'].dt.day

# Count occurrences of each day
day_counts = purchase_df_oct['day'].value_counts().sort_index()

# Get the day that appears most often and its count
most_common_day = day_counts.idxmax()
count_most_common_day = day_counts.max()

print(f"Most common day for purchases: {most_common_day} with {count_most_common_day} purchases.")

# Get the top 3 days with the most purchases
top_3_days = day_counts.nlargest(3)
print(f"Top 3 days with most purchases:\n{top_3_days}")
