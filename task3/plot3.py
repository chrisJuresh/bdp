import pandas as pd
import matplotlib.pyplot as plt
import json

with open('yellow_filtered_trip_counts.json', 'r') as file:
    trip_data = json.load(file)

trip_df = pd.DataFrame(trip_data)

trip_df.sort_values(by='day_of_week', inplace=True)

days_of_week = {1: 'Monday', 2: 'Tuesday', 3: 'Wednesday', 4: 'Thursday', 5: 'Friday', 6: 'Saturday', 7: 'Sunday'}
trip_df['day_of_week'] = trip_df['day_of_week'].map(days_of_week)

plt.figure(figsize=(10, 6))
plt.bar(trip_df['day_of_week'], trip_df['count'], color='orange')
plt.title('Count of Yellow Taxi Trips for Each Day of the First Week of 2023')
plt.xlabel('Day of the Week')
plt.ylabel('Number of Trips')
plt.xticks(rotation=45)
plt.show()
plt.savefig('chart.png')