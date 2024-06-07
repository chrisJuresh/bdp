import pandas as pd
import matplotlib.pyplot as plt
import json

def plot_taxi_data(ax, pickup_json_data, dropoff_json_data, title, is_pickup, color):
    if is_pickup:
        df = pd.DataFrame(json.loads(pickup_json_data))
        borough_column = 'Pickup_Borough'
    else:
        df = pd.DataFrame(json.loads(dropoff_json_data))
        borough_column = 'Dropoff_Borough'
    
    ax.bar(df[borough_column], df['count'], color=color)
    ax.set_title(title)
    ax.set_xlabel('Borough')
    ax.set_ylabel('Number of Trips')
    ax.set_xticks(range(len(df[borough_column])))
    ax.set_xticklabels(df[borough_column], rotation=45)

with open('yellow_pickup_top5.json', 'r') as file:
    yellow_pickup_top5_data = file.read()

with open('yellow_dropoff_top5.json', 'r') as file:
    yellow_dropoff_top5_data = file.read()

with open('green_pickup_top5.json', 'r') as file:
    green_pickup_top5_data = file.read()

with open('green_dropoff_top5.json', 'r') as file:
    green_dropoff_top5_data = file.read()

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 12))

plot_taxi_data(ax1, yellow_pickup_top5_data, yellow_dropoff_top5_data, 'Top 5 Boroughs for Yellow Taxis (Pickups and Drop-offs)', True, 'yellow')

plot_taxi_data(ax2, green_pickup_top5_data, green_dropoff_top5_data, 'Top 5 Boroughs for Green Taxis (Pickups and Drop-offs)', True, 'green')

plt.tight_layout()
plt.savefig('chart.png')
