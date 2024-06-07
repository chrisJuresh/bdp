import pandas as pd
import matplotlib.pyplot as plt
import json

with open('yellow_pickup.json', 'r') as file:
    yellow_pickup_data = json.load(file)
yellow_pickup_df = pd.DataFrame(yellow_pickup_data)

with open('green_pickup.json', 'r') as file:
    green_pickup_data = json.load(file)
green_pickup_df = pd.DataFrame(green_pickup_data)

with open('yellow_dropoff.json', 'r') as file:
    yellow_dropoff_data = json.load(file)
yellow_dropoff_df = pd.DataFrame(yellow_dropoff_data)

with open('green_dropoff.json', 'r') as file:
    green_dropoff_data = json.load(file)
green_dropoff_df = pd.DataFrame(green_dropoff_data)

plt.figure(figsize=(15, 10))

plt.subplot(2, 2, 1)
plt.bar(yellow_pickup_df['Pickup_Borough'], yellow_pickup_df['count'], color='yellow')
plt.title('Yellow Taxi Pickups by Borough')
plt.xlabel('Borough')
plt.ylabel('Pickup Count')

plt.subplot(2, 2, 2)
plt.bar(green_pickup_df['Pickup_Borough'], green_pickup_df['count'], color='green')
plt.title('Green Taxi Pickups by Borough')
plt.xlabel('Borough')
plt.ylabel('Pickup Count')

plt.subplot(2, 2, 3)
plt.bar(yellow_dropoff_df['Dropoff_Borough'], yellow_dropoff_df['count'], color='yellow')
plt.title('Yellow Taxi Dropoffs by Borough')
plt.xlabel('Borough')
plt.ylabel('Dropoff Count')

plt.subplot(2, 2, 4)
plt.bar(green_dropoff_df['Dropoff_Borough'], green_dropoff_df['count'], color='green')
plt.title('Green Taxi Dropoffs by Borough')
plt.xlabel('Borough')
plt.ylabel('Dropoff Count')

plt.tight_layout()
plt.show()
plt.savefig('chart.png')