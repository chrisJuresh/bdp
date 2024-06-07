import matplotlib.pyplot as plt
import json

with open('yellow_march_2023_fare_per_mile.json', 'r') as file:
    fare_per_mile_data = json.load(file)

fares = [data['fare_per_mile'] for data in fare_per_mile_data]
avg_fare = fare_per_mile_data[0]['avg_fare_per_mile']
x_values = list(range(len(fares)))

plt.figure(figsize=(10, 6))
plt.scatter(x_values, fares, alpha=0.5)
plt.axhline(y=avg_fare, color='r', linestyle='-', label=f'Average Fare per Mile: ${avg_fare:.2f}')
plt.title("Fare per Mile for Yellow Taxi Trips in March 2023")
plt.xlabel("Trip")
plt.ylabel("Fare per Mile ($)")
plt.legend()
plt.show()
plt.savefig('chart.png')