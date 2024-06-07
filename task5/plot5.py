import pandas as pd
import matplotlib.pyplot as plt
import json

def plot_anomalies(json_file, title):
    with open(json_file, 'r') as file:
        data = json.load(file)
    
    df = pd.DataFrame(data)
    
    df['date'] = pd.to_datetime(df['date'])
    
    plt.figure(figsize=(12, 6))
    
    for anomaly, color in zip(['High', 'Low', 'Normal'], ['red', 'blue', 'green']):
        subset = df[df['anomaly'] == anomaly]
        plt.scatter(subset['date'], subset['count'], color=color, label=anomaly)

    plt.title(title)
    plt.xlabel('Date')
    plt.ylabel('Number of Trips')
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
    plt.savefig('chart.png')

json_file_path = 'yellow_june_2023_anomalies.json' 

plot_anomalies(json_file_path, 'Anomalies in Yellow Taxi Trips for June 2023')
