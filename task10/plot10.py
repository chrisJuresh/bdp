import matplotlib.pyplot as plt
import json

# Replace this with the path to your JSON file
json_file_path = 'most_popular_months.json'

# Read JSON data
with open(json_file_path, 'r') as file:
    data = json.load(file)

# Prepare data for plotting
months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
taxi_types = [item['taxi_type'] for item in data]
trip_counts = [item['trips'] for item in data]
month_numbers = [item['month'] for item in data]
month_names = [months[number - 1] for number in month_numbers]  # Convert month number to name

# Create bar plot
plt.figure(figsize=(10, 6))
plt.bar(taxi_types, trip_counts, color=['yellow', 'green'])
plt.xlabel('Taxi Type')
plt.ylabel('Trip Count')
plt.title('Most Popular Month for Yellow and Green Taxis')
plt.xticks(taxi_types, month_names)

# Display the plot
plt.show()
plt.savefig('chart.png')