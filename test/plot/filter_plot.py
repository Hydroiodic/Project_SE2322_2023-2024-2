import pandas as pd
import matplotlib.pyplot as plt

# read csv from file
data = pd.read_csv('../data/filter_test_out.csv')

# get the table header
x_label = data.columns[0]
y1_label = data.columns[1]
y2_label = data.columns[2]

# plot and set title and labels
plt.plot(data[x_label], data[y1_label], label=y1_label)
plt.plot(data[x_label], data[y2_label], label=y2_label)
plt.title('Latency vs Size of BloomFilter')
plt.xlabel(x_label)
plt.ylabel('Latency')

# show the picture
plt.legend()
plt.show()
