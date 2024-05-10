import matplotlib.pyplot as plt

file_name = "../data/compaction_out"
numbers = []

with open(file_name) as f:
    for i, line in enumerate(f):
        # ignore the first line
        if i == 0:
            continue
        number = float(line.strip())
        numbers.append((i, number))

# Unzip the list of tuples into two lists
x, y = zip(*numbers)

# Plot the data
plt.plot(x, y)
plt.xlabel("Operation No")
plt.ylabel("Throughput of Put Per Second")
plt.title("Throughput Trend of Put")
plt.show()
