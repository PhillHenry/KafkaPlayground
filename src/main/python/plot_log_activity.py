import sys
from kafka_log_parser import LogLine, DATETIME_FORMAT
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

if __name__ == "__main__":
    filename = sys.argv[1]
    machine_to_logs = {}
    with open(filename, 'r') as f:
        for line in f:
            try:
                log = LogLine(line)
                logs = machine_to_logs.get(log.machine, [])
                machine_to_logs[log.machine] = logs + [log]
            except Exception:
                print(f"Could not parse line:\n{line}")
    fig, ax = plt.subplots(1, 1)
    colours = ['red', 'blue', 'yellow']
    for i, machine in enumerate(machine_to_logs.keys()):
        logs = [l.timestamp for l in (machine_to_logs[machine])]
        ax.hist(logs, bins=100, color=colours[i])
    ax.xaxis.set_major_locator(mdates.SecondLocator())
    ax.xaxis.set_major_locator(plt.MaxNLocator(2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter(DATETIME_FORMAT))
    plt.xticks(rotation=70)
    plt.show()

