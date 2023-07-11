import sys
from kafka_log_parser import LogLine, DATETIME_FORMAT, parse_logs
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def do_plot(filename: str):
    machine_to_logs = parse_logs(filename)
    fig, ax = plt.subplots(1, 1)
    colours = ['red', 'blue', 'yellow']
    for i, machine in enumerate(machine_to_logs.keys()):
        logs = [l.timestamp for l in (machine_to_logs[machine])]
        ax.hist(logs, bins=100, color=colours[i])
    ax.xaxis.set_major_locator(mdates.SecondLocator())
    ax.xaxis.set_major_locator(plt.MaxNLocator(3))
    ax.xaxis.set_major_formatter(mdates.DateFormatter(DATETIME_FORMAT))
    plt.xticks(rotation=70)
    plt.show()


if __name__ == "__main__":
    filename = sys.argv[1]
    do_plot(filename)


