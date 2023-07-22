import sys

import matplotlib.pyplot as plt

from main_lsh import do_lsh_using_tf_idf


def do_plot(log_index: []):
    print(f"{len(log_index)}")
    fig, ax = plt.subplots(1, 1)
    fig = plt.figure(figsize=(16,6))
    plot_timeseries(log_index, "kafka3:", "red")
    plot_timeseries(log_index, "kafka2:", "cyan")
    plot_timeseries(log_index, "kafka1:", "green")
    plt.legend()


def plot_timeseries(log_index: [], machine: str, colour: str):
    timestamps = [l.timestamp for l, i in log_index if l.machine == machine]
    indices = [i for l, i in log_index if l.machine == machine]
    plt.scatter(timestamps, indices, s=1, c=colour, label=machine)


def timeseries(input_file: str):
    table, random_vectors, bin_indices, bin_indices_bits, log_lines = do_lsh_using_tf_idf(input_file, 8)
    do_plot(list(zip(log_lines, bin_indices)))


if __name__ == "__main__":
    filename = sys.argv[1]
    timeseries(filename)
    file = filename[filename.rfind("/")+1:]
    print(f"Saving {file}")
    plt.savefig(f"/tmp/all_{file}.pdf")
    plt.show()