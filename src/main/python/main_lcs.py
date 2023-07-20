import sys

import matplotlib.pyplot as plt
import numpy as np

from kafka_log_parser import LogLine
from lcs import lcs, out_of_order
from main_compare_sequences import sequences_of, log_to_index
from rendering import human_readable
from text_utils import delimiting

WORD_SHINGLES = {2,3}
WORD_PENALTY = 1e-2
CHAR_SHINGLES = {2, 3, }


def to_logs(hash_to_logs: dict, machine: str) -> [int]:
    log_bins = log_to_index(hash_to_logs)
    log_bins = sorted(log_bins.items(), key=lambda x: (x[0].thread, x[0].timestamp))
    xs = list(reversed([bin for log, bin in log_bins if log.machine == machine])) #[:slice]
    print(f"Log size = {len(log_bins)} of which {len(xs)} are for machine {machine}")
    return xs


def filter(log_lines: [LogLine], machine: str) -> [LogLine]:
    return [x for x in log_lines if x.machine == machine]


def print_differences(first_logs: [LogLine],
                      first_delta: [int],
                      second_logs: [LogLine],
                      second_delta: [int],
                      machine: str,
                      label: str,
                      ignoring: [str]):
    first_logs = filter(first_logs, machine)
    second_logs = filter(second_logs, machine)
    print(f"Number of log lines is {len(first_logs)} and {len(second_logs)}")
    with open(f"/tmp/{delimiting(machine, '')}_first_{label}.log", "w") as f:
        print("First deltas")
        write_to_file(f, first_delta, first_logs, ignoring)
    with open(f"/tmp/{delimiting(machine, '')}_second_{label}.log", "w") as f:
        print("\nSecond deltas")
        f.write("\n")
        write_to_file(f, second_delta, second_logs, ignoring)


def write_to_file(f,
                  index: [int],
                  log_lines: [LogLine],
                  ignoring: [str]):
    for i in index:
        line = human_readable(log_lines[i])
        if not any([x in line.lower() for x in ignoring]):
            x = f"{i}: {line}"
            print(x)
            f.write(f"{x}\n")


def check_sequences(first_hash_to_logs: dict, second_hash_to_logs: dict, machine: str) -> [int]:
    m = lcs(to_logs(first_hash_to_logs, machine),
            to_logs(second_hash_to_logs, machine))
    print(f"{m[0, 0]} out of {min(m.shape[0], m.shape[1])} in order")
    # plot_heatmap(m)
    return out_of_order(m)


def plot_heatmap(m: np.ndarray):
    fig = plt.figure(figsize=(16, 6))
    plt.imshow(m, cmap='hot', interpolation='nearest')
    plt.show()


def compare_lcs(first_file, second_file, english_file):
    # Not sure about ignoring. We seem to be losing useful things like:
    # Vote request VoteRequestData(clusterId='AQIDBAUGBwgJCgsMDQ4PEA', ...
    first_hash_to_logs, first_logs, second_hash_to_logs, second_logs, ignored_words = sequences_of(
        first_file, second_file, english_file)
    machine = "kafka1:"
    first_delta = check_sequences(first_hash_to_logs, second_hash_to_logs, machine)
    second_delta = check_sequences(second_hash_to_logs, first_hash_to_logs, machine)
    print_differences(first_logs, first_delta, second_logs, second_delta, machine, "all", ignored_words)

    first_logs = filter(first_logs, machine)
    second_logs = filter(second_logs, machine)
    first_log_to_index = log_to_index(first_hash_to_logs)
    second_log_to_index = log_to_index(second_hash_to_logs)
    for x in first_delta:
        first = first_logs[x]
        second = second_logs[x]
        first_hash = first_log_to_index[first]
        second_hash = second_log_to_index[second]
        first_line = human_readable(first)
        second_line = human_readable(second)
        if first_hash != second_hash and not any([x in first_line.lower() for x in ignored_words]) and not any([x in second_line.lower() for x in ignored_words]):
            print("{:<10} {:}".format(first_hash, first_line))
            print("{:<10} {:}".format(second_hash, second_line))
            print("\n")

    first_log_index = [(first_logs[i], first_log_to_index[first_logs[i]]) for i in first_delta]
    print(f"no. deviations = {len(first_log_index)}")
    fig, ax = plt.subplots(1, 1)
    fig = plt.figure(figsize=(16,6))
    plt.scatter(first_delta, [first_log_to_index[first_logs[i]] for i in first_delta], s=1, c="red", label=machine)
    plt.scatter(second_delta, [second_log_to_index[second_logs[i]] for i in first_delta], s=1, c="blue", label=machine)
    plt.show()


if __name__ == "__main__":
    first_file = sys.argv[2]
    second_file = sys.argv[3]
    english_file = sys.argv[1]
    compare_lcs(first_file, second_file, english_file)
