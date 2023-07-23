import sys

import matplotlib.pyplot as plt
import numpy as np

from comparisons import search_nearby_bins
from kafka_log_parser import LogLine
from lcs import lcs, out_of_order
from main_compare_sequences import sequences_of, log_to_index, VEC_SIZE
from rendering import human_readable, BColors
from text_utils import delimiting

CONTEXT_IGNORE_WORDS = ["apache", "org", "bitnami"]


def to_logs(hash_to_logs: dict, machine: str) -> [int]:
    hash_to_logs = ignore_commons(hash_to_logs)
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
                      ignoring: [str],
                      first_log_to_index: dict,
                      second_log_to_index: dict):
    first_logs = filter(first_logs, machine)
    second_logs = filter(second_logs, machine)
    print(f"Number of log lines is {len(first_logs)} and {len(second_logs)}")
    machine = delimiting(machine, '')
    with open(f"/tmp/{machine}_first_{label}.log", "w") as f:
        print("First deltas")
        write_to_file(f, first_delta, first_logs, ignoring, first_log_to_index)
    with open(f"/tmp/{machine}_first_{label}_raw.log", "w") as f:
        for log in first_logs:
            f.write(f"{human_readable(log)}\n")
    with open(f"/tmp/{machine}_second_{label}_raw.log", "w") as f:
        for log in second_logs:
            f.write(f"{human_readable(log)}\n")
    # with open(f"/tmp/{delimiting(machine, '')}_second_{label}.log", "w") as f:
    #     print("\nSecond deltas")
    #     f.write("\n")
    #     write_to_file(f, second_delta, second_logs, ignoring, second_log_to_index)


def write_to_file(f,
                  index: [int],
                  log_lines: [LogLine],
                  ignoring: [str],
                  log_to_index: dict):
    last_bin = -1
    no_dupe_count = 0
    for i in index:
        log = log_lines[i]
        line = human_readable(log)
        bin = log_to_index[log]

        query = np.unpackbits(np.array([bin], dtype='>i8').view(np.uint8))[-VEC_SIZE:]
        if last_bin != -1:
            candidates = search_nearby_bins(query, {last_bin: []}, search_radius=3)
        else:
            candidates = {}
        # print(f"candidates = {candidates}")
        x = f"{line}"
        if last_bin == bin or len(candidates) > 0:
            x = f"{BColors.DARKGRAY}{x}"
        else:
            no_dupe_count += 1
            x = f"{BColors.BOLD}{BColors.RED}{x}{BColors.UNBOLD}"
        x = "{:<10}{}".format(f"{BColors.OKGREEN}{i}:", x)
        print(x)
        f.write(f"{x}\n")
        last_bin = bin
    print(f"{BColors.HEADER}{no_dupe_count}/{len(index)} non duped out of a total of {len(log_lines)} lines")


def ignore_commons(hash_to_logs: dict) -> dict:
    return {k: v for k, v in hash_to_logs.items() if len(v) <= 10}


def check_sequences(first_hash_to_logs: dict, second_hash_to_logs: dict, machine: str) -> [int]:
    m = lcs(to_logs(first_hash_to_logs, machine),
            to_logs(second_hash_to_logs, machine))
    print(f"{m[0, 0]} out of {min(m.shape[0], m.shape[1])} in order")
    # plot_heatmap(m)
    size = 20
    np.set_printoptions(linewidth=sys.maxsize)
    np.set_printoptions(threshold=size+1)
    for i in range(size):
        print(m[i][:size])
    for i in range(size):
        print(m[size-i:][:size])
    return out_of_order(m)


def plot_heatmap(m: np.ndarray):
    fig = plt.figure(figsize=(16, 6))
    plt.imshow(m, cmap='hot', interpolation='nearest')
    plt.show()


def compare_lcs(first_file, second_file, english_file):
    # Not sure about ignoring. We seem to be losing useful things like:
    # Vote request VoteRequestData(clusterId='AQIDBAUGBwgJCgsMDQ4PEA', ...
    first_hash_to_logs, first_logs, second_hash_to_logs, second_logs, ignored_words = sequences_of(
        first_file, second_file, english_file, CONTEXT_IGNORE_WORDS)
    machine = "kafka1:"
    first_delta = check_sequences(first_hash_to_logs, second_hash_to_logs, machine)
    second_delta = check_sequences(second_hash_to_logs, first_hash_to_logs, machine)
    first_log_to_index = log_to_index(first_hash_to_logs)
    second_log_to_index = log_to_index(second_hash_to_logs)
    print_differences(first_logs, first_delta, second_logs, second_delta, machine, "all", [], first_log_to_index, second_log_to_index)
    print(f"Number of bins in first  = {len(first_hash_to_logs)}")
    print(f"Number of bins in second = {len(second_hash_to_logs)}")

    first_logs = filter(first_logs, machine)
    second_logs = filter(second_logs, machine)
    # compare_discontinuous_points(first_delta, first_log_to_index, first_logs, ignored_words,
    #                              second_log_to_index, second_logs)

    # plot_bins_of_discontinuities(first_delta, first_log_to_index, first_logs, machine, second_delta,
    #                              second_log_to_index, second_logs)


def plot_bins_of_discontinuities(first_delta, first_log_to_index, first_logs, machine, second_delta,
                                 second_log_to_index, second_logs):
    first_log_index = [(first_logs[i], first_log_to_index[first_logs[i]]) for i in first_delta]
    print(f"no. deviations = {len(first_log_index)}")
    fig, ax = plt.subplots(1, 1)
    fig = plt.figure(figsize=(16, 6))
    plt.scatter(first_delta, [first_log_to_index[first_logs[i]] for i in first_delta], s=1, c="red",
                label=machine)
    plt.scatter(second_delta, [second_log_to_index[second_logs[i]] for i in first_delta], s=1,
                c="blue", label=machine)
    plt.show()


def compare_discontinuous_points(first_delta, first_log_to_index, first_logs, ignored_words,
                                 second_log_to_index, second_logs):
    for x in first_delta:
        first = first_logs[x]
        second = second_logs[x]
        first_hash = first_log_to_index[first]
        second_hash = second_log_to_index[second]
        first_line = human_readable(first)
        second_line = human_readable(second)
        if first_hash != second_hash and not any(
                [x in first_line.lower() for x in ignored_words]) and not any(
                [x in second_line.lower() for x in ignored_words]):
            print("{:<10} {:}".format(first_hash, first_line))
            print("{:<10} {:}".format(second_hash, second_line))
            print("\n")


if __name__ == "__main__":
    first_file = sys.argv[2]
    second_file = sys.argv[3]
    english_file = sys.argv[1]
    compare_lcs(first_file, second_file, english_file)
