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


def to_logs(hash_to_logs: dict, machine: str) -> ([int], dict):
    log_to_hash = {log: hash for log, hash in log_to_index(hash_to_logs).items() if log.machine == machine}
    log_bins = sorted(log_to_hash.items(), key=lambda x: (x[0].thread, x[0].timestamp))
    xs = list(reversed([bin for log, bin in log_bins if log.machine == machine])) #[:slice]
    print(f"Log size = {len(log_bins)} of which {len(xs)} are for machine {machine}")
    return xs, log_to_hash


def filter(log_lines: [LogLine], machine: str) -> [LogLine]:
    return [x for x in log_lines if x.machine == machine]


def print_differences(first_logs: [LogLine],
                      first_delta: [int],
                      second_logs: [LogLine],
                      machine: str,
                      label: str,
                      ignoring: [str],
                      first_log_to_index: dict,
                      hash_to_logs: dict):
    print(f"{BColors.RED}number of {label}: {len(first_delta)} ")
    if len(first_delta) > 0:
        first_logs = filter(first_logs, machine)
        second_logs = filter(second_logs, machine)
        print(f"Number of log lines is {len(first_logs)} and {len(second_logs)}")
        machine = delimiting(machine, '')
        with open(f"/tmp/{machine}_first_{label}.log", "w") as f:
            print("First deltas")
            write_to_file(f, first_delta, first_logs, ignoring, first_log_to_index, hash_to_logs)
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
                  log_to_index: dict,
                  hash_to_logs: dict):
    last_bin = -1
    no_dupe_count = 0
    last_blank = False
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
        # Squelch
        count = len(hash_to_logs[log_to_index[log]])
        if count < 6:
            print(x)
            last_blank = False
        else:
            if not last_blank:
                print(f"{BColors.DARKGRAY}...")
            last_blank = True
        f.write(f"{x}\n")
        last_bin = bin
    print(f"{BColors.HEADER}{no_dupe_count}/{len(index)} non duped out of a total of {len(log_lines)} lines")


def ignore_commons(hash_to_logs: dict) -> dict:
    return {k: v for k, v in hash_to_logs.items() if len(v) <= 10}


def check_sequences(first_hash_to_logs: dict, second_hash_to_logs: dict, machine: str) -> ([int], [int], [int]):
    first_logs, first_log_to_hash = to_logs(first_hash_to_logs, machine)
    second_logs, second_log_to_hash = to_logs(second_hash_to_logs, machine)
    m = lcs(first_logs, second_logs)
    print(f"{m[0, 0]} out of {min(m.shape[0], m.shape[1])} in order")
    # plot_heatmap(m)
    print_sample_of(m, 20)
    return out_of_order(m, to_ordered_by_timestamp_hash(first_log_to_hash), to_ordered_by_timestamp_hash(second_log_to_hash))


def to_ordered_by_timestamp_hash(log_to_hash: dict) -> [int]:
    return [log_to_hash[log] for log in sorted(log_to_hash.keys(), key=lambda x: (x.timestamp, x.payload_str))]


def print_sample_of(m, size):
    np.set_printoptions(threshold=size + 1, precision=3)
    for i in range(size):
        print(m[i][:size])
    print()
    for i in range(size, 0, -1):
        print(m[m.shape[0] - i][-size:])


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
    first_delta, first_missing, first_surplus = check_sequences(first_hash_to_logs, second_hash_to_logs, machine)
    second_delta, second_missing, second_surplus = check_sequences(second_hash_to_logs, first_hash_to_logs, machine)
    first_log_to_index = log_to_index(first_hash_to_logs)
    second_log_to_index = log_to_index(second_hash_to_logs)
    print_differences(first_logs, first_delta, second_logs, machine, "all", [], first_log_to_index, first_hash_to_logs)
    # print_differences(first_logs, first_missing, second_logs, second_missing, machine, "missing", [], first_log_to_index, second_log_to_index)
    # print_differences(first_logs, first_surplus, second_logs, second_surplus, machine, "surplus", [], first_log_to_index, second_log_to_index)
    print(f"Number of bins in first  = {len(first_hash_to_logs)}")
    print(f"Number of bins in second = {len(second_hash_to_logs)}")
    write_hashes("first", first_logs, first_log_to_index, machine)
    write_hashes("second", second_logs, second_log_to_index, machine)

    # first_logs = filter(first_logs, machine)
    # second_logs = filter(second_logs, machine)
    # compare_discontinuous_points(first_delta, first_log_to_index, first_logs, ignored_words,
    #                              second_log_to_index, second_logs)

    # plot_bins_of_discontinuities(first_delta, first_log_to_index, first_logs, machine, second_delta,
    #                              second_log_to_index, second_logs)


def write_hashes(label: str, logs: [LogLine], logs_to_index: dict, machine :str):
    with open(f"/tmp/{label}_hashes.txt", "w") as f:
        for log in logs:
            if log.machine == machine:
                hash = logs_to_index[log]
                f.write(f"{hash}\n")


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
    print(f"{BColors.RED}")
    for index in first_delta:
        first_index = second_index = index
        first_hash, first_line = has_and_line(first_log_to_index, first_logs, first_index)
        second_hash, second_line = has_and_line(second_log_to_index, second_logs, second_index)
        first_index_str = f"{BColors.OKGREEN}{first_index}"
        second_index_str = f"{BColors.OKGREEN}{second_index}"
        first_color = f"{BColors.RED}"
        second_color = f"{BColors.OKBLUE}"
        if first_hash == second_hash:
            second_index = index - 1
            second_hash, second_line = has_and_line(second_log_to_index, second_logs, second_index)
            second_index_str = second_index_str + f" -> {second_index}"
            if first_hash == second_hash:
                second_color = f"{BColors.DARKGRAY}"
                first_color = f"{BColors.DARKGRAY}"
        print("{:<20}: {:<10} {:}".format(first_index_str, f"{BColors.OKCYAN}{first_hash}", f"{first_color}{first_line}"))
        print("{:<20}: {:<10} {:}".format(second_index_str, f"{BColors.OKCYAN}{second_hash}", f"{second_color}{second_line}"))
        print("\n")


def has_and_line(log_to_index: dict, logs: [str], index: int):
    first = logs[index]
    first_hash = log_to_index[first]
    first_line = human_readable(first)
    return first_hash, first_line


if __name__ == "__main__":
    first_file = sys.argv[2]
    second_file = sys.argv[3]
    english_file = sys.argv[1]
    compare_lcs(first_file, second_file, english_file)
