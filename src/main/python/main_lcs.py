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


def to_logs(hash_to_logs: dict, machine: str):
    log_bins = log_to_index(hash_to_logs)
    log_bins = sorted(log_bins.items(), key=lambda x: x[0].timestamp)
    xs = list(reversed([bin for log, bin in log_bins if log.machine == machine])) #[:slice]
    print(f"Log size = {len(log_bins)} of which {len(xs)} are for machine {machine}")
    return xs


def filter(log_lines: [LogLine], machine: str) -> [LogLine]:
    return [x for x in log_lines if x.machine == machine]


def print_differences(first_logs: [LogLine],
                      first_delta: [int],
                      second_logs: [LogLine],
                      second_delta: [int],
                      machine: str):
    first_logs = filter(first_logs, machine)
    second_logs = filter(second_logs, machine)
    print(f"Number of log lines is {len(first_logs)} and {len(second_logs)}")
    with open(f"/tmp/{delimiting(machine, '')}_first.log", "w") as f:
        print("First deltas")
        write_to_file(f, first_delta, first_logs)
    with open(f"/tmp/{delimiting(machine, '')}_second.log", "w") as f:
        print("\nSecond deltas")
        f.write("\n")
        write_to_file(f, second_delta, second_logs)


def write_to_file(f, index: [int], log_lines: [LogLine]):
    for i in index:
        x = f"{i}: {human_readable(log_lines[i])}"
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


if __name__ == "__main__":
    first_hash_to_logs, first_logs, second_hash_to_logs, second_logs = sequences_of(sys.argv[2], sys.argv[3], sys.argv[1])
    machine = "kafka1:"
    first_delta = check_sequences(first_hash_to_logs, second_hash_to_logs, machine)
    second_delta = check_sequences(second_hash_to_logs, first_hash_to_logs, machine)
    print_differences(first_logs, first_delta, second_logs, second_delta, machine)
