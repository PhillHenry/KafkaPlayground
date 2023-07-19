import sys

import matplotlib.pyplot as plt
import numpy as np

from kafka_log_parser import LogLine
from main_compare_sequences import sequences_of, log_to_index
from rendering import human_readable

WORD_SHINGLES = {2,3}
WORD_PENALTY = 1e-2
CHAR_SHINGLES = {2, 3, }


def lcs(xs, ys):
    m = np.zeros([len(xs), len(ys)], float)

    x = len(xs) - 1
    y = len(ys) - 1
    for i in range(x, -1, -1):
        for j in range(y, -1, -1):
            if i == x or j == y:
                m[i, j] = 0
            elif xs[i] == ys[j]:
                m[i, j] = m[i + 1, j + 1] + 1
            else:
                m[i, j] = max(m[i, j + 1], m[i + 1, j])
    return m


def to_logs(hash_to_logs: dict, slice: int, machine: str):
    log_bins = log_to_index(hash_to_logs)
    log_bins = sorted(log_bins.items(), key=lambda x: x[0].timestamp)
    xs = list(reversed([bin for log, bin in log_bins if log.machine == machine]))[:slice]
    for x in log_bins[:20]:
        print(human_readable(x[0]))
    return xs


def out_of_order(m: np.ndarray, first_logs: [LogLine], second_logs: [LogLine]):
    print(f"\nOut of Order ({m.shape}:")
    i = j = 0
    while i < m.shape[0] - 2 and j < m.shape[1] - 2:
        d = m[i, j]
        if m[i + 1, j + 1] < d:
            i += 1
            j += 1
        elif m[i + 1, j] < m[i, j + 1]:
            print(f"First: {human_readable(first_logs[i])}")
            i += 1
        else:
            print(f"Second: {human_readable(second_logs[j])}")
            j += i


def filter(log_lines: [LogLine], machine: str) -> [LogLine]:
    return [x for x in log_lines if x.machine == machine]


if __name__ == "__main__":
    first_hash_to_logs, first_logs, second_hash_to_logs, second_logs = sequences_of(sys.argv[2], sys.argv[3], sys.argv[1])
    machine = "kafka1:"
    slice = 800
    m = lcs(to_logs(first_hash_to_logs, slice, machine), to_logs(second_hash_to_logs, slice, machine))
    first_logs = filter(first_logs, machine)
    second_logs = filter(second_logs, machine)
    out_of_order(m, first_logs, second_logs)
    print(f"{m[0, 0]} out of {slice} in order")
    fig = plt.figure(figsize=(16,6))
    plt.imshow(m, cmap='hot', interpolation='nearest')
    # plt.show()
