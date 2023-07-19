import sys

import matplotlib.pyplot as plt
import numpy as np

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
    print(m[0, 0])
    return m


def to_logs(hash_to_logs):
    log_bins = log_to_index(hash_to_logs)
    log_bins = sorted(log_bins.items(), key=lambda x: x[0].timestamp)
    xs = list(reversed([bin for _, bin in log_bins]))[:800]
    for x in log_bins[:20]:
        print(human_readable(x[0]))
    return xs


if __name__ == "__main__":
    first_hash_to_logs, first_lines, second_hash_to_logs, second_lines = sequences_of(sys.argv[2], sys.argv[3], sys.argv[1])
    machine = "kafka1:"
    m = lcs(to_logs(first_hash_to_logs), to_logs(second_hash_to_logs))
    fig, ax = plt.subplots(1, 1)
    fig = plt.figure(figsize=(16,6))
    plt.imshow(m, cmap='hot', interpolation='nearest')
    plt.show()
