import sys

import matplotlib.pyplot as plt
import numpy as np

from main_compare_sequences import sequences_of, log_to_index

WORD_SHINGLES = {2,3}
WORD_PENALTY = 1e-2
CHAR_SHINGLES = {2, 3, }


def lcs(xs, ys):
    m = np.zeros([len(xs), len(ys)], float)

    x = len(xs) - 1
    y = len(ys) - 1
    for i in range(x, -1, -1):
        for j in range(y, -1, -1):
            if i == x and j == y:
                m[i, j] = 0
            elif xs[i] == ys[j]:
                m[i, j] = m[i + 1, j + 1] + 1
            else:
                if i == x:
                    m[i, j] = m[i, j + 1]
                elif j == y:
                    m[i, j] = m[i + 1, j]
                else:
                    m[i, j] = max(m[i, j + 1], m[i + 1, j])

    return m


if __name__ == "__main__":
    first_hash_to_logs, first_lines, second_hash_to_logs, second_lines = sequences_of(sys.argv[2], sys.argv[3], sys.argv[1])
    machine = "kafka1:"
    first_log_indices = list(log_to_index(first_hash_to_logs).values())
    second_log_indices = list(log_to_index(second_hash_to_logs).values())
    m = lcs(first_log_indices, second_log_indices)
    plt.imshow(m, cmap='hot', interpolation='nearest')
    plt.show()
