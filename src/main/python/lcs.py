import numpy as np

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


def out_of_order(m: np.ndarray) -> [int]:
    deltas = set([])
    print(f"\nOut of Order ({m.shape}:")
    i = j = 0
    while i < m.shape[0] - 2 and j < m.shape[1] - 2:
        d = m[i, j]
        if m[i + 1, j + 1] < d:
            i += 1
            j += 1
        elif m[i + 1, j] < m[i, j + 1]:
            deltas.add(i)
            i += 1
        elif m[i, j + 1] < m[i + 1, j]:
            deltas.add(i)
            j += 1
        else:
            deltas.add(i)
            j += i
    print(f"Number of deltas {len(deltas)}")
    return list(sorted(list(deltas)))
