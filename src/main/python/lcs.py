import numpy as np

WORD_SHINGLES = {2,3}
WORD_PENALTY = 1e-2
CHAR_SHINGLES = {2, 3, }


def lcs(xs, ys):
    m = np.zeros([len(xs) + 1, len(ys) + 1], float)

    x = len(xs)
    y = len(ys)
    for i in range(x, -1, -1):
        for j in range(y, -1, -1):
            if i == x or j == y:
                m[i, j] = 0
            elif xs[i] == ys[j]:
                m[i, j] = m[i + 1, j + 1] + 1
            else:
                m[i, j] = max(m[i, j + 1], m[i + 1, j])
    return m


def out_of_order(m: np.ndarray, baseline: [], xs: []) -> ([int], [int], [int]):
    missing = set([])
    surplus = set([])
    print(f"\nOut of Order ({m.shape}:")
    i = j = 0
    max_i = m.shape[0] - 2
    max_j = m.shape[1] - 2
    print(f"{m.shape[1]} {len(baseline)}")
    print(f"{m.shape[0]} {len(xs)}")
    assert m.shape[0] == len(baseline) + 1
    assert m.shape[1] == len(xs) + 1
    while i <= max_i and j <= max_j:
        old_i = i
        old_j = j
        action = ""
        if xs[j] == baseline[i]:
            i += 1
            j += 1
        elif m[i + 1, j] >= m[i, j + 1]:
            i += 1
            surplus.add(j)
            # print(f"missing {i}, {j}")
            action = "missing"
        else:
            # print(f"different {i}, {j}")
            missing.add(j)
            j += 1
            action = "different"
        # print("{:<5}{:<5}{:<5}{:<5}{:<5}".format(old_i, old_j, xs[old_j], baseline[old_i], action))

    deltas = missing.union(surplus)
    print(f"Number of deltas {len(deltas)}")
    return list(sorted(list(deltas))), list(sorted(list(missing))), list(sorted(list(surplus)))
