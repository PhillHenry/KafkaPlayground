from lcs import lcs, out_of_order


def test_lcs_same_seq():
    n = 10
    xs = list(range(n))
    m = lcs(xs, xs)
    assert m.shape == (n + 1, n + 1)
    assert m[0][0] == n
    assert len(deltas(m, xs, xs)) == 0


def deltas(m, xs, ys):
    deltas, _, _ = out_of_order(m, xs ,ys)
    return deltas


def test_lcs_sub_seq():
    n = 10
    m = 3
    xs = list(range(n))
    ys = xs[:m]
    matrix = lcs(xs, ys)
    assert matrix.shape == (n + 1, m + 1)
    assert matrix[0][0] == m
    assert len(deltas(matrix, xs, ys)) == 0


def test_out_of_order():
    xs = "nematode knowledge"
    ys = "empty bottle"
    m = lcs(ys, xs)
    print(f"\n{m}")
    expected_matches = [1, 2, 4, 8, 11, 13, 14]
    assert m[0][0] == len(expected_matches)
    assert m.shape == (len(ys) + 1, len(xs) + 1)
    assert deltas(m, ys, xs) == [x for x in range(len(xs)) if x not in expected_matches and x <= len(ys)]
