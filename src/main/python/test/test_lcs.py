from lcs import lcs, out_of_order


def test_lcs_same_seq():
    n = 10
    xs = list(range(n))
    m = lcs(xs, xs)
    assert m.shape == (n, n)
    assert m[0][0] == n - 1
    assert len(out_of_order(m)) == 0


def test_lcs_sub_seq():
    n = 10
    m = 3
    xs = list(range(n))
    ys = xs[:m]
    matrix = lcs(xs, ys)
    assert matrix.shape == (n, m)
    assert matrix[0][0] == m - 1
    assert len(out_of_order(matrix)) == 0


def test_out_of_order():
    xs = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    ys = [0, 2, 1, 4, 3, 6, 5, 8, 7, 9]
    m = lcs(xs, ys)
    print(f"\n{m}")
    assert m.shape == (len(xs), len(ys))
    assert m[0][0] == 5
    assert len(out_of_order(m)) == 4
