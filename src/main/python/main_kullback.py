import sys

from kafka_log_parser import read_file, read_plain_file
from text_utils import clean, \
    word_shingle_probabilities_from, sorted_word_average_entropy, frequencies, normalize, \
    kullback_liebler

WORD_PENALTY = 1e-2
CHAR_SHINGLES = {2, 3, }
WORD_SHINGLES = {3, 4, 5}


def information(words_file: str, first: str, second: str):
    first_lines = read_file(first)
    first_docs = list(map(clean, first_lines))
    second_lines = read_file(second)
    second_docs = list(map(clean, second_lines))
    english = read_plain_file(words_file)
    char_freq = word_shingle_probabilities_from(english, CHAR_SHINGLES)

    ps = word_probabilities(first_docs)
    qs = word_probabilities(second_docs)

    first_top_word_scores = most_entropic(first_docs, char_freq)

    kl = kullback_liebler(first_docs, first_top_word_scores, ps, qs)
    docs_to_kl = list(set(zip(first_docs, kl)))
    sorted_kl = sorted(docs_to_kl, key=lambda x: x[1])
    print("Lowest 20")
    for doc, kl in sorted_kl[:20]:
        print(f"{kl} {doc}")
    print("Top 20")
    for doc, kl in sorted_kl[-20:]:
        print(f"{kl} {doc}")


def word_probabilities(docs: [str]):
    word_count = frequencies(docs, {1})
    probabilities = normalize(word_count)
    return probabilities


def most_entropic(docs: [str], char_freq: dict) -> [str]:
    word_score = sorted_word_average_entropy(docs, char_freq, CHAR_SHINGLES, WORD_PENALTY)
    max_score = word_score[-1][1]
    return [w for w, s in word_score if s > max_score / 2]


if __name__ == "__main__":
    information(sys.argv[1], sys.argv[2], sys.argv[3])
