import string
from collections import defaultdict
import math
from re import finditer
import re
import numpy as np
from kafka_log_parser import LogLine
from rendering import human_readable


def camel_case_split(identifier: str) -> [str]:
    if not any(i.isdigit() for i in identifier):
        matches = finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', identifier)
        return [m.group(0) for m in matches]
    else:
        return [identifier]


def to_shingles(doc: str, ngrams: set[int], split_on=" "):
    if split_on is None:
        tokens = doc
        split_on = ""
    else:
        tokens = doc.split(split_on)
    words = [word for word in tokens if len(word) > 0]
    shingles = []
    for ngram in ngrams:
        for start in range(len(words) - ngram + 1):
            end = start + ngram
            if end <= len(words):
                shingle = split_on.join(words[start:end])
                shingles.append(shingle)
    return shingles


def entropy_of(tokens: [],
               probabilities: dict,
               shingles: set[int],
               delimiter=" ",
               allow_dupes=False,
               penalty=0.) -> [float]:
    entropy = []
    for token in tokens:
        h = 0
        seen = set()
        for shingle in to_shingles(token, shingles, delimiter):
            if shingle not in seen or allow_dupes:
                p = float(probabilities.get(shingle, penalty))
                if p > 0:
                    h += -p * math.log(p)
                seen.add(shingle)
        entropy.append(h)
    return entropy


def average_entropy_of(docs: [],
                       doc_freq: dict,
                       shingles: set[int],
                       delimiter=" ",
                       allow_dupes=False,
                       penalty=0.) -> [float]:
    entropies = entropy_of(docs, doc_freq, shingles, delimiter, allow_dupes, penalty)
    return [h / (len(d)) for h, d in zip(entropies, docs)]


def frequencies(docs, shingles, delimiter=" "):
    doc_freq = defaultdict(int)
    for doc in docs:
        words = to_shingles(doc, shingles, delimiter)
        for word in words:
            if len(word) > 0:
                doc_freq[word] = doc_freq[word] + 1
    return doc_freq


def remove_pure_numbers(words):
    return [w for w in words if not w.strip().isdigit()]


def delimiting(x: str, replacement=" ") -> str:
    for delimiter in string.punctuation:
        x = x.replace(delimiter, replacement)
    return x


def clean(log: LogLine) -> str:
    return clean_line(log.payload_str)


def clean_line(line: str) -> str:
    camel_expanded = [w.strip().lower() for w in camel_case_split(line)]
    no_delimiters = delimiting(" ".join(camel_expanded))
    words = no_delimiters.split(" ")
    words = remove_timings(words)
    words = remove_pure_numbers(words)
    return " ".join(words).strip()


def remove_timings(words: [str]):
    p = re.compile('\d+ms')
    words = [w for w in words if not p.match(w)]
    p = re.compile('\d+\ millisecond')
    words = [w for w in words if not p.match(w)]
    return words


def word_shingle_probabilities_from(words: [str], shingles: set) -> dict:
    char_freq = frequencies(words, shingles, None)
    probabilities = normalize(char_freq)
    return probabilities


def normalize(token_freq: dict):
    n = sum([v for v in token_freq.values()])
    probabilities = {k: v / n for k, v in token_freq.items()}
    return probabilities


def sorted_word_average_entropy(docs: [str], probabilities: dict, shingles: set, penalty: float) -> list:
    words = list(frequencies(docs, {1}).keys())
    word_entropy = average_entropy_of(words, probabilities, shingles, None, True, penalty=penalty)
    word_scores = sorted(zip(words, word_entropy), key=lambda x: x[1])
    return word_scores


def kullback_liebler(docs: [str], ignore_words: [str], ps: dict, qs: dict) -> dict:
    kl = []
    for doc in docs:
        h = 0
        for word in [w for w in doc.split(" ") if w not in ignore_words]:
            if word in qs.keys():
                p = ps[word]
                q = qs[word]
                h += p * math.log(p / q)
        kl.append(h)
    return kl


def highest_entropy_words(docs: [str], char_freq: dict, shingles: set, penalty: float) -> [str]:
    word_score = sorted_word_average_entropy(docs, char_freq, shingles, penalty)
    max_score = word_score[-1][1]
    top = [w for w, s in word_score if s > max_score / 2]
    # top = [w for w, _ in word_score][-18:]  # TODO this is somewhat arbitrary...
    return top


def lsh_bin_lines(bin_indices: np.ndarray,
                  lines: list[LogLine]) -> dict:
    lines = map(human_readable, lines)
    return lsh_bin_logs(bin_indices, lines)


def lsh_bin_logs(bin_indices: np.ndarray, lines: []) -> dict:
    pairs = list(zip(lines, bin_indices))
    hash_to_logs = defaultdict(list)
    for line, hash_code in pairs:
        if line != "":
            hash_to_logs[hash_code].append(line)
    return hash_to_logs


def words_to_ignore_in(docs: [str], char_freq: dict, shingles: set, penalty: float) -> [str]:
    top = highest_entropy_words(docs, char_freq, shingles, penalty)
    print(f"Most entropic words: {top}")
    return top
