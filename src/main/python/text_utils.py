import string
from collections import defaultdict
import math
from re import finditer
import re

from kafka_log_parser import LogLine


def camel_case_split(identifier: str) -> [str]:
    matches = finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', identifier)
    return [m.group(0) for m in matches]


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


def delimiting(x: str) -> str:
    for delimiter in string.punctuation:
        x = x.replace(delimiter, " ")
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
    return words


def word_shingle_probabilities_from(words: [str], char_shingles: set) -> dict:
    char_freq = frequencies(words, char_shingles, None)
    probabilities = normalize(char_freq)
    return probabilities


def normalize(token_freq: dict):
    n = sum([v for v in token_freq.values()])
    probabilities = {k: v / n for k, v in token_freq.items()}
    return probabilities


def sorted_word_average_entropy(docs: [str], probabilities: dict, shingles: set, penalty: float) -> dict:
    words = list(frequencies(docs, {1}).keys())
    word_entropy = average_entropy_of(words, probabilities, shingles, None, True, penalty=penalty)
    word_scores = sorted(zip(words, word_entropy), key=lambda x: x[1])
    return word_scores
