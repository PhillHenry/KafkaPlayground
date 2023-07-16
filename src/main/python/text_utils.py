from collections import defaultdict
import math
from re import finditer


def camel_case_split(identifier):
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


def entropy_of(docs: [],
               doc_freq: dict,
               shingles: set[int],
               delimiter=" ",
               allow_dupes=False) -> [float]:
    entropy = []
    for doc in docs:
        h = 0
        doc_words = set()
        for word in to_shingles(doc, shingles, delimiter):
            if word not in doc_words or allow_dupes:
                p = float(doc_freq.get(word, 0)) / len(docs)
                if p > 0:
                    h += -p * math.log(p)
                doc_words.add(word)
        entropy.append(h)
    return entropy


def average_entropy_of(docs: [],
                       doc_freq: dict,
                       shingles: set[int],
                       delimiter=" ",
                       allow_dupes=False) -> [float]:
    entropies = entropy_of(docs, doc_freq, shingles, delimiter, allow_dupes)
    return [h / (len(d)) for h, d in zip(entropies, docs)]


def frequencies(docs, shingles, delimiter=" "):
    doc_freq = defaultdict(int)
    for doc in docs:
        words = to_shingles(doc, shingles, delimiter)
        for word in words:
            if len(word) > 0:
                doc_freq[word] = doc_freq[word] + 1
    return doc_freq
