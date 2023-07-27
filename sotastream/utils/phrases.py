import random


class PhraseSpanExtractor:
    """Re-implementation of phrase span extraction algorithm from Moses"""

    def __init__(self, srcSpans, trgSpans, alignment, maxLength=7):
        self.srcSpans = srcSpans
        self.trgSpans = trgSpans
        self.alignment = alignment
        self.maxLength = maxLength

        self.srcLength = len(srcSpans)
        self.trgLength = len(trgSpans)
        self.phrases = []
        self.marked = set([q for _, q in alignment])

    def extract(self, srcStart, srcEnd, trgStart, trgEnd):
        if trgEnd == -1:
            return []
        for p, q in self.alignment:
            if trgStart <= q <= trgEnd and (p < srcStart or p > srcEnd):
                return []
        E = []
        ts = trgStart
        while True:
            te = trgEnd
            while True:
                if te - ts < self.maxLength:
                    E.append(((srcStart, srcEnd), (ts, te)))
                else:
                    break
                te += 1
                if te in self.marked or te >= self.trgLength:
                    break
            ts -= 1
            if ts in self.marked or ts < 0:
                break
        return E

    def computePhraseSpans(self):
        for srcStart in range(self.srcLength):
            for srcEnd in range(srcStart, self.srcLength):
                if srcEnd - srcStart >= self.maxLength:
                    break
                trgStart = self.trgLength - 1
                trgEnd = -1
                for p, q in self.alignment:
                    if srcStart <= p <= srcEnd:
                        trgStart = min(q, trgStart)
                        trgEnd = max(q, trgEnd)
                E = self.extract(srcStart, srcEnd, trgStart, trgEnd)
                for p in E:
                    (sb, se), (tb, te) = p
                    self.phrases.append(
                        (
                            (self.srcSpans[sb][0], self.srcSpans[se][1]),
                            (self.trgSpans[tb][0], self.trgSpans[te][1]),
                        )
                    )

    def samplePhraseSpans(self, k=1):
        k = min(k, len(self.phrases))
        if k:
            return random.choices(
                self.phrases, weights=[2 / (s[1] - s[0] + t[1] - t[0] + 2) for s, t in self.phrases], k=k
            )
        else:
            return []

    def getPhraseSpans(self):
        return self.phrases
