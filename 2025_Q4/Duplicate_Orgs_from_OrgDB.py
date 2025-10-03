# https://elsevier.atlassian.net/browse/ORGD-13859
# title: Find the duplicate organization from orgdb.xml

import re
import numpy as np
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.feature import RegexTokenizer, HashingTF, StopWordsRemover


# --- Normalizer UDF ---
def normalize_udf():
    def normalize_txt(txt):
        if txt is None:
            return None
        txt = txt.lower()
        txt = re.sub(r"[^a-z0-9\s]", " ", txt)
        txt = re.sub(r"\s+", " ", txt).strip()
        return txt
    return udf(normalize_txt, StringType())

# --- Cosine UDF ---
def cosine_udf():
    def cosine_similarity(v1, v2):
        v1, v2 = v1.toArray(), v2.toArray()
        num = float(np.dot(v1, v2))
        denom = np.linalg.norm(v1) * np.linalg.norm(v2)
        return float(num / denom) if denom != 0 else 0.0
    return udf(cosine_similarity, DoubleType())

