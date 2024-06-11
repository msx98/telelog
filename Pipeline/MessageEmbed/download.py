from sentence_transformers import SentenceTransformer
import os

SentenceTransformer(os.environ["SENTENCE_TRANSFORMERS_MODEL_NAME"])