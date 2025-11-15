# modules/vectorstore.py
import faiss, pickle, numpy as np
from sentence_transformers import SentenceTransformer

embed_model = SentenceTransformer('multi-qa-MiniLM-L6-cos-v1')

def build_vectorstore(texts: list, index_path: str = "embeddings/vectordb"):
    embeddings = embed_model.encode(texts)
    index = faiss.IndexFlatL2(embeddings.shape[1])
    index.add(embeddings)
    faiss.write_index(index, f"{index_path}.index")

    with open(f"{index_path}_texts.pkl", "wb") as f:
        pickle.dump(texts, f)

def retrieve_relevant_chunks(query: str, k=3, index_path: str = "embeddings/vectordb", score_threshold: float = 1.0):
    index = faiss.read_index(f"{index_path}.index")
    with open(f"{index_path}_texts.pkl", "rb") as f:
        texts = pickle.load(f)

    query_embedding = embed_model.encode([query])
    distances, indices = index.search(np.array(query_embedding), k)

    relevant_chunks = []
    for distance, idx in zip(distances[0], indices[0]):
        if distance <= score_threshold:
            relevant_chunks.append(texts[idx])

    return relevant_chunks