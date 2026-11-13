"""Shared HTTP Chroma sink; Member 2 can instrument upsert()."""
import os
import time

import chromadb
import httpx
import requests

from common import MODEL, MODEL_REVISION


def connect(attempts=1):
    # Never silently fall back to a different embedded database.
    host = os.getenv("CHROMA_HOST", "localhost")
    port = int(os.getenv("CHROMA_PORT", "8001"))
    for attempt in range(attempts):
        try:
            # Check database readiness, not just the process heartbeat. In 1.5.9
            # HttpClient constructs its HTTP session with timeout=None.
            response = requests.get(
                f"http://{host}:{port}/api/v2/tenants/default_tenant/databases/default_database",
                timeout=(3, 10))
            response.raise_for_status()
            client = chromadb.HttpClient(host=host, port=port)
            # Private adapter is deliberately tied to our pinned Chroma version.
            client._server._session.timeout = httpx.Timeout(120, connect=10)
            return client
        except Exception:
            if attempt + 1 == attempts:
                raise
            time.sleep(1)


def hnsw_config():
    return {"space": "cosine", "max_neighbors": int(os.getenv("HNSW_M", "16")),
            "ef_construction": int(os.getenv("HNSW_EF_CONSTRUCTION", "100")),
            "ef_search": int(os.getenv("HNSW_EF_SEARCH", "50"))}


class ChromaSink:
    def __init__(self, collection, run_id):
        self.client = connect(attempts=5)
        self.client.heartbeat()
        metadata = {"run_id": run_id, "embedding_model": MODEL, "model_revision": MODEL_REVISION}
        self.col = self.client.get_or_create_collection(
            name=collection, embedding_function=None, metadata=metadata,
            configuration={"hnsw": hnsw_config()})
        for key, value in metadata.items():
            if (self.col.metadata or {}).get(key) != value:
                raise ValueError(f"Collection {collection} has incompatible {key}; use a new run-id")
        actual = self.col.configuration.get("hnsw") or {}
        for key, value in hnsw_config().items():
            if actual.get(key) != value:
                raise ValueError(f"Collection has different HNSW {key}; use a new run-id")

    def upsert(self, ids, embeddings, documents, metadatas, chunk=500):
        if not (len(ids) == len(embeddings) == len(documents) == len(metadatas)):
            raise ValueError("Mismatched vector payload lengths")
        if len(set(ids)) != len(ids):
            raise ValueError("Duplicate IDs in vector payload")
        if chunk <= 0:
            raise ValueError("chunk must be positive")
        chunk = min(chunk, self.client.get_max_batch_size())
        for i in range(0, len(ids), chunk):
            self.col.upsert(ids=ids[i:i + chunk], embeddings=embeddings[i:i + chunk],
                            documents=documents[i:i + chunk], metadatas=metadatas[i:i + chunk])

    def count(self):
        return self.col.count()
