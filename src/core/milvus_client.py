import os
from pymilvus import connections, Collection, CollectionSchema, FieldSchema, DataType, utility

# READ ENV VARIABLES
HOST = os.getenv("MILVUS_HOST", "localhost")
PORT = os.getenv("MILVUS_PORT", "19531")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "audio_fingerprints")
VECTOR_DIM=128

def connect_milvus():
    if connections.has_connection("default"): return
    print(f"🔌 Connecting to Milvus at {HOST}:{PORT}...")
    try:
        connections.connect("default", host=HOST, port=PORT)
        print("✅ Connected!")
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        raise

def get_milvus_collection():
    """Gets the collection object, creating it if it doesn't exist."""
    
    # 1. ENSURE CONNECTION EXISTS
    connect_milvus()
    
    # 2. Check if collection exists
    # This was the line causing your error previously
    if utility.has_collection(COLLECTION_NAME):
        print(f"📂 Loading existing collection: {COLLECTION_NAME}")
        col = Collection(COLLECTION_NAME)
        col.load()
        return col

    # 3. Create Schema if it doesn't exist
    print("⚠️ Collection not found. Creating new schema...")

    field_id = FieldSchema(
        name="id",
        dtype=DataType.INT64,
        is_primary=True,
        auto_id=True
    )
    field_vec = FieldSchema(
        name="embedding",
        dtype=DataType.FLOAT_VECTOR,
        dim=VECTOR_DIM
    )
    # NEW: mbid field
    field_mbid = FieldSchema(
        name="mbid",
        dtype=DataType.VARCHAR,
        max_length=64,   # MBID UUID fits easily
        is_primary=False
    )
    # Rename field to offsets (to match your desired schema)
    field_offset = FieldSchema(
        name="offsets",
        dtype=DataType.FLOAT
    )
    field_is_mbid_present = FieldSchema(
    name="is_mbid_present",
    dtype=DataType.BOOL
    )
    schema = CollectionSchema(
        fields=[field_id, field_vec, field_mbid, field_offset, field_is_mbid_present],
        description="Music Fingerprints with MBID"
    )
    collection = Collection(name=COLLECTION_NAME, schema=schema)

    # 4. Create Index
    index_params = {
        "metric_type": "COSINE",
        "index_type": "IVF_FLAT",
        "params": {"nlist": 1024}
    }
    collection.create_index(field_name="embedding", index_params=index_params)
    collection.load()
    return collection

def reset_collection():
    """Drops the table and creates a fresh empty one"""
    connect_milvus()
    if utility.has_collection(COLLECTION_NAME):
        utility.drop_collection(COLLECTION_NAME)
    
    return get_milvus_collection()