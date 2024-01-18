#### Fetch Response API
# http://127.0.0.1:5000/search-answers
# payload format raw-json
# {
#     "collection_name": "<collection_name>",
#     "query": "<query>"
# }


from flask import Flask, request, jsonify
from pymilvus import Collection, DataType, FieldSchema, CollectionSchema, connections
from sentence_transformers import SentenceTransformer
import torch
import os

app = Flask(__name__)

MILVUS_HOST = "localhost"
MILVUS_PORT = "19530"

# Connect to Milvus
connections.connect(host=MILVUS_HOST, port=MILVUS_PORT)
print('Connected to Milvus!')

# Load pre-trained model
model = SentenceTransformer("paraphrase-MiniLM-L6-v2")

def define_collection(collection_name):
    print("in define func")
    document_id = FieldSchema(name='document_id', dtype=DataType.INT64, is_primary=True, auto_id=True)
    metadata = FieldSchema(name='metadata', dtype=DataType.VARCHAR, max_length=15000)
    metadata_page = FieldSchema(name='metadata_page', dtype=DataType.INT64)    
    embeddings = FieldSchema(name='embeddings', dtype=DataType.FLOAT_VECTOR, dim=384)
    text = FieldSchema(name='text', dtype=DataType.VARCHAR, max_length=60000)
    schema = CollectionSchema(fields=[document_id, metadata, metadata_page, embeddings, text], enable_dynamic_field=True)
    
    if not utility.has_collection(collection_name):
        collection = Collection(name=collection_name, schema=schema, using='default')
        print('Collection created!')
    else:
        collection = Collection(collection_name)
        print('Collection already exists.')
    return collection

@app.route('/search-answers', methods=['POST'])
def search_answers():
    data = request.get_json()
    print(data)
    collection_name = data.get('collection_name', '')
    query = data.get('query', '')
    print(collection_name)
    print(query)
    # Define and load the Milvus collection
    collection = define_collection(collection_name)
    collection.load()
    print("Collection loaded.")

    # Encode the query
    query_encode = model.encode(query.lower())

    # Perform a search to get answers
    search_results = collection.search(data=[query_encode], anns_field="embeddings",
                                      param={"metric": "L2", "offset": 0},
                                      output_fields=["metadata", "metadata_page", "text"],
                                      limit=10, consistency_level="Strong")
    print(search_results)
    # Extract relevant information from search results
    answers_final = [search_results[0][i].entity.text for i in range(1,len(search_results[0]))]

    return jsonify({'answers_final': answers_final}), 200

if __name__ == '__main__':
    app.run(debug=False)
