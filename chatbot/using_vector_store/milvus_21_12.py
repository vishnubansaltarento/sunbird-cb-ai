from pymilvus import connections
import os 

from langchain.text_splitter import MarkdownHeaderTextSplitter, RecursiveCharacterTextSplitter, CharacterTextSplitter
from langchain.vectorstores import Milvus

from dotenv import load_dotenv

from pymilvus import CollectionSchema, FieldSchema, DataType, connections, utility, Collection
from sentence_transformers import SentenceTransformer
import re
import PyPDF2


MILVUS_HOST = "localhost"
MILVUS_PORT = "19530"

connections.connect(host=MILVUS_HOST, port=MILVUS_PORT)
print('Connected to Milvus!')

model= SentenceTransformer('paraphrase-MiniLM-L3-v2')

MILVUS_HOST = "localhost"
MILVUS_PORT = "19530"

connections.connect(host=MILVUS_HOST, port=MILVUS_PORT)
print('Connected to Milvus!')




document_id = FieldSchema(name='document_id', dtype=DataType.INT64, is_primary=True, auto_id=True)
metadata = FieldSchema(name='metadata', dtype=DataType.VARCHAR, max_length=50000)
metadata_page = FieldSchema(name='metadata_page', dtype=DataType.INT64)
embeddings = FieldSchema(name='embeddings', dtype=DataType.FLOAT_VECTOR, dim=384)
text = FieldSchema(name='text', dtype=DataType.VARCHAR, max_length=65535)
schema = CollectionSchema(fields=[document_id, metadata, metadata_page, embeddings, text], enable_dynamic_field=True)

class SimpleDocument:
    def __init__(self, page_content, metadata=None):
        self.page_content = page_content
        self.metadata = {} if metadata is None else metadata


collection_name = 'a12'
if not utility.has_collection(collection_name):
    collection = Collection(name=collection_name, schema=schema, using='default')
    print('Collection created!')
else:
    collection = Collection(collection_name)
    print('Collection already exists.')



metadata_list = []
metadata_page_list = []
embedding_list = []
text_list = []

folder_path = r"C:\Users\Palash Ashok Bhosale\Jupy\Projects\Bot_NLP\pdff"
for file_name in os.listdir(folder_path):
    if file_name.endswith(".pdf"):
        file_path = os.path.join(folder_path, file_name)
        with open(file_path, 'rb') as pdf_file:
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            count=1
            for page in range(0,len(pdf_reader.pages)):
                text = pdf_reader.pages[page].extract_text()
                try:
                    document_splitter = RecursiveCharacterTextSplitter(chunk_size=512, chunk_overlap=128, length_function=len)
                    document_chunks = document_splitter.split_documents([SimpleDocument(text)])
                    for i, document_chunk in enumerate(document_chunks):
                            
                        document_chunk_text = re.sub(" \n", " ", document_chunk.page_content)
                        paragraph=document_chunk_text
                        document_chunk_text = re.sub(r'\.{2,}', '', document_chunk_text)
                        document_chunk_text=re.sub("iGOT 20  An\tInitiative\tfor\tCapacity\tDevelopment\tof\tCivil\tServices\t|\tConsultation\tPaper\ton\tApproach\tto\tStrategy\tand\tImplementation  | 41 With the implementation of iGOT 20 already underway, there are some key choices which have been made\nDigital\tplatform", "", document_chunk_text)
                        paragraph=document_chunk_text
                        if len(paragraph)>=30:
                            text = paragraph
                            metadata = f"{file_name}_{count}_{i}"
                            metadata_page = i
                            text_list.append(text)
                            embeddings = model.encode(text)
                            embedding_list.append(embeddings)
                            metadata_list.append(metadata)
                            metadata_page_list.append(metadata_page)
                except:
                    print("in except block")
                    print(file_name)
            count+=1

collection.insert([metadata_list, metadata_page_list, embedding_list, text_list])
print('Data inserted into the collection.')
index_params = {
    'metric_type': 'L2',
    'index_type': "HNSW",
    'efConstruction' : 40, 
    'M' : 20
}
collection.create_index(field_name="embeddings", index_params=index_params)
print('Index created.')
collection.load()
print('Collection loaded.')
query = "What is learn hub?"
query_encode = model.encode(query)
documents = collection.search(data=[query_encode], anns_field="embeddings", param={"metric": "L2", "offset": 3},
                             output_fields=["metadata", "metadata_page", "text"], limit=3, consistency_level="Strong")
print("Similar documents:")

for doc in documents:
    num=0
    for i in doc:
        print(i.text)