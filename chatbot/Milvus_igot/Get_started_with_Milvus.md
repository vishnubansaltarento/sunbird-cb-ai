
Check the below link to work with Milvus collection that stores PDF embeddings and use LLM's to query against the PDF documents 
- [Milvus documentation](https://milvus.io/)
- Implementation of Milvus collection: [Milvus playbook](https://git.idc.tarento.com/aiml/intentless_chatbot/-/tree/working_with_milvus?ref_type=heads) 

How to get started with Milvus
refer this link and follow given steps https://milvus.io/blog/how-to-get-started-with-milvus.md

For windows-

-Install WSL
-install docker
-add docker-compose (.yaml) file 
-open wsl and run following commands 

cd
cd milvus_compose/ 	 		####Navigate to directory having docker-compose.yaml file
sudo docker-compose up -d
sudo docker ps -a                       ####Check if  milvus-standalone, milvus-minio, milvus-etcd are having status "healthy"

-Now from your code make connection to milvus using ""connections.connect(host=MILVUS_HOST, port=MILVUS_PORT)""