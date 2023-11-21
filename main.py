import logging
import weaviate
from llama_index.vector_stores import WeaviateVectorStore
from llama_index.vector_stores import WeaviateVectorStore
from langchain.llms.bedrock import Bedrock
from langchain.embeddings import BedrockEmbeddings
from llama_index.embeddings import LangchainEmbedding
from llama_index import VectorStoreIndex, StorageContext
from llama_index import SimpleDirectoryReader
from llama_index.node_parser import SimpleNodeParser
from llama_index import ServiceContext, set_global_service_context

import boto3

bedrock_runtime  = boto3.client(
        service_name="bedrock-runtime",
        region_name="us-east-1",
        )
llm_model:str="ai21.j2-ultra-v1"
embedding_model:str="amazon.titan-embed-text-v1"

def getLLM():
        try:
            logging.info("Starting Bedrock")
            return Bedrock(
                client=bedrock_runtime ,
                model_id = llm_model
            )
        except Exception as e:
            raise Exception(e)
        
def getEmbeddings():
        try:
            logging.info("Starting Bedrock Embedding")
            BR =  BedrockEmbeddings(
                client=bedrock_runtime ,
                model_id=embedding_model
            )
            return LangchainEmbedding(BR)
        except Exception as e:
            raise Exception(e)

def setContext():
    try:
        logging.info("Setting up Context")
        llm  = getLLM()
        embed = getEmbeddings()
        service = ServiceContext.from_defaults(
                llm=llm ,
                embed_model=embed
            )
        set_global_service_context(service_context=service)
        return service   
    except Exception as e:
        raise Exception(e)
        

def setStore():
    try:
            print("Setting Store")
            logging.info("Starting Weavaite store")
            client = weaviate.Client(url="http://localhost:8080")
            store = WeaviateVectorStore(client , index_name="BlogPost", text_key="content" )
            print("Store setup complete")
            return store
    except Exception as e:
            logging.error(f"Failed to start vector store {e}")
            raise Exception(e)   

def main():
    try:
      logging.info("Starting Scrript")
      setContext()
      store  = setStore()
      docs = SimpleDirectoryReader(input_dir="./out").load_data()
      parser =  SimpleNodeParser("TextSplitter")
      nodes = parser.get_nodes_from_documents(docs)
      storage_contect = StorageContext.from_defaults(vector_store=store)
      index = VectorStoreIndex(nodes=nodes , storage_context=storage_contect)
      engine = index.as_query_engine()
      exit = False
      while(not exit):
        print("press q to exit")
        print("Enter query")
        query = str(input())
        if("q" in query or "Q" in query):
            exit = True
            return
        res = engine.query(query)
        print(f"\n {res} \n")
        logging.info("End of Script")
    except Exception as e:
        logging.error("Messed up g " +str(e))
        print("Messed up g " +str(e))


if __name__ == "__main__":
    main()