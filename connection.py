from pymongo import MongoClient

DATABASE_NAME = 'tennis_db'
HOST = 'localhost'
PORT = '27017'

global db
db = MongoClient

def open_connection():
   URI = f"mongodb://{HOST}:{PORT}"
   
   global client
   client = MongoClient(URI)
   
   global db
   db = client[DATABASE_NAME]
   
def get_database():
   return db

def get_collection(collection_name):
   return db[str(collection_name)]
                   
def add_document(collection_name, document):
   return get_collection(collection_name).insert_one(document)
   
def add_many_documents(collection_name, documents):
   return get_collection(collection_name).insert_many(documents)

def delete_document(collection_name ,documents):
   return get_collection(collection_name).delete_one(documents)
                
def close_connection():
   client.close()
  


