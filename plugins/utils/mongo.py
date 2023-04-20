import pymongo

def get_mongo_db(institute:str):
    password = "p7Zj5AJGqrEEzd7v"
    client = pymongo.MongoClient(f"mongodb+srv://academic_admin:{password}@dbacademic-cluster.enzu3ui.mongodb.net/?retryWrites=true&w=majority")
    db = client.get_database(f"{institute}_dbacademic")
    return db

def insert_many(database, collection_name: str, data: list) -> dict:
    collection = database.get_collection(collection_name)
    try:
        collection.insert_many(data)
    except Exception as e:
        print(e)
        return {
            "status"  : "Error inserting documents.",
            "inserted": collection.count_documents({})
        }
    return {
        "status"  : "Success inserting documents.",
        "inserted": collection.count_documents({})
    }

def drop_collection(database, collection_name: str) -> dict:
    collection = database.get_collection(collection_name)
    try:
        collection.drop()
    except Exception as e:
        print(e)
        return {
            "status": "Error droping collection.",
            "count" : collection.count_documents({})
        }
    return {
        "status": "Success droping collection.",
        "count" : collection.count_documents({})
    }