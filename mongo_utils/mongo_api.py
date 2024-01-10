import pymongo

class MongoAPI:
    """A class to perform pymongo calls to a Mongo instance."""
    
    def __init__(self, host, port, username, password, timeout):
        self.session = pymongo.MongoClient(
            host = f"{host}:{port}",
            username = username,
            password = password,
            serverSelectionTimeoutMS = timeout
        )

    def _handler(self, command, **kwargs):
        """Broker Mongo commands"""
        return getattr(self, command)(**kwargs)
    
    def show_dbs(self, **kwargs):
        """Return a list of databases in the current MongoClient session

        Returns:
            dbs (list): a list of database names
        """
        
        dbs = self.session.list_database_names()
        
        return dbs
    
    def show_collections(self, **kwargs):
        """Return a list of collections in a database for the current MongoClient session

        Returns:
            collections (list): a list of collection names
        """
        
        db_name = kwargs.get("database")
        
        collections = self.session[db_name].list_collection_names()
        
        return collections
    
    def find_one(self, **kwargs):
        """Get a single document from the database.

        Returns:
            results (dict): A single document, or None if no matching document is found.
        """
        
        db_name = kwargs.get("database")
        collection = kwargs.get("collection")
        query = kwargs.get("query")
                
        results = self.session[db_name][collection].find_one(*query)

        return results
    
    def find(self, **kwargs):
        """_summary_

        Returns:
            results (list): Returns a listified instance of Cursor corresponding to this query.
        """
        db_name = kwargs.get("database")
        collection = kwargs.get("collection")
        query = kwargs.get("query")
        
        results = self.session[db_name][collection].find(*query)
        
        return list(results)

    def count_documents(self, **kwargs):
        """Count the number of documents in a collection.

        Returns:
            results (int): The number of documents in the collection
        """
        db_name = kwargs.get("database")
        collection = kwargs.get("collection")
        query = kwargs.get("query")
        
        results = self.session[db_name][collection].count_documents(*query)
        
        return results