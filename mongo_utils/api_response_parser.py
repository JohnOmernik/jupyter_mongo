class ResponseParser:
    """ A class to parse API responses from the Flashpoint API.
        Note: I follow this simple idiom: APIs return data. Formatting
        and parsing for use in the application happen separately (here)
    """
    def __init__(self):
        pass
    
    def _handler(self, response, **kwargs):
        """Brokers response parsing on behalf of the calling function

        Args:
            response (int, str, list, or dict): the response from our API
                call in its original form, hence the varying types

        Returns:
            Whatever is passed back to it from one of the functions below.
        """
        issued_command = kwargs.get("command")
        return getattr(self,issued_command)(response, **kwargs)
    
    def show_dbs(self, response, **kwargs):
        """Parse the "show_dbs" response from the Jupyter Mongo API

        Args:
            response (list): list of databases from the Jupyter Mongo API

        Returns:
            formatted_db_list (str): Markdown formatted string of dbs
        """
        
        instance = kwargs.get("instance")
        
        formatted_db_names = "".join(f"* {db}\n" for db in response)

        formatted_db_list = (f"#### Databases in `{instance}`\n"
               "***\n"
               f"{formatted_db_names}\n")
        
        return formatted_db_list
    
    def show_collections(self, response, **kwargs):
        """Parse the "show_collections" response from the Jupyter Mongo API

        Args:
            response (list): list of collections from the Jupyter Mongo API

        Returns:
            collections (str): Markdown formatted string of collections
        """
        
        instance = kwargs.get("instance")
        db_name = kwargs.get("database")
        
        formatted_collections_names = "".join(f"* {col}\n" for col in response)
        
        collections = (f"#### Collections in `{db_name}` in `{instance}` instance\n"
                       "***\n"
                       f"{formatted_collections_names}\n")
        
        return collections
    
    def find_one(self, response, **kwargs):
        """Parse the "find_one" response from the Jupyter Mongo API
            Note: Mongo returns a single dictionary, so we're transforming
            that into a list so we can easily turn it into a dataframe

        Args:
            response (dict): a single dictionary item from Mongo

        Returns:
            (list): the original response, now encapsulated in a list
        """
        return [response]
    
    def find(self, response, **kwargs):
        """Parse the "find" response from the Jupyter Mongo API

        Args:
            response (list): a list of dictionaries from Mongo

        Returns:
            (list): at the moment, this simply passes the response back through
        """

        return response

    def count_documents(self, response, **kwargs):
        """Parse the "count_documents" response from the Jupyter Mongo API

        Args:
            response (int): the total number of documents from a collection

        Returns:
            formatted_response (list): a list with a single dictionary item 
                so we can easily turn it into a dataframe
        """

        formatted_response = [{"count" : response}]

        return formatted_response