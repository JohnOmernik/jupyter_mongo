from argparse import ArgumentParser
import ast
import json
import re
from mongo_utils.mongo_api import MongoAPI


class UserInputParser(ArgumentParser):
    """A class to parse a user's line and cell magics from Jupyter."""

    def __init__(self, *args, **kwargs):
        self.valid_commands = list(filter(lambda func: not func.startswith("_") and hasattr(getattr(MongoAPI, func),
                                                                                            "__call__"), dir(MongoAPI)))
        self.line_parser = ArgumentParser(prog=r"%mongo")
        self.cell_parser = ArgumentParser(prog=r"%%mongo")

        self.line_subparsers = self.line_parser.add_subparsers(dest="command")
        self.cell_subparsers = self.cell_parser.add_subparsers(dest="command")

        # LINE SUBPARSERS #
        # Subparser for "show_dbs"
        self.parser_show_databases = self.line_subparsers.add_parser("show_dbs", help="Show database names \
            in your current connection")
        self.parser_show_databases.add_argument("-i", "--instance", required=True, help="the instance to run \
            the command against")

        # Subparser for "show_collections"
        self.parser_show_collections = self.line_subparsers.add_parser("show_collections", help="Show the collections \
            in a database")
        self.parser_show_collections.add_argument("-i", "--instance", required=True, help="the instance to run the \
            command against")
        self.parser_show_collections.add_argument("-d", "--database", required=True, help="the name of the database")

        # CELL SUBPARSERS #
        # Subparser for "find_one"
        self.parser_find_one = self.cell_subparsers.add_parser("find_one", help="Query the collection for a single \
            document matching a query")
        self.parser_find_one.add_argument("-i", "--instance", required=True, help="the name of the instance")
        self.parser_find_one.add_argument("-d", "--database", required=True, help="the name of the database that \
            contains the collection")
        self.parser_find_one.add_argument("-c", "--collection", required=True, help="the name of the collection")

        # Subparser for "find"
        self.parser_find = self.cell_subparsers.add_parser("find", help="Query the collection")
        self.parser_find.add_argument("-i", "--instance", required=True, help="the instance to run the command against")
        self.parser_find.add_argument("-d", "--database", required=True, help="the name of the database that contains \
            the collection")
        self.parser_find.add_argument("-c", "--collection", required=True, help="the name of the collection to query")

        # Subparser for "count_documents"
        self.parser_count_documents = self.cell_subparsers.add_parser("count_documents", help="Count the number of \
            documents in a collection")
        self.parser_count_documents.add_argument("-i", "--instance", required=True, help="the instance to run the \
            command against")
        self.parser_count_documents.add_argument("-d", "--database", required=True, help="the name of the database")
        self.parser_count_documents.add_argument("-c", "--collection", required=True, help="the name of the collection")

    def display_help(self, command):
        self.parser.parse_args([command], "--help")

    def transform_query(self, query):
        r"""Split a user-supplied Mongo query string into a list
            of JSON objects to be passed to pymongo commands as
            parameterized args.

            Regex pattern explanation: r"\}\s{0,}\,\s{0,}\{"
            -- Positive Lookbehind (?<=\})
                -- \} matches the character } literally (case sensitive)
                -- \s matches any whitespace character (equivalent to [\r\n\t\f\v ])
                -- {0,} matches the previous token between zero and unlimited times, as many times as possible
                -- \, matches the character , literally (case sensitive)
                -- \s matches any whitespace character (equivalent to [\r\n\t\f\v ])
                -- {0,} matches the previous token between zero and unlimited times, as many times as possible
            -- Positive Lookahead (?=\{)
                -- \{ matches the character { literally (case sensitive)

        Args:
            query (str): The user's query string

        Returns:
            split_query (str): a list
        """

        try:
            split_query = re.split(r"(?<=\})\s{0,}\,\s{0,}(?=\{)", query)
            split_query = list(map(lambda q: ast.literal_eval(json.loads(json.dumps(q))), split_query))

            return split_query

        except Exception:
            raise

    def parse_input(self, input, type):
        """Parses the user's line magic from Jupyter

        Args:
            input (str): the entire contents of the line from Jupyter

        Returns:
            parsed_input (dict): an object containing an error status, a message,
                and parsed command from argparse.parse()
        """

        parsed_input = {
            "type": type,
            "error": False,
            "message": None,
            "input": {}
        }

        # Process line magics
        if type == "line":
            try:
                if len(input.strip().split("\n")) > 1:
                    parsed_input["error"] = True
                    parsed_input["message"] = r"The line magic is more than one line and shouldn't be. \
                        Try `%splunk --help` or `%splunk -h` for proper formatting"

                else:
                    parsed_user_command = self.line_parser.parse_args(input.split())
                    parsed_input["input"].update(vars(parsed_user_command))

            except SystemExit:
                parsed_input["error"] = True
                parsed_input["message"] = r"Invalid input received, see the output above. \
                    Try `%mongo --help` or `%mongo -h`"

        # Process cell magics
        if type == "cell":

            # Split the cell magic by newline
            split_user_input = input.strip().split("\n")

            try:
                if len(split_user_input) == 1:
                    parsed_user_command = self.cell_parser.parse_args(split_user_input[0].split())
                    parsed_input["input"].update(vars(parsed_user_command))
                    parsed_input["error"] = True
                    parsed_input["message"] = "Expected to get 2 lines in your cell magic, but got 1. \
                        Did you forget to include a query?\nTry `--help` or `-h`"

                elif len(split_user_input) == 2:
                    parsed_user_command = self.cell_parser.parse_args(split_user_input[0].split())
                    parsed_user_query = split_user_input[1]

                    # Transform the user's query into a list of JSON objects that
                    # can be unpacked args like pymongo expects
                    split_user_query = self.transform_query(parsed_user_query)

                    parsed_input["input"].update(vars(parsed_user_command))
                    parsed_input["input"].update({"query": split_user_query})

                else:
                    parsed_input["error"] = True
                    parsed_input["message"] = f"Expected to get 2 lines in your cell magic, \
                        but got {len(split_user_input)}. Try `--help` or `-h`"

            except SystemExit:
                parsed_input["error"] = True
                parsed_input["message"] = r"Invalid input received, see the output above. \
                    Try `%%mongo --help` or `%%mongo -h`"

            except Exception as e:
                parsed_input["error"] = True
                parsed_input["message"] = f"Exception while parsing user input: {e}"

        return parsed_input
