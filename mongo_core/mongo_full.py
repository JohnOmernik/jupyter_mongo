import pandas as pd
from IPython.core.magic import (magics_class, line_cell_magic)
from mongo_core._version import __desc__
from integration_core import Integration
import jupyter_integrations_utility as jiu
from mongo_utils.mongo_api import MongoAPI
from mongo_utils.user_input_parser import UserInputParser
from mongo_utils.api_response_parser import ResponseParser


@magics_class
class Mongo(Integration):
    # Static Variables
    # The name of the integration
    name_str = "mongo"
    instances = {}
    custom_evars = ["mongo_conn_default", "server_selection_timeout"]

    # These are the variables in the opts dict that allowed to be set by the user.
    # These are specific to this custom integration and are joined
    # with the base_allowed_set_opts from the integration base
    custom_allowed_set_opts = ["mongo_conn_default", "server_selection_timeout"]

    myopts = {}
    myopts["mongo_conn_default"] = ["default", "Default instance to connect with"]
    myopts["server_selection_timeout"] = [5, "Time (in seconds) to wait while attempting to connect to an instance"]
    instvars = ["noAuth", "noPass", "namedpw"]

    # Class Init function - Obtain a reference to the get_ipython()
    def __init__(self, shell, debug=False, *args, **kwargs):
        super(Mongo, self).__init__(shell, debug=debug)
        self.debug = debug

        # Add local variables to opts dict
        for k in self.myopts.keys():
            self.opts[k] = self.myopts[k]

        self.user_input_parser = UserInputParser()
        self.response_parser = ResponseParser()
        self.load_env(self.custom_evars)
        self.parse_instances()

    def retCustomDesc(self):
        return __desc__

    def customHelp(self, curout):
        n = self.name_str
        mn = self.magic_name
        m = "%" + mn
        mq = "%" + m
        table_header = "| Magic | Description |\n"
        table_header += "| -------- | ----- |\n"
        out = curout
        qexamples = []
        qexamples.append(["myinstance", "use mydb", "Use the database mydb. All items with db will refer to that db"])
        qexamples.append(["myinstance", "curdb", "Show the current database that applies to db in your query"])
        qexamples.append(["myinstance", "listdbs", "List the available Databases on the connection"])
        qexamples.append(["myinstance", "show db", "This command is not mongo db, instead it shows the current db that \
            applies to db in jupyter integrations"])
        qexamples.append(["myinstance", "db[\"mycol\"].find({\"_id\":{\"$in\":[\"a\", \"b\", \"c\"]}}", "Fun a find \
            command on the current db for the collection mycol"])
        out += self.retQueryHelp(qexamples)

        return out

    def customAuth(self, instance):
        result = -1
        inst = None
        if instance not in self.instances.keys():
            result = -3
            print("Instance %s not found in instances - Connection Failed" % instance)
        else:
            inst = self.instances[instance]

        if inst is not None:
            inst["session"] = None
            mypass = ""
            if inst["enc_pass"] is not None:
                mypass = self.ret_dec_pass(inst["enc_pass"])
                inst["connect_pass"] = ""

            try:
                inst["session"] = MongoAPI(
                    host=inst["host"],
                    port=inst["port"],
                    username=inst["user"],
                    password=mypass,
                    timeout=self.opts["server_selection_timeout"][0]
                )

                result = 0

            except Exception as e:
                jiu.display_error(e)
                result = -2

        return result

    def customQuery(self, query, instance, reconnect=True):
        dataframe = None
        status = ""

        try:
            parsed_input = self.user_input_parser.parse_input(query, type="cell")

            if self.debug:
                jiu.displayMD(f"**[ Dbg ]** parsed_input\n{parsed_input}")

            response = self.instances[instance]["session"]._handler(**parsed_input["input"])

            parsed_response = self.response_parser._handler(response, **parsed_input["input"])

            dataframe = pd.DataFrame(parsed_response)

        except Exception as e:
            dataframe = None
            status = str(e)

        return dataframe, status

    # This is the magic name.
    @line_cell_magic
    def mongo(self, line, cell=None):

        if cell is None:
            line = line.replace("\r", "")
            line_handled = self.handleLine(line)

            if self.debug:
                print("line: %s" % line)
                print("cell: %s" % cell)

            if not line_handled:  # We based on this we can do custom things for integrations.
                try:
                    parsed_input = self.user_input_parser.parse_input(line, type="line")

                    if self.debug:
                        jiu.displayMD(f"**[ Dbg ]** Parsed Query: `{parsed_input}`")

                    if parsed_input["error"] is True:
                        jiu.display_error(f"{parsed_input['message']}")

                    else:
                        instance = parsed_input["input"]["instance"]

                        if instance not in self.instances.keys():
                            jiu.display_error(f"Instance **{instance}** not found in instances")

                        else:
                            response = self.instances[instance]["session"]._handler(**parsed_input["input"])
                            parsed_response = self.response_parser._handler(response, **parsed_input["input"])
                            jiu.displayMD(parsed_response)

                except Exception as e:
                    jiu.display_error(f"There was an error in your line magic: {e}")

        else:  # This is run is the cell is not none, thus it's a cell to process  - For us, that means a query
            self.handleCell(cell, line)
