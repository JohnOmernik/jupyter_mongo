#!/usr/bin/python

# Base imports for all integrations, only remove these at your own risk!
import json
import sys
import os
import time
import pandas as pd
from collections import OrderedDict
import re
from integration_core import Integration
import datetime
from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic, line_cell_magic)
from IPython.core.display import HTML
from mongo_core._version import __desc__
import urllib.parse


# Your Specific integration imports go here, make sure they are in requirements!
import pymongo
import jupyter_integrations_utility as jiu
#import IPython.display
from IPython.display import display_html, display, Javascript, FileLink, FileLinks, Image
import ipywidgets as widgets

@magics_class
class Mongo(Integration):
    # Static Variables
    # The name of the integration
    name_str = "mongo"
    instances = {} 
    custom_evars = ['mongo_conn_default']
    # These are the variables in the opts dict that allowed to be set by the user. These are specific to this custom integration and are joined
    # with the base_allowed_set_opts from the integration base

    # These are the variables in the opts dict that allowed to be set by the user. These are specific to this custom integration and are joined
    # with the base_allowed_set_opts from the integration base
    custom_allowed_set_opts = ["mongo_conn_default"]


    myopts = {}
    myopts['mongo_conn_default'] = ["default", "Default instance to connect with"]


    instvars = ['noAuth', 'noPass', 'namedpw']


    # Class Init function - Obtain a reference to the get_ipython()
    def __init__(self, shell, debug=False, *args, **kwargs):
        super(Mongo, self).__init__(shell, debug=debug)
        self.debug = debug

        #Add local variables to opts dict
        for k in self.myopts.keys():
            self.opts[k] = self.myopts[k]

        self.load_env(self.custom_evars)
        self.parse_instances()


    def retCustomDesc(self):
        return __desc__



# Handle no Auth situations for Testing
    def req_password(self, instance):

        noAuth = self.instances[instance]['options'].get('noAuth', 0)
        noPass = self.instances[instance]['options'].get('noPass', 0)
        if int(noAuth) == 1 or int(noPass) == 1:
            bPass = False
        else:
            bPass = True
        return bPass

    def req_username(self, instance):
        noAuth = self.instances[instance]['options'].get('noAuth', 0)
        if int(noAuth) == 1:
            bUser = False
        else:
            bUser = True
        return bUser

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
        qexamples.append(["myinstance", "show db", "This command is not mongo db, instead it shows the current db that applies to db in jupyter integrations"])
        qexamples.append(["myinstance", "db['mycol'].find({'_id':{'$in':['a', 'b', 'c']}}", "Fun a find command on the current db for the collection mycol"])
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
            inst['session'] = None
            mypass = ""
            if self.req_password(instance) and inst['enc_pass'] is not None:
                mypass = self.ret_dec_pass(inst['enc_pass'])
                inst['connect_pass'] = ""

            if self.req_username(instance):
                myuser = urllib.parse.quote_plus(inst['user'])
                if self.req_password(instance):
                    urlpass = urllib.parse.quote_plus(mypass)
                    upstr = f"{myuser}:{urlpass}"
                else:
                    upstr = f"{myuser}"
                upstr += "@"
            else:
                upstr = ""
            con_url = f"{inst['scheme']}://{upstr}{inst['host']}"
            try:
                # Get Client into session
                inst['session'] = pymongo.MongoClient(con_url)
                # Get the DB list and push into dictionary 
                inst['db_list'] = {}
                all_dbs = [x['name'] for x in list(inst['session'].list_databases())]
                for db in all_dbs:
                    inst['db_list'][db] = []
                # Determine if there is a default db provided or use "local"
                inst['def_db'] = inst['options'].get('def_db', 'local')
                # If the Default DB is in the Database list, get the collection list and set the cur_db to be the Database Object 
                if inst['def_db'] in inst['db_list']:
                    inst['use_cmd'] = f"use {inst['def_db']}"
                    inst['cur_db'] = inst['session'][inst['def_db']]
                    inst['db_list'][inst['def_db']] = inst['cur_db'].list_collection_names()
                else:
                    print(f"Warning: {inst['def_db']} not in db_list - Not setting db")
                    inst['use_cmd'] = ""
                    inst['cur_db'] = None
                result = 0
            except Exception as e:
                if str(e).find("'codeName': 'Unauthorized'") >= 0:
                    print("Error with authentication")
                    print(str(e))
                    result = -2
                else:
                    print(f"Error Connecing: {str(e)}")
                    result = -3

        return result

    def parse_mongo_query(self, q):


        debug = self.debug

        q = q.strip()
        client_str = "c"
        db_str = "db"
        dot_split = q.strip().split(".")

        db_provided = None
        col_provided = None

        q_builder  = {}

# Must have a Paren
        if q.find("(") < 0:
            print("Query must have a parentheses in it to denote what you are querying: Example db['mycol'].find({})")
            return None

        p_split = q.split("(")
        q_str = p_split[1][0:-1] # Remove the closing paren
        try: 
            q_dict = eval(q_str)
        except:
            print(f"Could not evaluate Query: {q_str}")
            return None
        q_builder['q_str'] = q_str
        q_builder['q_dict'] = q_dict

        full_str = p_split[0]
        full_split = full_str.split(".")
        if debug:
            print(f"full_str: {full_str}")
            print(f"full_split: {full_split}")
        f_str = full_split[-1].lower().strip()
# The command (the last item in the str) must be find or find_one
        if f_str not in ['find', 'find_one']:
            print("Command for Magic use must be find or find one see %%mongo help for examples")
            return None

        q_builder['q_type'] = f_str
        if debug:
            print(f"p_split[0]: {p_split[0]}")
# Ok, remove .find or .find_one now we have client, db, collection stuff
        col_str = p_split[0].strip().replace(".find_one", "").replace(".find", "")
# Split by .
        col_split = col_str.split(".")

# If there are no dots (len of 1)
# it could be
# c['db']['col']
# db['col']
        if debug:
            print(f"Col Str: {col_str}")
            print(f"Col split: {col_split}")
            print(f"Len Col Split: {len(col_split)}")
        if len(col_split) == 1:
            if col_split[0].find(f"{client_str}") == 0:
                sq_split = col_split[0].split("][")
                if debug:
                    print(f"sq split: {sq_split}")
                if len(sq_split) == 2:
                    db_provided = sq_split[0].replace("c[", "").replace("'", "").replace('"', '')
                    col_provided = sq_split[1].replace("]", "").replace("'", "").replace('"', '')
            elif col_split[0].find(f"{db_str}") == 0:
                if col_split[0].strip().find("[") >= 0:
                    col_provided = col_split[0].strip().replace("db[", "").replace("]", "").replace("'", "").replace('"', '')

# IF there are two items, it could
# c.db (No db or col provided)
# c.db['col']
# c['db'].col
# db.col
        elif len(col_split) == 2:
            if col_split[0].strip() == client_str and col_split[1].strip() == db_str:
                pass
            elif col_split[0].strip() == client_str and col_split[1].strip() != db_str:
                if col_split[1].find(f"{db_str}") == 0:
                    if col_split[1].find("[") >= 0:
                        col_provided = col_split[1].replace("db[", "").replace("]", "").replace("'", "").replace('"', '')
            elif col_split[0].find(f"{client_str}") == 0:
                if col_split[0].find("[") >= 0:
                    db_provided = col_split[0].replace("c[", "").replace("]", "").replace("'", "").replace('"', '')
                    col_provided = col_split[1].strip()
            elif col_split[0].strip() == db_str:
                col_provided = col_split[1].strip()

# If there are 3 items
# c.db.col
# c.db1.col1
        elif len(col_split) == 3:
            if col_split[0].strip() == client_str and col_split[1].strip() == db_str:
                col_provided = col_split[2].strip()
            if col_split[0].strip() == client_str and col_split[1].strip() != db_str:
                db_provided = col_split[1].strip()
                col_provided = col_split[2].strip()

        else:
            pass

        q_builder['db_provided'] = db_provided
        q_builder['col_provided'] = col_provided
        return q_builder



    def validateQuery(self, query, instance):
        bRun = True
        bReRun = False


        query = query.strip()

        space_split = query.split(" ")
        if space_split[0] in ['use', 'curdb', 'listdbs']:
            return True

        q_dict = self.parse_mongo_query(query)
        if self.instances[instance]['last_query'] == query:
            # If the validation allows rerun, that we are here:
            bReRun = True

        if q_dict is None:
            print(f"Query did not parse as a mongo find or find_one query - Not running")
            bRun = False

        if q_dict['col_provided'] is None:
            print("Query parsed, but we could not identify a collection to query in your query. Please see %%mongo for help")
            bRun = False

        return bRun

    def customQuery(self, query, instance, reconnect=True):

        mydf = None
        status = ""

        inst = self.instances[instance]

        if self.debug:
            print(f"Instance: {instance}")
            print(f"Query: {query}")

        if query.strip().find("use") == 0:
            mydb = query.replace("use", "").strip()
            if mydb in inst['db_list']:
                inst['use_cmd'] = query.strip()
                inst['cur_db'] = inst['session'][mydb]
                inst['db_list'][mydb] = inst['cur_db'].list_collection_names()
                print(f"Changed current db (db) to {mydb}")
                status = "Success - No Results"
            else:
                print(f"{mydb} is not in current db list")
                print(f"To see current db:\n")
                print(f"%%mongo {instance}\ncurdb\n\n")
                print(f"To see list of dbs:\n")
                print(f"%%mongo {instance}\nlistdbs\n")
                status = "Failure - DB Does not Exist"
        elif query.strip() == "curdb":
            print(f"Current db is {inst['use_cmd'].replace('use', '').strip()}\n")
            status = "Success - No Results"
        elif query.strip() == "listdbs":
            print("List of DBs available on connection:")
            for x in inst['db_list']:
                print(x)
            print("\n")
            status = "Success - No Results"
        else: # Let's process this query
            q_dict = self.parse_mongo_query(query)

            try:
                if q_dict['q_type'] == 'find':
                    if q_dict['db_provided'] is None:
                        res_list = list(inst['cur_db'][q_dict['col_provided']].find(q_dict['q_dict']))
                    else:
                        res_list = list(inst['session'][q_dict['db_provided']][q_dict['col_provided']].find(q_dict['q_dict']))
                elif q_dict['q_type'] == 'find_one':
                    if q_dict['db_provided'] is None:
                        res_list = [inst['cur_db'][q_dict['col_provided']].find_one(q_dict['q_dict'])]
                    else:
                        res_list = [inst['session'][q_dict['db_provided']][q_dict['col_provided']].find_one(q_dict['q_dict'])]
                else:
                    print("I have no idea how you got here. You get a gold star")
            except Exception as e:
                status = f"Failure - Mongo DB Error: {str(e)}"
                mydf = None

            if status == "":
                try:
                    if len(res_list) > 0:
                        mydf = pd.DataFrame(res_list)
                        status = "Success"
                    else:
                        mydf = None
                        status = "Success - No Results"
                except Exception as e:
                    status = f"Failure - Mongo Parse Results Error: {str(e)}"
                    mydf = None

    
        return mydf, status






##########################


    # This is the magic name.
    @line_cell_magic
    def mongo(self, line, cell=None):
        if cell is None:
            line = line.replace("\r", "")
            line_handled = self.handleLine(line)
            if self.debug:
                print("line: %s" % line)
                print("cell: %s" % cell)
            if not line_handled: # We based on this we can do custom things for integrations. 
                if line.lower() == "testintwin":
                    print("You've found the custom testint winning line magic!")
                else:
                    print("I am sorry, I don't know what you want to do with your line magic, try just %" + self.name_str + " for help options")
        else: # This is run is the cell is not none, thus it's a cell to process  - For us, that means a query
            self.handleCell(cell, line)

