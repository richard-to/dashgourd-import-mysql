import os.path
import os
import subprocess
from dashgourd.importer.mysql import MysqlImporter

def import_users(query_name, query):
    """Boilerplate for loading user import scripts
    
    This uses os environment variables for the mysql and mongodb 
    uris.
    
    Main reason for this is to avoid copying and pasting this boilerplate 
    code in dozens of query files. Granted you could just have them all 
    in a single file. That may actually be simpler.
    
    Args:
        query_name: Name of query. Needed for import api to track last_update
        query: Query to execute with format tokens {start} and {end}
    """
    
    importer = MysqlImporter(os.environ.get('MYSQL_URI'), 
        os.environ.get('MONGO_URI'), os.environ.get('MONGO_DB'))
    importer.import_users(query_name, query)
    importer.close()
    
def import_actions(action_name, query, query_name=None):
    """Boilerplate for loading actions from import scripts
    
    See import_users method for more info.
    """
    
    if query_name is None:
        query_name = "{}_{}".format('action', action_name)
        
    importer = MysqlImporter(os.environ.get('MYSQL_URI'), 
        os.environ.get('MONGO_URI'), os.environ.get('MONGO_DB'))
    importer.import_actions(action_name, query_name, query)
    importer.close()
    
def import_abtests(abtest, query, query_name=None):
    """Boilerplate for loading abtests from import scripts
    
    See import_users method for more info. 
    """

    if query_name is None:
        query_name = "{}_{}".format('ab', abtest)
            
    importer = MysqlImporter(os.environ.get('MYSQL_URI'), 
        os.environ.get('MONGO_URI'), os.environ.get('MONGO_DB'))
    importer.import_abtests(abtest, query_name, query)
    importer.close()
    
def run_import_scripts(location, args=[], whitelist=None):
    """Runs import scripts at specified folder location.
    
    This is a helper classs in the case that you split import 
    scripts into multiple files. And don't want to manually run 
    each script.
    
    This is helpful if you are evaluating how to import data and 
    what format the data should be in.
    
    Additionally this could be used to update the system in batch 
    instead of sending events as they come.
    
    This assumes the scripts are self contained and will send the 
    appropriate data to DashGourd.
    
    Args:
        location: Can be a single file or a directory. Not recursive.
        args: An optional list of arguments to pass into the script
        whitelist: A list of files to run in the directory.
    """

    cmd_args = [0]
    cmd_args.extend(args)

    if os.path.isfile(location):
        path, filename = os.path.split(location)
        label, ext = os.path.splitext(filename)
        if whitelist is None or label in whitelist:
            cmd_args[0] = location 
            subprocess.call(cmd_args)          
    else:
        for dirname, dirnames, filenames in os.walk(location):

            for filename in filenames:
                script = os.path.join(dirname, filename)
                label, ext = os.path.splitext(filename)
                if whitelist is None or label in whitelist:
                    cmd_args[0] = script 
                    subprocess.call(cmd_args)