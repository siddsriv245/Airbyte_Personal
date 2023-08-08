import logging
import os,sys
from datetime import datetime
from functools import wraps
import boto3
import smart_open



def log_file_creation():
    #   session = boto3.session.Session(profile_name="ann04-sandbox-poweruser")
      session = boto3.session.Session()
      s3 = session.resource('s3')
      report=f'{"logfile_"}'+f'{datetime.now().strftime("%Y-%m-%d")}'+f'{".txt"}'
      with smart_open.open (os.path.abspath("exc_logger.log")) as f:
        data=f.read()
        s3.meta.client.put_object(Body=data, Bucket="sidpractice245024", Key='airbyte/logs/'+report)
def create_logger():
    # create a logger object
    logger = logging.getLogger('exc_logger')
    logger.setLevel(logging.INFO)
    logfile_path = 'exc_logger.log'
    if os.path.isfile(logfile_path):
        logfile_mtime = datetime.fromtimestamp(os.path.getmtime(logfile_path)).date()  # Corrected line
    else:
       logfile_mtime = None

    if logfile_mtime is not None and logfile_mtime != datetime.now().strftime("%Y-%m-%d"):
        mode = 'w'  # overwrite the file
    else:
        mode = 'a'  # append to the file


    # create a file to store all the
    # logged exceptions
    logfile = logging.FileHandler(logfile_path,mode=mode)
    fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)

    logfile.setFormatter(formatter)
    logger.addHandler(logfile)
    return logger


logger = create_logger()

# you will find a log file
# created in a given path
print(logger)


def exception(logger):
    # logger is the logging object
    # exception is the decorator objects
    # that logs every exception into log file
    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):

            try:
                return func(*args, **kwargs)

            except Exception as err :
                issue = "exception in " + func.__name__ + "\n"
                issue = issue + "-------------------------\
				------------------------------------------------\n"
                logger.exception(issue)
                logger.info(f"An error occurred: {err}")
                # logs exception in a given file-

                log_file_creation()

                raise err


        return wrapper

    return decorator
