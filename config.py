from dotenv import load_dotenv
import os

load_dotenv()
PATH_LOG_SAVED = os.getenv('PATH_LOG_SAVED')
PATH_ANALIZELOG_SAVED = os.getenv('PATH_ANALIZELOG_SAVED')
PATH_BATCH_LOG = os.getenv('PATH_BATCH_LOG')
PATH_PROJECT = os.getenv('PATH_PROJECT')

