import os
from dotenv import load_dotenv
load_dotenv()
import dropbox

# Connect to dropbox account
dbx = dropbox.Dropbox(
  app_key = os.getenv('APP_KEY'), 
  app_secret=os.getenv('APP_SECRET'), 
  oauth2_refresh_token=os.getenv('DROPBOX_REFRESH_TOKEN'))