import os
import json
from controllers.helpers.dropbox import dbx
from dropbox.exceptions import ApiError

# Initialize elasticsearch
from elasticsearch import Elasticsearch
es = Elasticsearch(
	cloud_id=os.getenv('ELASTICSEARCH_CLOUD_ID'),
  basic_auth=(os.getenv('ELASTICSEARCH_USERNAME'), os.getenv('ELASTICSEARCH_PASSWORD')),
)



if not es.indices.exists(index="dropbox_files"):
	es.indices.create(index="dropbox_files")

# Initialize tika for file parsing
import tika
tika.initVM()
from tika import parser

# env vars
from dotenv import load_dotenv
load_dotenv()

# Initialize Flask app
from flask import Flask, jsonify, request
from flask_cors import CORS
app = Flask(__name__)
CORS(app)


# Retrieve already Stored Data from elastic search index
def get_old_data():
	db = {}
	response = es.search(index="dropbox_files", body={"query": {"match_all": {}}})

	for x in response['hits']['hits']:
		db[x['_id']] = x['_source']
	return db

# Get a list of metadata of all files in dropbox account
def get_new_metadata():
	new_metadata = {}
	result = dbx.files_list_folder(path="", recursive=True)
	for file in result.entries:
		if hasattr(file, 'content_hash'):
			# Key for elasticsearch index is the <dropbox_file_id;content_hash>
			new_metadata[file.id+";"+file.content_hash] = file.path_display

	return new_metadata

# Delete the files deleted from dropbox account, from the local db copy
def delete_old_data(db, to_delete):
	for key in to_delete:
		del db[key]

# Download newly added/updated files from dropbox account & save it to local db copy
def save_new_files(db, to_download, new_metadata):
	for key in to_download:
		metadata, result = dbx.files_download(path=new_metadata[key])
		file_contents = result.content
		parsed = parser.from_buffer(file_contents)
		db[key] = [parsed['content'], metadata.path_display]
  
# Update es index using local db copy
def update_search_index(db, to_download, to_delete):
	bulk_requests = []
	for file_id in to_download:
		doc = {
			'content': db[file_id][0],
			'path': db[file_id][1],
		}
		action= {"index": {"_index": "dropbox_files", "_id": file_id}}
		bulk_requests.append(action)
		bulk_requests.append(doc)

	for file_id in to_delete:
		bulk_requests.append({"delete": {"_index": "dropbox_files", "_id": file_id}})

	if len(bulk_requests):
		es.bulk(operations=bulk_requests)

# Fetch shareable links for all files & return the final result 
def get_search_result(response):
	result_paths = []
	for x in response['hits']['hits']:
		result_paths.append(x['_source']['path'])

	final_response = []

	# Fetch metadata containing links for each file from dropbox
	for path in result_paths:

		result = dbx.sharing_list_shared_links(path=path, direct_only=True)
		if len(result.links):
			temp = result.links[0]
			final_response.append({"link": temp.url, "name": temp.name, "path": temp.path_lower})
	
	return final_response


@app.route("/search", methods=['GET'])
def search():

		try:
			q = request.args.get('q')
			if not q:
				return jsonify({"error": "no query"}), 400

			# Get old data from elastic search index & store it in local copy
			db = get_old_data();

			# Get new metadata of all files from dropbox account
			new_metadata = get_new_metadata()

			# Get a list of keys in stored index and new_metadata from dropbox
			old_keys = set(list(db.keys()))
			new_keys = set(list(new_metadata.keys()))

			to_download = new_keys - old_keys
			to_delete = old_keys - new_keys

			# Delete keys for files that have been deleted from dropbox, from the local copy
			delete_old_data(db, to_delete)

			# Download newly added files on dropbox & save it to local copy
			save_new_files(db, to_download, new_metadata)
			
			# Update elastic search index using updated local copy
			update_search_index(db, to_download, to_delete)


			# Search files in elastic search index for given query q
			es.indices.refresh(index="dropbox_files")
			response = es.search(index="dropbox_files", body={"query": {"match_phrase": {"content": q}}})

			# Fetch shareable links with given file paths
			final_response = get_search_result(response)

		except:
			return jsonify({"error": "internal server error"}), 500

    # Output/send response to client
		return jsonify(final_response), 200