import os
import argparse
import glob
import html
import io
import re
import time
from azure.core.credentials import AzureKeyCredential
from azure.storage.blob import BlobServiceClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import *
from azure.search.documents import SearchClient

from dotenv import load_dotenv,dotenv_values


load_dotenv()
values_env_openai = dotenv_values(".env")
FILE_PATH = "docs_text_new"

search_creds = AzureKeyCredential(values_env_openai['searchkey'])
storage_creds = values_env_openai['storagekey']

MAX_SECTION_LENGTH = 1000
SENTENCE_SEARCH_LIMIT = 100
SECTION_OVERLAP = 100

def blob_name_from_file(filename):
    return os.path.basename(filename)

def upload_blobs(filename):
    blob_service = BlobServiceClient(account_url=f"https://{values_env_openai['storageaccount']}.blob.core.windows.net", credential=storage_creds)
    blob_container = blob_service.get_container_client('chat')
    
    if not blob_container.exists():
        blob_container.create_container()

    blob_name = blob_name_from_file(filename)
    with open(filename, "rb") as data:
        blob_container.upload_blob(blob_name, data, overwrite=True)

def get_document_text(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        text = file.read()
    return text

def split_text(text):
    SENTENCE_ENDINGS = [".", "!", "?"]
    WORDS_BREAKS = [",", ";", ":", " ", "(", ")", "[", "]", "{", "}", "\t", "\n"]
    
    length = len(text)
    start = 0
    end = length
    while start + SECTION_OVERLAP < length:
        last_word = -1
        end = start + MAX_SECTION_LENGTH

        if end > length:
            end = length
        else:
            while end < length and (end - start - MAX_SECTION_LENGTH) < SENTENCE_SEARCH_LIMIT and text[end] not in SENTENCE_ENDINGS:
                if text[end] in WORDS_BREAKS:
                    last_word = end
                end += 1
            if end < length and text[end] not in SENTENCE_ENDINGS and last_word > 0:
                end = last_word
        if end < length:
            end += 1

        last_word = -1
        while start > 0 and start > end - MAX_SECTION_LENGTH - 2 * SENTENCE_SEARCH_LIMIT and text[start] not in SENTENCE_ENDINGS:
            if text[start] in WORDS_BREAKS:
                last_word = start
            start -= 1
        if text[start] not in SENTENCE_ENDINGS and last_word > 0:
            start = last_word
        if start > 0:
            start += 1

        section_text = text[start:end]
        yield section_text

        start = end - SECTION_OVERLAP

    if start + SECTION_OVERLAP < end:
        yield text[start:end]

def create_sections(filename, text):
    for i, section in enumerate(split_text(text)):
        yield {
            "id": re.sub("[^0-9a-zA-Z_-]","_",f"{filename}-{i}"),
            "content": section,
            "category": values_env_openai['category'],
            "sourcepage": blob_name_from_file(filename),
            "sourcefile": filename
        }

def create_search_index():
    if values_env_openai['verbose']: print(f"Ensuring search index {values_env_openai['index']} exists")
    index_client = SearchIndexClient(endpoint=f"https://{values_env_openai['searchservice']}.search.windows.net/",
                                     credential=search_creds)
    if values_env_openai['index'] not in index_client.list_index_names():
        search_index = SearchIndex(
            name=values_env_openai['index'],
            fields=[
                SimpleField(name="id", type="Edm.String", key=True),
                SearchableField(name="content", type="Edm.String", analyzer_name="en.microsoft"),
                SimpleField(name="category", type="Edm.String", filterable=True, facetable=True),
                SimpleField(name="sourcepage", type="Edm.String", filterable=True, facetable=True),
                SimpleField(name="sourcefile", type="Edm.String", filterable=True, facetable=True)
            ],
            semantic_settings=SemanticSettings(
                configurations=[SemanticConfiguration(
                    name='default',
                    prioritized_fields=PrioritizedFields(
                        title_field=None, prioritized_content_fields=[SemanticField(field_name='content')]))])
        )
        if values_env_openai['verbose']: print(f"Creating {values_env_openai['index']} search index")
        index_client.create_index(search_index)
    else:
        if values_env_openai['verbose']: print(f"Search index {values_env_openai['index']} already exists")

def index_sections(filename, sections):
    if values_env_openai['verbose']: print(f"Indexing sections from '{filename}' into search index '{values_env_openai['index']}'")
    search_client = SearchClient(endpoint=f"https://{values_env_openai['searchservice']}.search.windows.net/",
                                    index_name=values_env_openai['index'],
                                    credential=search_creds)
    i = 0
    batch = []
    for s in sections:
        batch.append(s)
        i += 1
        if i % 1000 == 0:
            results = search_client.upload_documents(documents=batch)
            succeeded = sum([1 for r in results if r.succeeded])
            if values_env_openai['verbose']: print(f"\tIndexed {len(results)} sections, {succeeded} succeeded")
            batch = []

    if len(batch) > 0:
        results = search_client.upload_documents(documents=batch)
        succeeded = sum([1 for r in results if r.succeeded])
        if values_env_openai['verbose']: print(f"\tIndexed {len(results)} sections, {succeeded} succeeded")

def remove_from_index(filename):
    if values_env_openai['verbose']: print(f"Removing sections from '{filename or '<all>'}' from search index '{values_env_openai['index']}'")
    search_client = SearchClient(endpoint=f"https://{values_env_openai['searchservice']}.search.windows.net/",
                                    index_name=values_env_openai['index'],
                                    credential=search_creds)
    while True:
        filter = None if filename == None else f"sourcefile eq '{os.path.basename(filename)}'"
        r = search_client.search("", filter=filter, top=1000, include_total_count=True)
        if r.get_count() == 0:
            break
        r = search_client.delete_documents(documents=[{ "id": d["id"] } for d in r])
        if values_env_openai['verbose']: print(f"\tRemoved {len(r)} sections from index")
        time.sleep(2)

create_search_index()

for filename in glob.glob(FILE_PATH + "/*.txt"):
    if values_env_openai['verbose']: print(f"Processing '{filename}'")
    upload_blobs(filename)
    text = get_document_text(filename)
    sections = create_sections(filename, text)
    index_sections(filename, sections)
