# app.py (MODIFIED)
from fastapi import FastAPI, Request
from schema_utils import infer_schema, merge_schemas
# UPDATED: Import the new data retrieval function
from store import save_doc, get_schema, save_schema, get_all_docs 

app = FastAPI()

@app.post("/ingest")
async def ingest(request: Request):
    data = await request.json()
    # handle single or list
    docs = data if isinstance(data, list) else [data]
    global_schema = get_schema() or {}
    for d in docs:
        # infer schema for this doc
        s = infer_schema(d)
        global_schema = merge_schemas(global_schema, s)
        # Data is saved here:
        save_doc("docs", d) 
    save_schema(global_schema)
    return {"status": "ok", "merged_schema": global_schema}

@app.get("/schema")
def get_current_schema():
    """Retrieves the current dynamic schema stored in the database."""
    global_schema = get_schema()
    return global_schema

# NEW: Add a route to retrieve the raw stored data
@app.get("/data")
def get_all_documents():
    """Retrieves all stored documents from the database."""
    docs = get_all_docs()
    return docs
