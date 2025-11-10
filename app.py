# app.py
from fastapi import FastAPI, Request
from schema_utils import infer_schema, merge_schemas
from store import save_doc, get_schema, save_schema

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
        save_doc("docs", d)
    save_schema(global_schema)
    return {"status": "ok", "merged_schema": global_schema}
