# dashboard.py (MODIFIED - New button added at the bottom)
import streamlit as st
import requests
import json
from schema_utils import transform_to_flat_schema # Ensure this is present

st.title("Dynamic ETL Demo")

# Use accept_multiple_files=True
uploaded_files = st.file_uploader("Upload JSON file(s) (each containing a single object or array)", type=["json"], accept_multiple_files=True)

if uploaded_files:
    if st.button("Ingest All Files (One Batch)"):
        all_documents = []
        st.info(f"Preparing to ingest {len(uploaded_files)} files into a single batch...")
        
        # 1. Extract: Loop through all files and collect all documents
        for uploaded_file in uploaded_files:
            try:
                # Load JSON data from the file content
                payload = json.load(uploaded_file)
                
                # If the payload is a list (array of documents), extend the list.
                if isinstance(payload, list):
                    all_documents.extend(payload)
                    st.success(f"✅ Extracted {len(payload)} documents from: {uploaded_file.name}")
                elif isinstance(payload, dict):
                    all_documents.append(payload)
                    st.success(f"✅ Extracted 1 document from: {uploaded_file.name}")
                else:
                    st.warning(f"File {uploaded_file.name} contained unexpected data type ({type(payload).__name__}) and was skipped.")

            except Exception as e:
                st.error(f"❌ Error reading JSON from {uploaded_file.name}: {e}")
                
        if all_documents:
            st.subheader(f"Sending {len(all_documents)} total documents to the ETL pipeline...")

            # 2. Transform & Load: Send all documents in a single POST request
            try:
                # Send the entire list of documents to the /ingest endpoint
                resp = requests.post("http://127.0.0.1:8000/ingest", json=all_documents)
                
                if resp.status_code == 200:
                    st.success("🎉 Ingestion complete! All documents processed and global schema updated.")
                    response_data = resp.json()
                    st.write("Final Ingest response (Flattened Schema for Readability):")
                    
                    # Use the flattener function for cleaner output
                    clean_schema = transform_to_flat_schema(response_data.get("merged_schema", {}))
                    st.json(clean_schema)
                else:
                    st.error(f"❌ Upload failed. Status: {resp.status_code}. Check if the FastAPI server is running.")
            
            except requests.exceptions.ConnectionError:
                st.error("❌ Could not connect to the FastAPI server. Ensure 'app.py' is running (uvicorn app:app --reload).")

# Button to show the final stored schema
if st.button("Show Stored Global Schema"):
    # This hits the /schema endpoint added previously
    try:
        resp = requests.get("http://127.0.0.1:8000/schema")
        
        if resp.status_code == 200:
            schema_data = resp.json()
            st.write("Current Global Schema from DB (Flattened View):")
            
            # Use the flattener function for cleaner output
            clean_schema = transform_to_flat_schema(schema_data)
            st.json(clean_schema)
        else:
            st.error("❌ Failed to fetch schema. Check if the FastAPI server is running.")
    except requests.exceptions.ConnectionError:
        st.error("❌ Could not connect to the FastAPI server. Please ensure 'app.py' is running (uvicorn app:app --reload).")

# 🆕 NEW BLOCK: Button to show the raw data
if st.button("Show Stored Documents (Raw Data)"):
    # This hits the new /data endpoint we created in app.py
    try:
        resp = requests.get("http://127.0.0.1:8000/data")
        
        if resp.status_code == 200:
            data_list = resp.json()
            st.subheader(f"Current Raw Documents from DB ({len(data_list)} documents):")
            # Display the list of documents
            st.json(data_list) 
        else:
            st.error("❌ Failed to fetch data. Check if the FastAPI server is running.")
    except requests.exceptions.ConnectionError:
        st.error("❌ Could not connect to the FastAPI server. Please ensure 'app.py' is running (uvicorn app:app --reload).")ease ensure 'app.py' is running (uvicorn app:app --reload).")vicorn app:app --reload).")
