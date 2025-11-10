# dashboard.py (MODIFIED for single batch ingest)
import streamlit as st
import requests
import json

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
                # If it's a single object, append it.
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
                    st.write("Final Ingest response (includes singular merged schema):")
                    st.json(resp.json())
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
            st.write("Current Global Schema from DB:")
            st.json(resp.json())
        else:
            st.error("❌ Failed to fetch schema. Check if the FastAPI server is running.")
    except requests.exceptions.ConnectionError:
        st.error("❌ Could not connect to the FastAPI server. Please ensure 'app.py' is running (uvicorn app:app --reload).")vicorn app:app --reload).")
