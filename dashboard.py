# dashboard.py (MODIFIED for multiple file upload)
import streamlit as st
import requests
import json

st.title("Dynamic ETL Demo")

# MODIFICATION: Use accept_multiple_files=True
uploaded_files = st.file_uploader("Upload JSON file(s) (single object or array)", type=["json"], accept_multiple_files=True)

if uploaded_files:
    for uploaded_file in uploaded_files:
        st.subheader(f"Processing: {uploaded_file.name}")
        try:
            # Load JSON data from the file content
            # st.file_uploader returns file-like objects, so we use json.load
            payload = json.load(uploaded_file)
            
            # Post to FastAPI endpoint
            resp = requests.post("http://127.0.0.1:8000/ingest", json=payload)
            
            if resp.status_code == 200:
                st.success(f"✅ File '{uploaded_file.name}' uploaded and schema updated successfully!")
            else:
                st.error(f"❌ Upload failed for '{uploaded_file.name}'. Status: {resp.status_code}. Check if the FastAPI server is running.")
            
            st.write("Ingest response (includes latest merged schema):")
            st.json(resp.json())

        except Exception as e:
            st.error(f"An error occurred while processing '{uploaded_file.name}': {e}")


if st.button("Show stored schema"):
    # This hits the newly added /schema endpoint in app.py
    try:
        resp = requests.get("http://127.0.0.1:8000/schema")
        
        if resp.status_code == 200:
            st.write("Current Global Schema from DB:")
            st.json(resp.json())
        else:
            st.error("❌ Failed to fetch schema. Check if the FastAPI server is running.")
    except requests.exceptions.ConnectionError:
        st.error("❌ Could not connect to the FastAPI server. Please ensure 'app.py' is running (uvicorn app:app --reload).")
