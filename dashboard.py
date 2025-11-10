# dashboard.py
import streamlit as st
import requests
import json

st.title("Dynamic ETL Demo")

uploaded = st.file_uploader("Upload JSON file (single object or array)", type=["json"])
if uploaded:
    payload = json.load(uploaded)
    resp = requests.post("http://127.0.0.1:8000/ingest", json=payload)
    
    if resp.status_code == 200:
        st.success("✅ File uploaded and schema updated successfully!")
    else:
        st.error("❌ Upload failed. Check if the FastAPI server is running.")
    
    st.write("Ingest response:")
    st.json(resp.json())


if st.button("Show stored schema"):
    resp = requests.get("http://127.0.0.1:8000/schema")  # we'll add this route if needed, or read directly from DB in dashboard
    st.json(resp.json())
