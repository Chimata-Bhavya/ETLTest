# app.py
import streamlit as st
import json
from ETLPipeline import run_pipeline, node_outputs
from GraphTest import ETLGraphBuilder, load_json_file

st.set_page_config(page_title="ETL Pipeline UI", layout="wide")
st.title("ETL Pipeline Explorer")

DATA_FOLDER = r"C:\Users\Chimata.Charita\Downloads\Trial_Copy\Test"
ROOT_JOB_NAME = "GENDER_KTR"

# Initialize builder once
if "builder" not in st.session_state:
    builder = ETLGraphBuilder(DATA_FOLDER)
    builder.build_from_json(ROOT_JOB_NAME)
    st.session_state.builder = builder

builder = st.session_state.builder

# Graph display (simple JSON-based for now)
st.subheader("Pipeline Graph")
st.json(builder.get_graph_data())  # temporary — replace with real graph later

# Run pipeline
if st.button("Run Pipeline"):
    with st.spinner("Executing..."):
        run_pipeline()
    st.success("Done!")

# Preview section
if node_outputs:
    st.subheader("Preview Node Output")
    node_list = list(node_outputs.keys())
    selected = st.selectbox("Select node", node_list)

    if selected:
        df = node_outputs.get(selected)
        if df is None:
            st.error("No output (failed or empty)")
        else:
            st.write(f"**{selected}** — Total rows: {df.count()}")
            st.dataframe(df.limit(10).toPandas())
