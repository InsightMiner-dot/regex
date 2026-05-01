import streamlit as st
import fitz  # PyMuPDF
import base64
import instructor
import os
import sqlite3
import asyncio
from openai import AsyncAzureOpenAI
from pydantic import BaseModel, Field
from typing import List, Optional, Tuple
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv(override=True)

AZURE_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")
AZURE_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")

# ==============================================================
# 1. New Schema for PO & Contract Extraction
# ==============================================================

class POLineItem(BaseModel):
    page_number: Optional[int] = Field(None, description="Page number where item appears")
    part_number: Optional[str] = Field(None, description="Part number or SKU")
    base_material: Optional[str] = Field(None, description="Description of the base material")
    index_flag: Optional[str] = Field(None, description="Y/N or indicator if an index price is applied")
    index_name: Optional[str] = Field(None, description="Name of the index (e.g., LME, Platts)")
    index_price: Optional[float] = Field(None, description="The specific index price used")
    quantity: Optional[float] = Field(None, description="Number of units")
    final_value: Optional[float] = Field(None, description="Unit price or final calculated line price")
    total_price: Optional[float] = Field(None, description="Line total (Quantity * Final Value)")

class POData(BaseModel):
    contract_number: Optional[str] = Field(None, description="The master contract or PO reference number")
    issue_date: Optional[str] = Field(None, description="Date the document was issued")
    currency: Optional[str] = Field(None, description="3-letter currency code")
    document_total: float = Field(description="The grand total listed on the document")
    line_items: List[POLineItem]

class PODocument(BaseModel):
    documents: List[POData] = Field(description="List of POs or Contracts found in the file")

# ==============================================================
# 2. PDF Processing Helpers
# ==============================================================

async def pdf_to_images(file_bytes, max_pages, dpi):
    doc = fitz.open(stream=file_bytes, filetype="pdf")
    images = []
    for i in range(min(len(doc), max_pages)):
        page = doc[i]
        pix = page.get_pixmap(dpi=dpi)
        images.append(base64.b64encode(pix.tobytes("jpeg")).decode('utf-8'))
    total_pages = len(doc)
    doc.close()
    return images, total_pages

async def extract_po_async(client, file_bytes, max_pages, dpi):
    images, total_pages = await pdf_to_images(file_bytes, max_pages, dpi)
    
    sys_prompt = (
        "You are an expert supply chain analyst. Extract data from Purchase Orders and Contracts. "
        "Pay close attention to Index Flags and Material Indexing prices. "
        "Strictly group items by Contract Number."
    )

    content = [{"type": "text", "text": "Extract all PO and Contract line items."}]
    for img in images:
        content.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img}"}})

    response = await client.chat.completions.create(
        model=AZURE_DEPLOYMENT,
        response_model=PODocument,
        messages=[{"role": "system", "content": sys_prompt}, {"role": "user", "content": content}]
    )
    return response, total_pages

# ==============================================================
# 3. Streamlit UI Logic
# ==============================================================

st.set_page_config(page_title="PO & Contract Intelligence", layout="wide")

with st.sidebar:
    st.header("⚙️ Settings")
    max_p = st.number_input("Max Pages", 1, 100, 15)
    res_dpi = st.slider("Resolution (DPI)", 72, 400, 200)
    batch_size = st.slider("Concurrency", 1, 10, 4)
    st.divider()
    uploaded_files = st.file_uploader("Upload PO/Contract PDFs", type="pdf", accept_multiple_files=True)

st.title("📑 PO & Contract Data Extraction")

if st.button("🚀 Start Extraction") and uploaded_files:
    client = instructor.from_openai(AsyncAzureOpenAI(
        azure_endpoint=AZURE_ENDPOINT, 
        api_key=AZURE_API_KEY, 
        api_version=AZURE_API_VERSION
    ))
    
    all_extracted_rows = []
    progress_bar = st.progress(0)
    
    # Batch Processing
    for i in range(0, len(uploaded_files), batch_size):
        chunk = uploaded_files[i : i + batch_size]
        chunk_bytes = [f.read() for f in chunk]
        
        # Async Execution
        tasks = [extract_po_async(client, b, max_p, res_dpi) for b in chunk_bytes]
        batch_results = asyncio.run(asyncio.gather(*tasks, return_exceptions=True))
        
        for idx, result in enumerate(batch_results):
            filename = chunk[idx].name
            
            if isinstance(result, Exception):
                st.error(f"Failed to process {filename}: {result}")
                continue
            
            doc_data, total_pages = result
            for doc in doc_data.documents:
                for item in doc.line_items:
                    # Logic: Check if (Qty * Final Value) matches reported Total Price
                    calc_line_total = (item.quantity or 0) * (item.final_value or 0)
                    variance = round(calc_line_total - (item.total_price or 0), 2)
                    
                    all_extracted_rows.append({
                        "File Name": filename,
                        "Page Number": item.page_number,
                        "Contract Number": doc.contract_number,
                        "Issue Date": doc.issue_date,
                        "Part Number": item.part_number,
                        "Base Material": item.base_material,
                        "Index Flag": item.index_flag,
                        "Index Name": item.index_name,
                        "Index Price": item.index_price,
                        "Quantity": item.quantity,
                        "Final Value": item.final_value,
                        "Total Price": item.total_price,
                        "Math Variance": variance
                    })
        
        progress_bar.progress((i + len(chunk)) / len(uploaded_files))

    if all_extracted_rows:
        df = pd.DataFrame(all_extracted_rows)
        st.subheader("✅ Extraction Results")
        st.dataframe(df, use_container_width=True, hide_index=True)
        
        # Download Option
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Download Results (CSV)", csv, "extracted_po_data.csv", "text/csv")
    else:
        st.warning("No data found in the uploaded documents.")
