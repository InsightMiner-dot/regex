import streamlit as st
import fitz  # PyMuPDF
import base64
import instructor
import os
import asyncio
from openai import AsyncAzureOpenAI
from pydantic import BaseModel, Field
from typing import List, Optional
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv(override=True)

AZURE_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")
AZURE_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")

# ==============================================================
# 1. Precise Schema for Material Cost Breakdown
# ==============================================================

class MaterialLine(BaseModel):
    page_number: Optional[int] = Field(None, description="Page number")
    part_number: Optional[str] = Field(None, description="The primary Part Number or SKU")
    base_price: Optional[str] = Field(None, description="The Base Price listed above the material breakdown")
    base_material: Optional[str] = Field(None, description="The name of the material (e.g. Copper, Plastic)")
    auto_update_y_n: Optional[str] = Field(None, description="The Y/N value found under 'Auto Update'")
    exchange: Optional[str] = Field(None, description="The exchange source (e.g. LME Monthly)")
    exchange_value: Optional[str] = Field(None, description="The price/value of the exchange")
    mass: Optional[str] = Field(None, description="The Mass/Weight value")
    cost: Optional[str] = Field(None, description="The Cost value for this specific material component")
    material_level_total_cost: Optional[float] = Field(None, description="The calculated total for the material row (formerly final value)")
    quantity: Optional[float] = Field(None, description="Quantity of units")
    final_value: Optional[float] = Field(None, description="The ultimate total price/grand total at the very end")

class ContractData(BaseModel):
    contract_number: Optional[str] = Field(None, description="Contract/PO Reference Number")
    issue_date: Optional[str] = Field(None, description="Document Date")
    seller_name: Optional[str] = Field(None, description="Full Name of the Seller/Supplier")
    seller_address: Optional[str] = Field(None, description="Full Address of the Seller")
    buyer_name: Optional[str] = Field(None, description="Full Name of the Buyer/Customer")
    buyer_address: Optional[str] = Field(None, description="Full Address of the Buyer")
    currency: Optional[str] = Field(None, description="3-letter Currency code")
    material_breakdown: List[MaterialLine]

class ContractDocument(BaseModel):
    documents: List[ContractData]

# ==============================================================
# 2. Async & Loop Management (Thread Safe)
# ==============================================================

def run_async_tasks(tasks):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))

async def pdf_to_images(file_bytes, max_pages, dpi):
    doc = fitz.open(stream=file_bytes, filetype="pdf")
    images = []
    for i in range(min(len(doc), max_pages)):
        page = doc[i]
        pix = page.get_pixmap(dpi=dpi)
        images.append(base64.b64encode(pix.tobytes("jpeg")).decode('utf-8'))
    doc.close()
    return images

async def extract_contract_async(client, file_bytes, max_pages, dpi):
    images = await pdf_to_images(file_bytes, max_pages, dpi)
    
    sys_prompt = """
    You are an expert procurement analyst specializing in material indexing contracts.
    
    EXTRACTION RULES:
    1. BASE PRICE: This is explicitly mentioned above the Base Material section. Extract it first.
    2. AUTO UPDATE Y or N: Extract only the 'Y' or 'N' indicator.
    3. EXCHANGE: This is the index source like 'LME Monthly'.
    4. MATERIAL LEVEL TOTAL COST: This is the specific sub-total for that material row.
    5. FINAL VALUE: This is the grand total price appearing at the end of the line or document.
    
    Support multiple languages and handle vertical layouts where headers and values are offset.
    """

    content = [{"type": "text", "text": "Extract all Seller, Buyer, and Material Indexing data into the structured schema."}]
    for img in images:
        content.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img}"}})

    response = await client.chat.completions.create(
        model=AZURE_DEPLOYMENT,
        response_model=ContractDocument,
        messages=[{"role": "system", "content": sys_prompt}, {"role": "user", "content": content}]
    )
    return response

# ==============================================================
# 3. UI Implementation
# ==============================================================

st.set_page_config(page_title="Advanced Material Indexing", layout="wide")

with st.sidebar:
    st.header("Settings")
    max_p = st.number_input("Max Pages", 1, 100, 15)
    res_dpi = st.slider("DPI", 72, 400, 200)
    batch_size = st.slider("Concurrency", 1, 10, 4)
    uploaded_files = st.file_uploader("Upload PDFs", type="pdf", accept_multiple_files=True)

st.title("📑 Supply Chain Contract Intelligence")

if st.button("🚀 Run Extraction") and uploaded_files:
    client = instructor.from_openai(AsyncAzureOpenAI(
        azure_endpoint=AZURE_ENDPOINT, api_key=AZURE_API_KEY, api_version=AZURE_API_VERSION
    ))
    
    results_data = []
    progress = st.progress(0)
    
    for i in range(0, len(uploaded_files), batch_size):
        chunk = uploaded_files[i : i + batch_size]
        tasks = [extract_contract_async(client, f.read(), max_p, res_dpi) for f in chunk]
        batch_out = run_async_tasks(tasks)
        
        for idx, result in enumerate(batch_out):
            if isinstance(result, Exception):
                st.error(f"Error in {chunk[idx].name}: {result}")
                continue
            
            for doc in result.documents:
                for row in doc.material_breakdown:
                    results_data.append({
                        "File Name": chunk[idx].name,
                        "Seller Name": doc.seller_name,
                        "Seller Address": doc.seller_address,
                        "Buyer Name": doc.buyer_name,
                        "Buyer Address": doc.buyer_address,
                        "Contract Number": doc.contract_number,
                        "Issue Date": doc.issue_date,
                        "Base Price": row.base_price,
                        "Part Number": row.part_number,
                        "Base Material": row.base_material,
                        "Auto Update Y or N": row.auto_update_y_n,
                        "Exchange": row.exchange,
                        "Exchange Value": row.exchange_value,
                        "Mass": row.mass,
                        "Cost": row.cost,
                        "Material Level Total Cost": row.material_level_total_cost,
                        "Quantity": row.quantity,
                        "Final Value": row.final_value
                    })
        progress.progress((i + len(chunk)) / len(uploaded_files))

    if results_data:
        df = pd.DataFrame(results_data)
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.download_button("📥 Export to CSV", df.to_csv(index=False), "contract_data.csv", "text/csv")
