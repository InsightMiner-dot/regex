import os
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.core.credentials import AzureKeyCredential

# ==========================================
# 0. Load Environment Variables
# ==========================================
# This reads your local .env file. override=True ensures it updates any existing vars.
load_dotenv(override=True) 

# ==========================================
# 1. Page Configuration & UI Layout
# ==========================================
st.set_page_config(page_title="Invoice Automation Platform", layout="wide")

# Custom vanilla CSS for a clean, top-level sticky header
st.markdown("""
    <style>
    .sticky-header {
        position: sticky;
        top: 0px;
        background-color: var(--background-color);
        z-index: 999;
        padding: 15px 0px;
        border-bottom: 1px solid #ddd;
        margin-bottom: 20px;
    }
    .sticky-header h3 { 
        margin: 0; 
        padding: 0; 
    }
    </style>
    
    <div class="sticky-header">
        <h3>📄 Invoice Automation Platform</h3>
    </div>
""", unsafe_allow_html=True)


# ==========================================
# 2. Azure Client Initialization
# ==========================================
@st.cache_resource
def get_azure_client():
    """Initializes the Document Intelligence client securely."""
    endpoint = os.getenv("DOCUMENT_INTELLIGENCE_ENDPOINT")
    key = os.getenv("DOCUMENT_INTELLIGENCE_KEY")
    
    if not endpoint or not key:
        st.error("⚠️ Azure credentials not found. Please ensure they are set in your .env file.")
        st.stop()
        
    return DocumentIntelligenceClient(endpoint=endpoint, credential=AzureKeyCredential(key))


# ==========================================
# 3. Core Extraction Logic
# ==========================================
def process_invoices(uploaded_files, client):
    """Processes multiple PDFs in memory and returns a structured DataFrame."""
    all_table_rows = []
    
    # Progress bar for UX
    progress_bar = st.progress(0)
    status_text = st.empty()

    for idx, file in enumerate(uploaded_files):
        file_name = file.name
        file_bytes = file.read() # Read directly into memory
        
        status_text.text(f"Processing: {file_name} ({idx + 1}/{len(uploaded_files)})...")
        
        # Call Azure AI (Using the updated 'body' parameter for v1.0.0+)
        poller = client.begin_analyze_document(
            model_id="prebuilt-invoice", 
            body=file_bytes, 
            content_type="application/pdf"
        )
        result = poller.result()
        
        if result.documents:
            invoice = result.documents[0]
            fields = invoice.fields
            
            # ==========================================
            # EXTRACT TOP-LEVEL DOCUMENT FIELDS
            # ==========================================
            
            # 1. Vendor Details
            vendor_name = fields.get("VendorName", {}).get("content", "Unknown")
            vendor_conf = fields.get("VendorName", {}).get("confidence", 0.0)
            vendor_tax_id = fields.get("VendorTaxId", {}).get("content", "")
            
            # 2. Customer Details
            customer_name = fields.get("CustomerName", {}).get("content", "")
            
            # 3. Invoice Metadata
            invoice_id = fields.get("InvoiceId", {}).get("content", "")
            invoice_date = fields.get("InvoiceDate", {}).get("content", "")
            po_number = fields.get("PurchaseOrder", {}).get("content", "")
            
            # 4. Financial Totals
            sub_total = fields.get("SubTotal", {}).get("content", "")
            total_tax = fields.get("TotalTax", {}).get("content", "")
            invoice_total = fields.get("InvoiceTotal", {}).get("content", "")

            # Enforce the 95% confidence threshold logic for data integrity
            needs_review = "Yes" if vendor_conf < 0.95 else "No"
            
            # ==========================================
            # EXTRACT LINE ITEM COLUMNS
            # ==========================================
            if "Items" in fields:
                items_array = fields["Items"].get("valueArray", [])
                
                for item in items_array:
                    item_fields = item.get("valueObject", {})
                    
                    # Create the base row with ALL top-level information
                    row_data = {
                        "FileName": file_name,
                        "InvoiceId": invoice_id,
                        "InvoiceDate": invoice_date,
                        "PO_Number": po_number,
                        "Vendor": vendor_name,
                        "VendorTaxId": vendor_tax_id,
                        "Vendor_Confidence": vendor_conf,
                        "Requires_Manual_Review": needs_review,
                        "Customer": customer_name,
                        "InvoiceSubTotal": sub_total,
                        "InvoiceTotalTax": total_tax,
                        "InvoiceTotal": invoice_total
                    }
                    
                    # Dynamically add all available line-item columns
                    for field_name, field_value in item_fields.items():
                        content = field_value.get("content")
                        if content and str(content).strip():
                            row_data[f"LineItem_{field_name}"] = content
                            
                    all_table_rows.append(row_data)
        
        # Update progress
        progress_bar.progress((idx + 1) / len(uploaded_files))
        
    status_text.text("Processing complete!")
    
    # Convert to DataFrame and align columns perfectly
    df = pd.DataFrame(all_table_rows).fillna("")
    return df


# ==========================================
# 4. Main Application Flow
# ==========================================
def main():
    st.write("Upload one or more PDF invoices to extract full metadata and structured line items.")
    
    client = get_azure_client()
    
    # Allow multiple file uploads
    uploaded_files = st.file_uploader(
        "Choose PDF invoices", 
        type=["pdf"], 
        accept_multiple_files=True
    )
    
    if uploaded_files:
        if st.button("Extract Data", type="primary"):
            with st.spinner("Extracting documents..."):
                # Run the extraction
                final_df = process_invoices(uploaded_files, client)
                
                if not final_df.empty:
                    st.success(f"Successfully extracted {len(final_df)} line items from {len(uploaded_files)} files.")
                    
                    # Function to color rows based on the 95% confidence threshold check
                    def highlight_review_rows(row):
                        if row.get('Requires_Manual_Review') == 'Yes':
                            return ['background-color: #ffcccc'] * len(row)
                        return [''] * len(row)

                    # Apply the styling to the DataFrame
                    styled_df = final_df.style.apply(highlight_review_rows, axis=1)
                    
                    # Display the styled DataFrame in Streamlit
                    st.dataframe(
                        styled_df, 
                        use_container_width=True
                    )
                    
                    # Provide CSV download button (Make sure to export final_df, not styled_df)
                    csv_data = final_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download Data as CSV",
                        data=csv_data,
                        file_name="invoice_batch_extraction_full.csv",
                        mime="text/csv",
                    )
                else:
                    st.warning("No line items could be extracted from the provided files.")

if __name__ == "__main__":
    main()
