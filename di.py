import os
import time
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.core.credentials import AzureKeyCredential

# ==========================================
# 0. Load Environment Variables
# ==========================================
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
    """Processes multiple PDFs in memory, appends results, and flattens data into rows."""
    all_table_rows = []
    
    # Progress bar for UX
    progress_bar = st.progress(0)
    status_text = st.empty()

    for idx, file in enumerate(uploaded_files):
        file_name = file.name
        file_bytes = file.read() 
        
        status_text.text(f"Processing: {file_name} ({idx + 1}/{len(uploaded_files)})...")
        
        # Call Azure AI 
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
            # EXTRACT TOP-LEVEL FIELDS
            # ==========================================
            
            # 1. Vendor Details
            vendor_name = fields.get("VendorName", {}).get("content", "Unknown")
            vendor_conf = fields.get("VendorName", {}).get("confidence", 0.0)
            vendor_address = fields.get("VendorAddress", {}).get("content", "")
            vendor_address_recipient = fields.get("VendorAddressRecipient", {}).get("content", "")
            vendor_tax_id = fields.get("VendorTaxId", {}).get("content", "")
            
            # 2. Customer Details
            customer_name = fields.get("CustomerName", {}).get("content", "")
            customer_address = fields.get("CustomerAddress", {}).get("content", "")
            customer_tax_id = fields.get("CustomerTaxId", {}).get("content", "")
            billing_address = fields.get("BillingAddress", {}).get("content", "")
            
            # 3. Remit To & Destination Details
            remit_address = fields.get("RemittanceAddress", {}).get("content", "")
            remit_recipient = fields.get("RemittanceAddressRecipient", {}).get("content", "")
            shipping_address = fields.get("ShippingAddress", {}).get("content", "")
            shipping_recipient = fields.get("ShippingAddressRecipient", {}).get("content", "")
            service_address = fields.get("ServiceAddress", {}).get("content", "")
            
            # 4. Invoice Metadata
            invoice_id = fields.get("InvoiceId", {}).get("content", "")
            invoice_date = fields.get("InvoiceDate", {}).get("content", "")
            due_date = fields.get("DueDate", {}).get("content", "")
            po_number = fields.get("PurchaseOrder", {}).get("content", "")
            
            # 5. Financial Totals
            sub_total = fields.get("SubTotal", {}).get("content", "")
            total_tax = fields.get("TotalTax", {}).get("content", "")
            invoice_total = fields.get("InvoiceTotal", {}).get("content", "")
            amount_due = fields.get("AmountDue", {}).get("content", "")
            prev_unpaid_balance = fields.get("PreviousUnpaidBalance", {}).get("content", "")

            # 6. Extract Multiple Taxes (CGST, SGST, etc.)
            tax_breakdown_list = []
            if "TaxDetails" in fields:
                for tax_item in fields["TaxDetails"].get("valueArray", []):
                    tax_obj = tax_item.get("valueObject", {})
                    tax_amount = tax_obj.get("Amount", {}).get("content", "")
                    tax_rate = tax_obj.get("Rate", {}).get("content", "")
                    
                    if tax_rate and tax_amount:
                        tax_breakdown_list.append(f"{tax_rate} ({tax_amount})")
                    elif tax_amount:
                        tax_breakdown_list.append(f"{tax_amount}")
                        
            tax_summary_string = " | ".join(tax_breakdown_list)

            # Enforce the 95% confidence threshold logic
            needs_review = "Yes" if vendor_conf < 0.95 else "No"
            
            # ==========================================
            # FLATTEN INTO ROWS: Line Items + Top Level
            # ==========================================
            if "Items" in fields:
                items_array = fields["Items"].get("valueArray", [])
                
                # Loop through every single line item
                for item in items_array:
                    item_fields = item.get("valueObject", {})
                    
                    # Create the base row. THIS is where we insert all top-level info into the row.
                    row_data = {
                        "FileName": file_name,
                        "InvoiceId": invoice_id,
                        "InvoiceDate": invoice_date,
                        "DueDate": due_date,
                        "PO_Number": po_number,
                        "Vendor": vendor_name,
                        "VendorTaxId": vendor_tax_id,
                        "VendorAddress": vendor_address,
                        "VendorAddressRecipient": vendor_address_recipient,
                        "Vendor_Confidence": vendor_conf,
                        "Requires_Manual_Review": needs_review,
                        "Customer": customer_name,
                        "CustomerTaxId": customer_tax_id,
                        "CustomerAddress": customer_address,
                        "BillingAddress": billing_address,
                        "RemittanceAddress": remit_address,
                        "RemittanceRecipient": remit_recipient,
                        "DestinationAddress": shipping_address,
                        "DestinationRecipient": shipping_recipient,
                        "ServiceAddress": service_address,
                        "InvoiceSubTotal": sub_total,
                        "InvoiceTotalTax": total_tax,
                        "TaxBreakdown": tax_summary_string,
                        "InvoiceTotal": invoice_total,
                        "AmountDue": amount_due,
                        "PreviousUnpaidBalance": prev_unpaid_balance
                    }
                    
                    # Dynamically add the specific line-item columns (ProductCode, Quantity, Freight, etc.)
                    for field_name, field_value in item_fields.items():
                        content = field_value.get("content")
                        if content and str(content).strip():
                            # Prefix with LineItem_ so it doesn't get confused with document totals
                            row_data[f"LineItem_{field_name}"] = content
                            
                    # Append the fully loaded row to our master list
                    all_table_rows.append(row_data)
        
        # Update progress
        progress_bar.progress((idx + 1) / len(uploaded_files))
        
    status_text.text("Processing complete!")
    
    # Convert the list to a DataFrame and align columns
    df = pd.DataFrame(all_table_rows).fillna("")
    return df


# ==========================================
# 4. Main Application Flow
# ==========================================
def main():
    st.write("Upload one or more PDF invoices to extract full metadata, addresses, multi-taxes, and structured line items.")
    
    client = get_azure_client()
    
    # Allow multiple file uploads
    uploaded_files = st.file_uploader(
        "Choose PDF invoices (Batch Processing)", 
        type=["pdf", "png", "jpeg", "jpg"], 
        accept_multiple_files=True
    )
    
    if uploaded_files:
        if st.button("Extract Batch Data", type="primary"):
            with st.spinner("Extracting documents..."):
                
                # START Timer
                start_time = time.time()
                
                # Run extraction
                final_df = process_invoices(uploaded_files, client)
                
                # STOP Timer
                end_time = time.time()
                execution_time = end_time - start_time
                
                if not final_df.empty:
                    # Display success and total execution time
                    st.success(f"✅ Successfully extracted {len(final_df)} total line items from {len(uploaded_files)} files in **{execution_time:.2f} seconds**.")
                    
                    # Style rows based on 95% threshold
                    def highlight_review_rows(row):
                        if row.get('Requires_Manual_Review') == 'Yes':
                            return ['background-color: #ffcccc'] * len(row)
                        return [''] * len(row)

                    styled_df = final_df.style.apply(highlight_review_rows, axis=1)
                    
                    # Display table
                    st.dataframe(
                        styled_df, 
                        use_container_width=True
                    )
                    
                    # Download button
                    csv_data = final_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="⬇️ Download Combined Batch Data as CSV",
                        data=csv_data,
                        file_name="invoice_batch_extraction_master.csv",
                        mime="text/csv",
                    )
                else:
                    st.warning("No line items could be extracted from the provided files.")

if __name__ == "__main__":
    main()
