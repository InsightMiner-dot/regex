import os
import re
import io
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
    .sticky-header h3 { margin: 0; padding: 0; }
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
    endpoint = os.getenv("DOCUMENT_INTELLIGENCE_ENDPOINT")
    key = os.getenv("DOCUMENT_INTELLIGENCE_KEY")
    if not endpoint or not key:
        st.error("⚠️ Azure credentials not found. Please ensure they are set in your .env file.")
        st.stop()
    return DocumentIntelligenceClient(endpoint=endpoint, credential=AzureKeyCredential(key))


# ==========================================
# Helper: Math String Cleaner
# ==========================================
def clean_amount_to_float(val):
    """Converts strings like '$1,500.00' or '1,500' to a clean float 1500.00 for math."""
    if pd.isna(val) or val == "":
        return 0.0
    # Strip everything except numbers, decimals, and negative signs
    cleaned = re.sub(r'[^\d.-]', '', str(val))
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


# ==========================================
# 3. Core Extraction Logic (EXHAUSTIVE)
# ==========================================
def process_invoices(uploaded_files, client):
    all_table_rows = []
    progress_bar = st.progress(0)
    status_text = st.empty()

    for idx, file in enumerate(uploaded_files):
        file_name = file.name
        file_bytes = file.read() 
        
        status_text.text(f"Processing: {file_name} ({idx + 1}/{len(uploaded_files)})...")
        
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
            # EXTRACT TOP-LEVEL FIELDS (ALL RESTORED)
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
                for item in fields["Items"].get("valueArray", []):
                    item_fields = item.get("valueObject", {})
                    
                    # MASSIVE BASE ROW: Nothing is missed
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
                    
                    # Dynamically add the specific line-item columns
                    for field_name, field_value in item_fields.items():
                        content = field_value.get("content")
                        if content and str(content).strip():
                            row_data[f"LineItem_{field_name}"] = content
                            
                    all_table_rows.append(row_data)
        
        progress_bar.progress((idx + 1) / len(uploaded_files))
        
    status_text.text("Processing complete!")
    return pd.DataFrame(all_table_rows).fillna("")


# ==========================================
# 4. Main Application Flow
# ==========================================
def main():
    st.write("Upload PDF invoices to extract full exhaustive metadata, flatten line items, and generate an automated QC summary.")
    
    client = get_azure_client()
    uploaded_files = st.file_uploader(
        "Choose PDF invoices (Batch Processing)", 
        type=["pdf", "png", "jpeg", "jpg"], 
        accept_multiple_files=True
    )
    
    if uploaded_files:
        if st.button("Extract & Run QC Check", type="primary"):
            with st.spinner("Extracting documents and calculating variances..."):
                
                # START Timer
                start_time = time.time()
                
                final_df = process_invoices(uploaded_files, client)
                
                # STOP Timer
                execution_time = time.time() - start_time
                
                if not final_df.empty:
                    st.success(f"✅ Extracted {len(final_df)} line items in **{execution_time:.2f} seconds**.")
                    
                    # ==========================================
                    # QC RECONCILIATION MATH ENGINE
                    # ==========================================
                    if 'LineItem_Amount' not in final_df.columns:
                        final_df['LineItem_Amount'] = "0"
                        
                    # Clean the strings to floats for math
                    final_df['Math_LineTotal'] = final_df['LineItem_Amount'].apply(clean_amount_to_float)
                    final_df['Math_InvoiceTotal'] = final_df['InvoiceTotal'].apply(clean_amount_to_float)
                    
                    # Group by File and Invoice Number to sum up the lines
                    qc_df = final_df.groupby(['FileName', 'InvoiceId']).agg(
                        Invoice_Total_Extracted=('Math_InvoiceTotal', 'first'),
                        Sum_Of_Line_Totals=('Math_LineTotal', 'sum')
                    ).reset_index()
                    
                    # Calculate Variance
                    qc_df['Variance'] = round(qc_df['Invoice_Total_Extracted'] - qc_df['Sum_Of_Line_Totals'], 2)
                    
                    # Determine Status (Allowing 0.05 margin of error for rounding)
                    qc_df['Status'] = qc_df['Variance'].apply(lambda x: '✅ Match' if abs(x) < 0.05 else '❌ Mismatch')

                    # ==========================================
                    # UI TABS: Displaying the Results
                    # ==========================================
                    tab1, tab2 = st.tabs(["📊 QC Summary (Reconciliation)", "📝 Master Extracted Data"])
                    
                    with tab1:
                        st.subheader("Invoice Math Reconciliation")
                        def color_qc_status(val):
                            color = '#ff4b4b' if '❌' in str(val) else '#21c354'
                            return f'color: {color}; font-weight: bold;'
                            
                        styled_qc = qc_df.style.map(color_qc_status, subset=['Status'])
                        st.dataframe(styled_qc, use_container_width=True)
                        
                    with tab2:
                        st.subheader("Flattened Master Data (Exhaustive)")
                        def highlight_review_rows(row):
                            if row.get('Requires_Manual_Review') == 'Yes':
                                return ['background-color: #ffcccc'] * len(row)
                            return [''] * len(row)
                            
                        # Drop temporary math columns before displaying/exporting
                        display_df = final_df.drop(columns=['Math_LineTotal', 'Math_InvoiceTotal'], errors='ignore')
                        styled_raw = display_df.style.apply(highlight_review_rows, axis=1)
                        st.dataframe(styled_raw, use_container_width=True)

                    # ==========================================
                    # MULTI-SHEET EXCEL EXPORT
                    # ==========================================
                    excel_buffer = io.BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                        qc_df.to_excel(writer, sheet_name='QC Summary', index=False)
                        display_df.to_excel(writer, sheet_name='Master Data', index=False)
                    
                    excel_data = excel_buffer.getvalue()

                    st.download_button(
                        label="⬇️ Download Full Excel Report (QC & Master Data)",
                        data=excel_data,
                        file_name="invoice_automation_master_report.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
                else:
                    st.warning("No line items could be extracted from the provided files.")

if __name__ == "__main__":
    main()
