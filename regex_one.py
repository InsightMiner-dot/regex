import pdfplumber
import camelot
import pandas as pd
import re
import os
import glob

# --- CONFIGURATION: LIST ONLY THE COLUMNS YOU WANT TO KEEP ---
# This ignores "noise" like 'Remarks', 'Total Weight', etc.
WANTED_COLUMNS = [
    "Part Number", 
    "Base Material", 
    "UOM", 
    "Base Price", 
    "Total Price"
]

def extract_header_data(pdf_path):
    headers = {"Contract Number": "Not Found", "Issue Date": "Not Found", "Seller": "Not Found", "Buyer": "Not Found"}
    with pdfplumber.open(pdf_path) as pdf:
        first_page = pdf.pages[0]
        text_standard = first_page.extract_text()
        if not text_standard: return headers

        # Regex for single line fields
        date_match = re.search(r'Issue\s*Date[^:]*:\s*(\d{1,2}-[A-Za-z]{3}-\d{4})', text_standard, re.IGNORECASE)
        if date_match: headers["Issue Date"] = date_match.group(1)

        contract_match = re.search(r'Contract\s*Number[^:]*:\s*([A-Za-z0-9\-]+)', text_standard, re.IGNORECASE)
        if contract_match: headers["Contract Number"] = contract_match.group(1).strip()

        # Crop logic to separate Seller (Left) and Buyer (Right)
        w, h = first_page.width, first_page.height
        
        # Left Side (Seller)
        left_side = first_page.crop((0, 0, w * 0.5, h * 0.4))
        seller_text = left_side.extract_text()
        if seller_text:
            m = re.search(r'Seller\s*Name[^:]*:\s*(.*?)(?=\n\s*\n|$)', seller_text, re.IGNORECASE | re.DOTALL)
            if m: headers["Seller"] = re.sub(r'\s{2,}', ' ', m.group(1).strip().replace('\n', ', '))

        # Right Side (Buyer)
        right_side = first_page.crop((w * 0.5, 0, w, h * 0.4))
        buyer_text = right_side.extract_text()
        if buyer_text:
            m = re.search(r'Buyer\s*Name[^:]*:\s*(.*?)(?=\n\s*\n|$)', buyer_text, re.IGNORECASE | re.DOTALL)
            if m: headers["Buyer"] = re.sub(r'\s{2,}', ' ', m.group(1).strip().replace('\n', ', '))

    return headers

def extract_full_contract_data(pdf_path):
    filename = os.path.basename(pdf_path) 
    header_data = extract_header_data(pdf_path)
    
    # Extract tables - 'stream' is often better if 'lattice' creates too many empty columns
    tables = camelot.read_pdf(pdf_path, pages='all', flavor='stream')
    
    all_table_data = []
    for table in tables:
        df = table.df
        # Clean headers: remove newlines and extra spaces
        df.columns = df.iloc[0].str.replace('\n', ' ').str.strip()
        df = df[1:].reset_index(drop=True)
        
        # 1. IDENTIFY RELEVANT COLUMNS
        # We find which of our 'WANTED_COLUMNS' actually exist in this PDF table
        existing_wanted_cols = [c for c in df.columns if any(wanted.lower() in str(c).lower() for wanted in WANTED_COLUMNS)]
        
        # 2. FILTER THE DATAFRAME
        if existing_wanted_cols:
            df_filtered = df[existing_wanted_cols].copy()
            
            # Remove rows that are completely empty in the 'Part Number' column
            # (Adjust 'Part Number' to the exact name found in your PDF if different)
            part_col = next((c for c in df_filtered.columns if 'part' in c.lower()), None)
            if part_col:
                df_filtered = df_filtered[df_filtered[part_col].astype(str).str.strip() != '']
                all_table_data.append(df_filtered)

    if not all_table_data:
        return pd.DataFrame()

    line_items_df = pd.concat(all_table_data, ignore_index=True)
    
    # Insert Header Data
    line_items_df.insert(0, "file_name", filename)
    line_items_df.insert(1, "Contract Number", header_data["Contract Number"])
    line_items_df.insert(2, "Issue Date", header_data["Issue Date"])
    line_items_df.insert(3, "Seller", header_data["Seller"])
    line_items_df.insert(4, "Buyer", header_data["Buyer"])

    return line_items_df

def process_folder(folder_path):
    all_dfs = []
    for f in glob.glob(os.path.join(folder_path, "*.pdf")):
        print(f"Processing: {os.path.basename(f)}")
        try:
            df = extract_full_contract_data(f)
            if not df.empty: all_dfs.append(df)
        except Exception as e: print(f"Error in {f}: {e}")
    
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df.to_excel("Clean_Extracted_Data.xlsx", index=False)
        print("Done! Data saved to Clean_Extracted_Data.xlsx")
    else:
        print("No data extracted.")

# RUN
process_folder('./')
