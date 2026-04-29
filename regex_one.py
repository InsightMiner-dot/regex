import pdfplumber
import camelot
import pandas as pd
import re
import os
import glob

# =====================================================================
# CONFIGURATION
# =====================================================================
# List exactly the columns you want from the line-item table. 
# Anything Camelot extracts that is NOT in this list will be ignored.
WANTED_COLUMNS = [
    "Part Number", 
    "Base Material", 
    "UOM", 
    "Base Price", 
    "Total Price"
]

# =====================================================================
# EXTRACTION FUNCTIONS
# =====================================================================
def extract_header_data(pdf_path):
    """Uses pdfplumber and visual cropping to extract the top-level contract info."""
    headers = {
        "Contract Number": "Not Found",
        "Issue Date": "Not Found",
        "Seller": "Not Found",
        "Buyer": "Not Found"
    }
    
    with pdfplumber.open(pdf_path) as pdf:
        first_page = pdf.pages[0]
        text_standard = first_page.extract_text()
        
        if not text_standard: 
            return headers

        # 1. Single-line extractions (Regex)
        date_match = re.search(r'Issue\s*Date[^:]*:\s*(\d{1,2}-[A-Za-z]{3}-\d{4})', text_standard, re.IGNORECASE)
        if date_match: 
            headers["Issue Date"] = date_match.group(1)

        contract_match = re.search(r'Contract\s*Number[^:]*:\s*([A-Za-z0-9\-]+)', text_standard, re.IGNORECASE)
        if contract_match: 
            headers["Contract Number"] = contract_match.group(1).strip()

        # 2. Bounding Box Cropping for side-by-side addresses
        w, h = first_page.width, first_page.height
        
        # Crop Left Side (Top 40%) for Seller
        left_side = first_page.crop((0, 0, w * 0.5, h * 0.4))
        seller_text = left_side.extract_text()
        
        if seller_text:
            m = re.search(r'Seller\s*Name[^:]*:\s*(.*?)(?=\n\s*\n|$)', seller_text, re.IGNORECASE | re.DOTALL)
            if m: 
                # Replace newlines with commas, remove extra spaces
                seller_clean = m.group(1).strip().replace('\n', ', ')
                headers["Seller"] = re.sub(r'\s{2,}', ' ', seller_clean)

        # Crop Right Side (Top 40%) for Buyer
        right_side = first_page.crop((w * 0.5, 0, w, h * 0.4))
        buyer_text = right_side.extract_text()
        
        if buyer_text:
            m = re.search(r'Buyer\s*Name[^:]*:\s*(.*?)(?=\n\s*\n|$)', buyer_text, re.IGNORECASE | re.DOTALL)
            if m: 
                buyer_clean = m.group(1).strip().replace('\n', ', ')
                headers["Buyer"] = re.sub(r'\s{2,}', ' ', buyer_clean)

    return headers

def extract_full_contract_data(pdf_path):
    """Combines header data and filtered table line items."""
    filename = os.path.basename(pdf_path) 
    
    # 1. Get the static header data
    header_data = extract_header_data(pdf_path)
    
    # 2. Extract tables using Camelot (stream flavor is best for semi-structured)
    tables = camelot.read_pdf(pdf_path, pages='all', flavor='stream')
    
    all_table_data = []
    
    for table in tables:
        df = table.df
        
        # Clean column headers
        df.columns = df.iloc[0].str.replace('\n', ' ').str.strip()
        df = df[1:].reset_index(drop=True)
        
        # 3. Apply the Column Filter (Drop the "Noise")
        # Identify which of the extracted columns match our WANTED_COLUMNS list
        existing_wanted_cols = [c for c in df.columns if any(wanted.lower() in str(c).lower() for wanted in WANTED_COLUMNS)]
        
        if existing_wanted_cols:
            # Create a new dataframe with ONLY the columns we care about
            df_filtered = df[existing_wanted_cols].copy()
            
            # 4. Clean Empty Rows
            # Find the actual name of the Part Number column in this dataframe
            part_col = next((c for c in df_filtered.columns if 'part' in c.lower()), None)
            
            if part_col:
                # Remove rows where the Part Number is blank
                df_filtered = df_filtered[df_filtered[part_col].astype(str).str.strip() != '']
                
                # If there's still data left after filtering out blank rows, add it to our list
                if not df_filtered.empty:
                    all_table_data.append(df_filtered)

    # If no valid tables were found, return an empty dataframe
    if not all_table_data:
        return pd.DataFrame()

    # Combine all valid table parts into one dataframe
    line_items_df = pd.concat(all_table_data, ignore_index=True)
    
    # 5. Merge Header Data into the Line Items table
    line_items_df.insert(0, "file_name", filename)
    line_items_df.insert(1, "Contract Number", header_data["Contract Number"])
    line_items_df.insert(2, "Issue Date", header_data["Issue Date"])
    line_items_df.insert(3, "Seller", header_data["Seller"])
    line_items_df.insert(4, "Buyer", header_data["Buyer"])

    return line_items_df

def process_folder(folder_path):
    """Iterates through a directory, extracts data, and saves to Excel."""
    all_dfs = []
    search_pattern = os.path.join(folder_path, "*.pdf")
    pdf_files = glob.glob(search_pattern)
    
    if not pdf_files:
        print(f"No PDFs found in the directory: {folder_path}")
        return
        
    print(f"Found {len(pdf_files)} PDFs. Starting extraction...\n")

    for f in pdf_files:
        print(f"Processing: {os.path.basename(f)}")
        try:
            df = extract_full_contract_data(f)
            if not df.empty: 
                all_dfs.append(df)
            else:
                print(f"   -> No valid line items found in {os.path.basename(f)}")
        except Exception as e: 
            print(f"   -> Error in {os.path.basename(f)}: {e}")
    
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        # Save to Excel
        final_df.to_excel("Clean_Extracted_Data.xlsx", index=False)
        print("\nSuccess! All data compiled and saved to Clean_Extracted_Data.xlsx")
    else:
        print("\nExtraction finished, but no valid data was found to save.")

# =====================================================================
# RUN THE SCRIPT
# =====================================================================
if __name__ == "__main__":
    # Define the folder containing your PDFs. 
    # './' means the current folder where the python script is saved.
    my_pdf_folder = './' 
    
    # Run the processor
    process_folder(my_pdf_folder)
