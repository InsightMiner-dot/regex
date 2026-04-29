import pdfplumber
import camelot
import pandas as pd
import re
import os
import glob

def extract_header_data(pdf_path):
    """Uses pdfplumber to extract the static top-level contract info via cropping."""
    headers = {
        "Contract Number": "Not Found",
        "Issue Date": "Not Found",
        "Seller": "Not Found",
        "Buyer": "Not Found"
    }
    
    with pdfplumber.open(pdf_path) as pdf:
        first_page = pdf.pages[0]
        
        # 1. Standard extraction for single-line items at the very top
        text_standard = first_page.extract_text()
        if not text_standard:
            return headers

        date_match = re.search(r'Issue\s*Date[^:]*:\s*(\d{1,2}-[A-Za-z]{3}-\d{4})', text_standard, re.IGNORECASE)
        if date_match:
            headers["Issue Date"] = date_match.group(1)

        contract_match = re.search(r'Contract\s*Number[^:]*:\s*([A-Za-z0-9\-]+)', text_standard, re.IGNORECASE)
        if contract_match:
            headers["Contract Number"] = contract_match.group(1).strip()

        # 2. CROP THE PAGE FOR SELLER AND BUYER
        # Bounding box coordinates: (x0, top, x1, bottom)
        width = first_page.width
        height = first_page.height
        
        # Crop Top-Left Quadrant (Assuming Seller is on the left)
        left_bbox = (0, 0, width * 0.5, height * 0.5)
        left_side = first_page.crop(left_bbox)
        seller_text = left_side.extract_text()
        
        # Crop Top-Right Quadrant (Assuming Buyer is on the right)
        right_bbox = (width * 0.5, 0, width, height * 0.5)
        right_side = first_page.crop(right_bbox)
        buyer_text = right_side.extract_text()

        # 3. Clean Regex on the isolated text
        if seller_text:
            # Look for "Seller Name:", capture everything until a blank line or end of text
            seller_match = re.search(r'Seller\s*Name[^:]*:\s*(.*?)(?=\n\s*\n|$)', seller_text, re.IGNORECASE | re.DOTALL)
            if seller_match:
                # Replace newlines with commas for clean Excel formatting
                seller = seller_match.group(1).strip().replace('\n', ', ')
                headers["Seller"] = re.sub(r'\s{2,}', ' ', seller)

        if buyer_text:
            # Look for "Buyer Name:", capture everything until a blank line or end of text
            buyer_match = re.search(r'Buyer\s*Name[^:]*:\s*(.*?)(?=\n\s*\n|$)', buyer_text, re.IGNORECASE | re.DOTALL)
            if buyer_match:
                buyer = buyer_match.group(1).strip().replace('\n', ', ')
                headers["Buyer"] = re.sub(r'\s{2,}', ' ', buyer)

    return headers

def extract_full_contract_data(pdf_path):
    """Combines header data and table line items."""
    filename = os.path.basename(pdf_path) 
    
    # 1. Get the static header data
    header_data = extract_header_data(pdf_path)
    
    # 2. Extract tables using Camelot (powered by OpenCV)
    tables = camelot.read_pdf(pdf_path, pages='all', flavor='lattice')
    
    if not tables:
        # Fallback if lattice finds no hard lines
        tables = camelot.read_pdf(pdf_path, pages='all', flavor='stream')
        
    if not tables:
        print(f"   -> No tables found in {filename}")
        return pd.DataFrame()

    all_table_data = []
    
    for i, table in enumerate(tables):
        df = table.df
        
        # Clean up column headers
        df.columns = df.iloc[0].str.replace('\n', ' ').str.strip()
        df = df[1:].reset_index(drop=True)
        
        # Filter out empty rows
        df = df.replace('', pd.NA).dropna(how='all')
        
        # Check if this table actually contains our line items
        has_part_column = any('part' in str(col).lower() for col in df.columns)
        
        if has_part_column:
            all_table_data.append(df)

    if not all_table_data:
        return pd.DataFrame()

    # Combine all pages of tables into one dataframe
    line_items_df = pd.concat(all_table_data, ignore_index=True)
    
    # 3. Merge Header Data into the Line Items table
    line_items_df.insert(0, "file_name", filename)
    line_items_df.insert(1, "Contract Number", header_data["Contract Number"])
    line_items_df.insert(2, "Issue Date", header_data["Issue Date"])
    line_items_df.insert(3, "Seller", header_data["Seller"])
    line_items_df.insert(4, "Buyer", header_data["Buyer"])

    return line_items_df

def process_folder_of_pdfs(folder_path):
    """Iterates through a directory, extracts data from all PDFs, and returns a master table."""
    all_dataframes = []
    search_pattern = os.path.join(folder_path, "*.pdf")
    pdf_files = glob.glob(search_pattern)
    
    if not pdf_files:
        print(f"No PDFs found in the folder: {folder_path}")
        return pd.DataFrame()
        
    print(f"Found {len(pdf_files)} PDFs. Starting extraction...\n")

    for file_path in pdf_files:
        print(f"Processing: {os.path.basename(file_path)}")
        try:
            df = extract_full_contract_data(file_path)
            if not df.empty:
                all_dataframes.append(df)
            else:
                print(f"   -> Could not extract structured line items from {os.path.basename(file_path)}")
        except Exception as e:
            print(f"   -> Error processing {os.path.basename(file_path)}: {e}")

    if all_dataframes:
        master_df = pd.concat(all_dataframes, ignore_index=True)
        return master_df
    else:
        return pd.DataFrame()

if __name__ == "__main__":
    # Define the folder containing your PDFs
    my_pdf_folder = './' 
    
    # Run the extraction
    final_master_table = process_folder_of_pdfs(my_pdf_folder)
    
    # Save the results to Excel
    if not final_master_table.empty:
        final_master_table.to_excel('extracted_contracts.xlsx', index=False)
        print("\nExtraction complete! Saved to extracted_contracts.xlsx")
    else:
        print("\nFailed to extract usable data.")
