import pdfplumber
import pandas as pd
import re
import os
import glob

def extract_master_contract_data(pdf_path):
    filename = os.path.basename(pdf_path)
    
    # Set default values in case something is missing from the document
    contract_no = "Not Found"
    issue_date = "Not Found"
    seller = "Not Found"
    buyer = "Not Found"
    part_no = "Not Found"

    # We will accumulate all text across all pages here
    full_text = ""
    left_text_all = ""
    right_text_all = ""

    # --- STEP 1: READ ALL PAGES AND SPLIT FOR ADDRESSES ---
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # 1a. Grab standard full-page text
            text = page.extract_text()
            if text:
                full_text += text + "\n"

            # 1b. Split the page exactly in half to protect the Seller/Buyer boxes
            page_width = page.width
            page_height = page.height
            
            left_side = page.crop((0, 0, page_width / 2, page_height))
            right_side = page.crop((page_width / 2, 0, page_width, page_height))
            
            l_text = left_side.extract_text()
            r_text = right_side.extract_text()
            
            if l_text: left_text_all += l_text + "\n"
            if r_text: right_text_all += r_text + "\n"

    # --- STEP 2: EXTRACT GLOBAL DOCUMENT HEADERS ---
    
    # Issue Date
    date_match = re.search(r'Issue\s*Date[^:]*:\s*(\d{1,2}-[A-Za-z]{3}-\d{4})', full_text, re.IGNORECASE)
    if date_match:
        issue_date = date_match.group(1)

    # Contract Number
    contract_match = re.search(r'Contract\s*Number[^:]*:\s*([A-Za-z0-9\-]+)', full_text, re.IGNORECASE)
    if contract_match:
        contract_no = contract_match.group(1).strip()

    # Part Number
    part_match = re.search(r'Part\s*Number[^:]*:\s*([A-Za-z0-9\-]+)', full_text, re.IGNORECASE)
    if part_match:
        part_no = part_match.group(1).strip()

    # Seller Address (Only looks at the left half of the document)
    seller_match = re.search(r'Seller\s*Name[^:]*:\s*(.*?)(?=\n\s*\n|Contract|Part|$)', left_text_all, re.IGNORECASE | re.DOTALL)
    if seller_match:
        seller = seller_match.group(1).strip().replace('\n', ', ')
        seller = re.sub(r'\s{2,}', ' ', seller) # Clean up messy spacing

    # Buyer Address (Only looks at the right half of the document)
    buyer_match = re.search(r'Buyer\s*Name[^:]*:\s*(.*?)(?=\n\s*\n|Contract|Part|$)', right_text_all, re.IGNORECASE | re.DOTALL)
    if buyer_match:
        buyer = buyer_match.group(1).strip().replace('\n', ', ')
        buyer = re.sub(r'\s{2,}', ' ', buyer)

    # --- STEP 3: EXTRACT DYNAMIC BASE MATERIALS ---
    materials_list = []
    
    # Split the document at "Base Material" and only look at the text below it
    sections = re.split(r'Base\s*Material[^:]*:', full_text, flags=re.IGNORECASE)
    
    if len(sections) > 1:
        base_material_text = sections[1]
        
        # The Sequence Regex to capture the cascading data points
        sequence_pattern = r'([A-Za-z0-9#\s\-]+?)\s+([YyNn])\s+([A-Za-z\s]+?)\s+([\d.]+)\s+(USD/\s*KILOGRAM|[^0-9\n]+)\s+([\d.]+)\s+(GRAM|[^0-9\n]+)\s+([\d.]+)\s+(USD/\s*KILOGRAM|[^0-9\n]+)\s+([\d.]+)'
        
        for match in re.finditer(sequence_pattern, base_material_text):
            clean_material_name = re.sub(r'\s+', ' ', match.group(1).strip())
            
            materials_list.append({
                "Base Material": clean_material_name,
                "Auto Update": match.group(2).strip(),
                "Exchange": match.group(3).strip(),
                "Exchange Value": match.group(4).strip(),
                "Mass": match.group(6).strip(),
                "Cost": match.group(8).strip(),
                "Final Value": match.group(10).strip()
            })

    # --- STEP 4: FLATTEN AND ASSEMBLE THE ROWS ---
    extracted_rows = []

    # If we found Base Materials, generate a row for EVERY material, repeating the header data
    if materials_list:
        for mat in materials_list:
            row_data = {
                "file_name": filename,
                "Contract Number": contract_no,
                "Issue Date": issue_date,
                "Seller": seller,
                "Buyer": buyer,
                "Part Number": part_no,
                **mat # This unpacks all the material data directly into the row
            }
            extracted_rows.append(row_data)
    else:
        # If no Base Materials were found at all, still generate one row so we don't lose the document
        extracted_rows.append({
            "file_name": filename,
            "Contract Number": contract_no,
            "Issue Date": issue_date,
            "Seller": seller,
            "Buyer": buyer,
            "Part Number": part_no,
            "Base Material": "Not Found",
            "Auto Update": "", "Exchange": "", "Exchange Value": "",
            "Mass": "", "Cost": "", "Final Value": ""
        })

    return pd.DataFrame(extracted_rows)

# --- BATCH PROCESSOR ---
def process_folder_of_pdfs(folder_path):
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
            df = extract_master_contract_data(file_path)
            if not df.empty:
                all_dataframes.append(df)
        except Exception as e:
            print(f"   -> Error processing {os.path.basename(file_path)}: {e}")

    # Combine all DataFrames and enforce column order
    if all_dataframes:
        master_df = pd.concat(all_dataframes, ignore_index=True)
        
        column_order = [
            "file_name", "Contract Number", "Issue Date", "Seller", "Buyer", "Part Number",
            "Base Material", "Auto Update", "Exchange", "Exchange Value", "Mass", "Cost", "Final Value"
        ]
        master_df = master_df[column_order]
        return master_df
    else:
        return pd.DataFrame()

# --- HOW TO RUN IT ---
if __name__ == "__main__":
    # 1. Point this to your folder containing the PDFs
    my_pdf_folder = './my_contract_documents' 
    
    # 2. Run the processor
    final_master_table = process_folder_of_pdfs(my_pdf_folder)
    
    # 3. View the results
    print("\n--- FINAL EXTRACTED DATA ---")
    print(final_master_table.to_string())
    
    # 4. Save directly to Excel (Uncomment the line below when you are ready)
    # final_master_table.to_excel('Final_Extracted_Contracts.xlsx', index=False)
