import fitz  # PyMuPDF
import pytesseract
from PIL import Image
import json
import io
import cv2
import os
import re
import numpy as np
from collections import defaultdict
from .google_drive import authenticate_gdrive, get_specific_file, download_file_from_drive

# Path to Tesseract executable
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

def preprocess_image(img):
    open_cv_image = np.array(img)
    open_cv_image = open_cv_image[:, :, ::-1].copy()  # Convert RGB to BGR
    gray = cv2.cvtColor(open_cv_image, cv2.COLOR_BGR2GRAY)
    adaptive_thresh = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
                                           cv2.THRESH_BINARY, 11, 2)
    denoised = cv2.fastNlMeansDenoising(adaptive_thresh, None, 30, 7, 21)
    return Image.fromarray(denoised)

def sanitize_filename(filename):
    """Removes invalid characters from filenames."""
    return re.sub(r'[<>:"/\\|?*]', '_', filename)
    
def extract_matching_values_with_positions(pdf_path, input_json):
    # Google Drive file download setup
    service = authenticate_gdrive()
    parts = pdf_path.split(os.sep)
    supplier_domain = ''
    supplier_name = ''
    basic_dn = ''
    filename = ''
    if len(parts) == 4:
        supplier_domain, supplier_name, basic_dn, filename = parts
    
    file_info = get_specific_file(service, supplier_domain, supplier_name, basic_dn, filename)
    DOWNLOAD_FOLDER = "ocr_downloads/"
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
    download_file_from_drive(file_info["id"], file_info["name"], DOWNLOAD_FOLDER)
    
    local_path = os.path.join(DOWNLOAD_FOLDER, sanitize_filename(filename))
    if not os.path.exists(local_path):
        return {"data": [{"matches": [], "pdf_path": local_path}]}
    
    doc = fitz.open(local_path)
    all_matches = []
    num_items = len(input_json)
    
    # First pass: find all matches in document
    matches_dict = defaultdict(list)
    
    for page_num in range(len(doc)):
        page = doc.load_page(page_num)
        pix = page.get_pixmap(matrix=fitz.Matrix(4, 4))
        img = Image.open(io.BytesIO(pix.tobytes()))
        img = preprocess_image(img)
        
        # Get OCR data
        ocr_data = pytesseract.image_to_data(
            img, 
            output_type=pytesseract.Output.DICT,
            config='--psm 6'
        )
        
        # Process OCR results
        for i in range(len(ocr_data['text'])):
            text = ocr_data['text'][i].strip()
            if text:
                for item in input_json:
                    for key, target in item.items():
                        if target and str(target) == text:
                            matches_dict[(key, str(target))].append({
                                "x": ocr_data['left'][i],
                                "y": ocr_data['top'][i],
                                "width": ocr_data['width'][i],
                                "height": ocr_data['height'][i],
                                "page": page_num + 1,
                                "page_width": pix.width,
                                "page_height": pix.height
                            })
    
    # Second pass: create output matches with correct count per key
    for item_idx, item in enumerate(input_json):
        for key, target in item.items():
            if not target:
                continue
                
            target_str = str(target)
            matches = matches_dict.get((key, target_str), [])
            
            # If no matches found, create a dummy entry
            if not matches:
                all_matches.append({
                    "index": str(item_idx + 1),
                    "key": key,
                    "value": target_str,
                    "x": 0,
                    "y": 0,
                    "width": 0,
                    "height": 0,
                    "page": 1,
                    "page_width": 0,
                    "page_height": 0
                })
            else:
                # Use the first match or cycle through available matches
                match_idx = item_idx % len(matches)
                match = matches[match_idx]
                all_matches.append({
                    "index": str(item_idx + 1),
                    "key": key,
                    "value": target_str,
                    "x": match["x"],
                    "y": match["y"],
                    "width": match["width"],
                    "height": match["height"],
                    "page": match["page"],
                    "page_width": match["page_width"],
                    "page_height": match["page_height"]
                })
    
    return {
        "data": [{
            "matches": all_matches,
            "pdf_path": local_path
        }]
    }