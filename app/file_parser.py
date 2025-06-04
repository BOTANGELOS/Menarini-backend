
import pytesseract
from PyPDF2 import PdfReader
from PIL import Image
from docx import Document
import pandas as pd
import os
from email.header import decode_header 
import re

def parse_pdf(filepath):
    reader = PdfReader(filepath)
    return "\n".join(page.extract_text() for page in reader.pages)

# def parse_image(filepath):
#     return pytesseract.image_to_string(Image.open(filepath))

# def parse_doc(filepath):
#     doc = Document(filepath)
#     return "\n".join(p.text for p in doc.paragraphs)

def parse_excel(filepath):
    data = pd.read_excel(filepath)
    return data.to_dict(orient="records")

def parse_attachment(filepath):
    ext = filepath.split('.')[-1]
    if ext == "pdf":
        return parse_pdf(filepath)
    # elif ext in ["jpg", "jpeg", "png"]:
    #     return parse_image(filepath)
    # elif ext == "docx":
    #     return parse_doc(filepath)
    elif ext in ["xlsx", "xls"]:
        return parse_excel(filepath)
    else:
        raise ValueError("Unsupported file format.")
import os
import re
from email.header import decode_header

import os
import re
from email.header import decode_header

def save_attachment(part, download_folder):
    """
    Saves an email attachment to the specified download folder.
    
    Args:
        part: The part of the email containing the attachment.
        download_folder: The folder where attachments should be saved.
        
    Returns:
        The file path of the saved attachment.
    """
    # Get the filename from the attachment part
    filename = part.get_filename()

    # Decode the filename if needed
    if filename:
        filename = decode_header(filename)[0][0]
        if isinstance(filename, bytes):
            filename = filename.decode(errors='ignore')

        # Remove any hidden newline characters (\r, \n)
        filename = filename.replace("\r", "").replace("\n", "")

        # Sanitize the filename by replacing invalid characters
        filename = re.sub(r'[\/:*?"<>|]+', '_', filename)

    else:
        return None  # If there's no filename, return None

    # Ensure the download folder exists
    os.makedirs(download_folder, exist_ok=True)

    # Save the file
    filepath = os.path.join(download_folder, filename)
    with open(filepath, "wb") as f:
        f.write(part.get_payload(decode=True))
    
    return filepath
