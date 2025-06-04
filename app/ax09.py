from flask import Blueprint, request, jsonify
import mysql.connector
import os
import re
import ast
from datetime import datetime
import json
import xml.etree.ElementTree as ET
from dateparser import parse

ax_header1 = """
              <RecordSet>
                  <BUSINESS_UNIT_ID>S304</BUSINESS_UNIT_ID>
                  <PO_NUMBER>PO1-005763</PO_NUMBER>
                  <ERP_VENDOR_ID>VTAMEN0007</ERP_VENDOR_ID>
                  <VENDOR_SITE_ID>XX</VENDOR_SITE_ID>
                  <VENDOR_SITE_CODE>XX</VENDOR_SITE_CODE>
                  <PO_STATUS>Open order</PO_STATUS>
                  <CURRENCY_CODE>EUR</CURRENCY_CODE>
                  <PO_REQUESTOR_EMAIL>Sherry.Zhang@menariniapac.com</PO_REQUESTOR_EMAIL>
                  <RETURN_MESSAGE></RETURN_MESSAGE>
              </RecordSet>
              """
ax_header_fail = """<RecordSet>
    <BUSINESS_UNIT_ID>S304</BUSINESS_UNIT_ID>
    <PO_NUMBER>PO22-00067</PO_NUMBER>
    <ERP_VENDOR_ID> </ERP_VENDOR_ID>
    <VENDOR_SITE_ID></VENDOR_SITE_ID>
    <VENDOR_SITE_CODE></VENDOR_SITE_CODE>
    <PO_STATUS></PO_STATUS>
    <CURRENCY_CODE></CURRENCY_CODE>
    <PO_REQUESTOR_EMAIL> </PO_REQUESTOR_EMAIL>
    <RETURN_MESSAGE>Error: PO number PO1-006794 is invalid in Business unit Id S304</RETURN_MESSAGE>
</RecordSet>
"""

ax_body1 = """<RecordSet>
    <lines>
        <line>
            <BUSINESS_UNIT_ID>S304</BUSINESS_UNIT_ID>
            <PO_NUMBER>PO1-006452</PO_NUMBER>
            <LINE_NUMBER>1.0</LINE_NUMBER>
            <MATERIAL_NO>1300001</MATERIAL_NO>
            <MATERIAL_GROUP>SERVICES</MATERIAL_GROUP>
            <DESCRIPTION>PREPAID INSURANCE</DESCRIPTION>
            <PO_QUANTITY>2000</PO_QUANTITY>
            <UOM>ea</UOM>
            <UNIT_PRICE>100.0</UNIT_PRICE>
            <PRICE_UNIT>1.0</PRICE_UNIT>
            <PO_TOTAL>100.00</PO_TOTAL>
            <TAX_CODE>GST</TAX_CODE>
            <TAX_JUR_CODE>8S</TAX_JUR_CODE>
            <ITEM_CATEGORY>1300001</ITEM_CATEGORY>
            <PLANT>XX</PLANT>
            <PUOM>ea</PUOM>
            <EXTERNAL_ MATERIAL_NO>ABC</EXTERNAL_ MATERIAL_NO>
         </line>
        <line>
            <BUSINESS_UNIT_ID>S304</BUSINESS_UNIT_ID>
            <PO_NUMBER>PO1-006452</PO_NUMBER>
            <LINE_NUMBER>2.0</LINE_NUMBER>
            <MATERIAL_NO>1200009</MATERIAL_NO>
            <MATERIAL_GROUP>SERVICES</MATERIAL_GROUP>
            <DESCRIPTION>PASS THROUGHS - I/C</DESCRIPTION>
            <PO_QUANTITY>1300</PO_QUANTITY>
            <UOM>ea</UOM>
            <UNIT_PRICE>100.0</UNIT_PRICE>
            <PRICE_UNIT>1.0</PRICE_UNIT>
            <PO_TOTAL>100.00</PO_TOTAL>
            <TAX_CODE>GST</TAX_CODE>
            <TAX_JUR_CODE>8S</TAX_JUR_CODE>
            <ITEM_CATEGORY>1200009</ITEM_CATEGORY>
            <PLANT>XX</PLANT>
            <PUOM>ea</PUOM>
            <EXTERNAL_ MATERIAL_NO>DEF</EXTERNAL_ MATERIAL_NO>
          </line>
      </lines>
    <RETURN_MESSAGE></RETURN_MESSAGE>
</RecordSet>
"""


ax_body2 = """
<RecordSet>
    <lines>
        <line>
            <BUSINESS_UNIT_ID>S304</BUSINESS_UNIT_ID>
            <PO_NUMBER>PO1-005763</PO_NUMBER>
            <LINE_NUMBER>1.0</LINE_NUMBER>
            <MATERIAL_NO>1300001</MATERIAL_NO>
            <MATERIAL_GROUP>SERVICES</MATERIAL_GROUP>
            <DESCRIPTION>PREPAID INSURANCE</DESCRIPTION>
            <PO_QUANTITY>4200</PO_QUANTITY>
            <UOM>ea</UOM>
            <UNIT_PRICE>120.0</UNIT_PRICE>
            <PRICE_UNIT>1.0</PRICE_UNIT>
            <PO_TOTAL>100.00</PO_TOTAL>
            <TAX_CODE>GST</TAX_CODE>
            <TAX_JUR_CODE>8S</TAX_JUR_CODE>
            <ITEM_CATEGORY>1300001</ITEM_CATEGORY>
            <PLANT>XX</PLANT>
            <PUOM>ea</PUOM>
            <EXTERNAL_ MATERIAL_NO>ABC</EXTERNAL_ MATERIAL_NO>
         </line>
      </lines>
    <RETURN_MESSAGE></RETURN_MESSAGE>
</RecordSet>
"""
ax_body_fail = """<RecordSet>
    <lines />
    <RETURN_MESSAGE> Error: PO number P01-001004 is invalid in Business unit Id S302 </RETURN_MESSAGE>
</RecordSet>
"""
ax_bp = Blueprint('ax_bp', __name__)

_original_connect = mysql.connector.connect
def fixed_connect(*args, **kwargs):
    """Automatically add use_pure=True to all mysql connections"""
    if 'use_pure' not in kwargs:
        kwargs['use_pure'] = True
    return _original_connect(*args, **kwargs)

# Replace the original connect function globally
mysql.connector.connect = fixed_connect
def get_db_connection():
    return mysql.connector.connect(
        host='mysql-server-test.mysql.database.azure.com',
        user='menarini',
        password='menarini@2025',  # no password
        database='menarini-backend'
    )
def parse_recordsets_to_json(xml_string: str) -> list:
    """
    Parse an XML string containing multiple <RecordSet> elements and return a list of JSON-compatible dicts.
    Automatically fix invalid XML tag names (e.g., with spaces).
    """
    # Replace invalid tag names like <EXTERNAL_ MATERIAL_NO> with <EXTERNAL_MATERIAL_NO>
    xml_string = re.sub(r'<(/?)EXTERNAL_ MATERIAL_NO>', r'<\1EXTERNAL_MATERIAL_NO>', xml_string)

    # Wrap in root to parse multiple top-level RecordSet elements
    root = ET.fromstring(f"<root>{xml_string}</root>")
    result = []

    for record in root.findall('RecordSet'):
        record_dict = {}

        # Handle <lines> block
        lines = record.find('lines')
        if lines is not None:
            record_dict['lines'] = []
            for line in lines.findall('line'):
                line_data = {
                    elem.tag.strip(): (elem.text.strip() if elem.text else "")
                    for elem in line
                }
                record_dict['lines'].append(line_data)

        # Handle direct children (excluding <lines>)
        for child in record:
            if child.tag != 'lines':
                record_dict[child.tag.strip()] = child.text.strip() if child.text else ""

        result.append(record_dict)

    return result

def extract_number(quantity_str: str) -> float:
    """
    Extracts a number from a quantity string like '19,002 kg' or '19002'.
    Handles commas and optional units.
    """
    # Remove non-numeric and non-comma/period characters
    cleaned = re.sub(r'[^\d,\.]', '', quantity_str)

    # Remove thousands separators (commas), keep decimal points
    cleaned = cleaned.replace(',', '')

    try:
        return float(cleaned)
    except ValueError:
        return 0.0  # fallback if parsing fails

def get_data_from_ax09(domain,po):
    if po =="PO1-005763":
       return parse_recordsets_to_json(ax_body2)
    elif po == "PO1-006452":
       return parse_recordsets_to_json(ax_body1)
    else:
      return parse_recordsets_to_json(ax_body_fail)
def get_header_from_ax09(domain,po):
  if po == "PO1-005763" or po=="PO1-006452":
     return parse_recordsets_to_json(ax_header1)
  else:
     return parse_recordsets_to_json(ax_header_fail)
  
# def normalize_date(date_str: str, date_format: str) -> str:
#     """
#     Convert various messy date formats into 'dd/mm/yyyy', based on known date_format ('ddmmyyyy' or 'mmddyyyy').
#     If day is missing, default to 1.
#     """
#     if not date_str or date_str.strip().upper() in {"NONE", "NOT SPECIFIED", "NOT APPLICATED","N/A"}:
#         return ""
    
#     # Normalize separators and uppercase months
#     date_str = date_str.upper().replace('.', '/').replace('-', '/').replace(' ', '/')
#     parts = [p for p in date_str.split('/') if p]

#     # Map month name to number
#     month_map = {
#         'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
#         'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
#         'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'
#     }

#     # Try to convert textual month
#     parts = [month_map.get(p[:3], p) for p in parts]

#     # Pad with default values if needed
#     if len(parts) == 2:
#         parts.insert(0, '1')  # Add day = 1
#     elif len(parts) == 1:
#         raise ValueError(f"Cannot parse date: {date_str}")

#     # Reorder based on input format
#     if date_format == 'mmddyyyy':
#         mm, dd, yyyy = parts
#     elif date_format == 'ddmmyyyy':
#         dd, mm, yyyy = parts
#     else:
#         raise ValueError("date_format must be 'mmddyyyy' or 'ddmmyyyy'")

#     # Normalize and format to dd/mm/yyyy
#     try:
#         date_obj = datetime(int(yyyy), int(mm), int(dd))
#         return date_obj.strftime('%d/%m/%Y')
#     except ValueError:
#         raise ValueError(f"Invalid date values in: {date_str}")
    

def normalize_date(date_input) -> str:
    """
    Normalize various date inputs (string or datetime) into 'dd/mm/yyyy' format.
    If day is missing, it defaults to the 1st.
    """
    if isinstance(date_input, datetime):
        return date_input.strftime('%d/%m/%Y')

    if not date_input or str(date_input).strip().upper() in {"NONE", "NOT SPECIFIED", "NOT APPLICATED", "N/A"}:
        return ""

    parsed_date = parse(str(date_input), settings={'PREFER_DAY_OF_MONTH': 'first'})

    if not parsed_date:
        raise ValueError(f"Cannot parse date: {date_input}")

    return parsed_date.strftime('%d/%m/%Y')


def get_ocr_result(dn,po):
  conn = get_db_connection()
  cursor = conn.cursor()
  dn_data = []
  inv_data = []
  coa_data = []
  bol_data = []
  po_percept = 0
  incoterms = ""
  # date_format = ""
  cursor.execute("SELECT `Incoterms` FROM dn_table WHERE `DN#`=%s",(dn,))
  result = cursor.fetchone()
  if result:
     incoterms = result[0]
  
  # cursor.execute("SELECT `date_format` FROM attachment_table WHERE `DN#`=%s",(dn,))
  # result = cursor.fetchone()
  # if result:
  #    date_format = result[0]

  cursor.execute("SELECT `PO#` FROM dn_table WHERE `DN#` = %s",(dn,))
  results = cursor.fetchall()
  cursor.execute("SELECT `Item Code`,`Packing Slip#`,`Quantity`,`Batch#`,`Manufacturing Date`,`Expiry Date`,`Document Date`,`Incoterms`,`Item Description`,`percent` FROM dn_table WHERE `DN#` = %s AND `PO#` = %s",(dn,po,))
  results = cursor.fetchall()
  if results:
     for result in results:
        entry = {
           "Item Code":result[0],
           "Packing Slip#":result[1],
          #  "Quantity":extract_number(result[2]),
           "Quantity":result[2],
           "Batch#":result[3],
           "Manufacturing Date":result[4],
           "Expiry Date":result[5],
           "Document Date":result[6],
           "Incoterms":result[7],
           "Item Description":result[8],
        }
        if po_percept == 0:
          po_percept = result[9]
        dn_data.append(entry)


  cursor.execute("SELECT `Packing Slip#`,`Quantity`,`Batch#`,`Manufacturing Date`,`Item Code`,`Expiry Date`,`Document Date`,`INV NO#`,`Incoterms`,`Item Description` FROM inv_table WHERE `DN#` = %s AND `PO#` = %s",(dn,po,))
  results = cursor.fetchall()
  if results:
     for result in results:
        entry = {
           "Packing Slip#":result[0],
           "Quantity":extract_number(result[1]),
           "Batch#":result[2],
           "Manufacturing Date":result[3],
           "Item Code":result[4],
           "Expiry Date":result[5],
           "Document Date":result[6],
           "INV NO#":result[7],
           "Incoterms":result[8],
           "Item Description":result[9],
        }
        inv_data.append(entry)
  
  cursor.execute("SELECT `Item Description`,`Manufacturing Date`,`Expiry Date`,`Batch#` FROM coa_table WHERE `DN#` = %s AND `flag`=%s",(dn,1,))
  results = cursor.fetchall()
  if results:
     for result in results:
        entry = {
           "Item Description":result[0],
           "Manufacturing Date":result[1],
           "Expiry Date":result[2],
           "Batch#":result[3]
        }
        coa_data.append(entry)


  cursor.execute("SELECT `created_at` FROM blawb_table WHERE `DN#` = %s",(dn,))
  results = cursor.fetchall()
  if results:
     for result in results:
        entry = {
          #  "Incoterms":result[0],
          "Incoterms":"",
          "Posting Date":result[0],
        }
        bol_data.append(entry)

  final_result = []
  inv_index = 0
  for (index,data) in enumerate(dn_data):
    customer_part_code = data["Item Code"]
    packing_list = data["Packing Slip#"]
    quantity = data["Quantity"]
    batch = data["Batch#"]
    
    coa_manu = ""
    coa_expiry = ""
    for coaData in coa_data:
      if coaData["Batch#"] == batch:
        coa_manu = coaData["Manufacturing Date"]
        coa_expiry = coaData["Expiry Date"]
        break
        
    manufacturing_date = coa_manu
    expiry_date = coa_expiry
    
    document_date = data["Document Date"]
    item_description = data["Item Description"]
    inv_number = inv_data[inv_index]["INV NO#"]
    posting_date = bol_data[0]["Posting Date"]

    if customer_part_code == "N/A" or customer_part_code == "Null" or customer_part_code == "None" or customer_part_code == "null" or not customer_part_code:
      customer_part_code = inv_data[inv_index]["Item Code"]
    if packing_list == "N/A" or packing_list == "Null" or packing_list == "None" or packing_list == "null" or not packing_list:
      packing_list = inv_data[inv_index]["Packing Slip#"]
    if quantity == "N/A" or quantity == "Null" or quantity == "None" or quantity == "null" or not quantity:
      quantity = inv_data[inv_index]["Quantity"]
    if batch == "N/A" or batch == "Null" or batch == "None" or batch == "null" or not batch:
      batch = inv_data[inv_index]["Batch#"]
    if manufacturing_date == "N/A" or manufacturing_date == "Null" or manufacturing_date == "None" or manufacturing_date == "null" or not manufacturing_date:
      manufacturing_date = data["Manufacturing Date"]
    if expiry_date == "N/A" or expiry_date == "Null" or expiry_date == "None" or expiry_date == "null" or not expiry_date:
      expiry_date = data["Expiry Date"]
    if document_date == "N/A" or document_date == "Null" or document_date == "None" or document_date == "null" or not document_date:
      document_date = inv_data[inv_index]["Document Date"]
    if item_description == "N/A" or item_description == "Null" or item_description == "None" or item_description == "null" or not item_description:
      item_description = inv_data[inv_index]["Item Description"]


    if manufacturing_date == "N/A" or manufacturing_date == "Null" or manufacturing_date == "None" or manufacturing_date == "null" or not manufacturing_date:
      manufacturing_date = inv_data[inv_index]["Manufacturing Date"]
    if expiry_date == "N/A" or expiry_date == "Null" or expiry_date == "None" or expiry_date == "null" or not expiry_date:
      manufacturing_date = inv_data[inv_index]["Manufacturing Date"]
    
    # manufacturing_date = normalize_date(manufacturing_date,date_format)
    # expiry_date = normalize_date(expiry_date,date_format)
    # document_date = normalize_date(document_date,date_format)
    # posting_date = normalize_date(posting_date,date_format)

    # if incoterms == "exWorks" or incoterms == "FCA" or incoterms == "EXW":
    #   posting_date = normalize_date(data["Document Date"],date_format)
    # elif incoterms == "CIP" or incoterms == "CIF" or incoterms == "CFR" or incoterms == "CPT":
    #   posting_date = normalize_date(posting_date,date_format)
    # elif incoterms == "DAP" or incoterms == "DPU" or incoterms == "DDP":
    #   posting_date = normalize_date(posting_date,date_format)
    # else:
    #   posting_date = normalize_date(inv_data[inv_index]["Document Date"],date_format)

    manufacturing_date = normalize_date(manufacturing_date)
    expiry_date = normalize_date(expiry_date)
    document_date = normalize_date(document_date)
    posting_date = normalize_date(posting_date)

    if incoterms == "exWorks" or incoterms == "FCA" or incoterms == "EXW":
      posting_date = normalize_date(data["Document Date"])
    elif incoterms == "CIP" or incoterms == "CIF" or incoterms == "CFR" or incoterms == "CPT":
      posting_date = normalize_date(posting_date)
    elif incoterms == "DAP" or incoterms == "DPU" or incoterms == "DDP":
      posting_date = normalize_date(posting_date)
    else:
      posting_date = normalize_date(inv_data[inv_index]["Document Date"])

    entry = {
      "Item Code":customer_part_code,
      "Item Name":item_description,
      "Packing Slip No":packing_list,
      "Invoice No":inv_number,
      "Posting Date":posting_date,
      "Shipped Quantity":quantity,
      "Batch Number":batch,
      "Manufacturing Date":manufacturing_date,
      "Expiry Date":expiry_date,
      "Incoterm":incoterms,
      "Document Date":document_date
    }
    final_result.append(entry)
    if inv_index < len(inv_data)-1:
      inv_index = inv_index + 1

  print("------------------------------")
  return final_result

@ax_bp.route('/get_po_data', methods=['POST'])
def get_po_data():
  data = request.get_json()
  dn = data.get("DN#")
  po = data.get("PO#")
  
  conn = get_db_connection()
  cursor = conn.cursor()
  cursor.execute("SELECT `vendor_id` FROM dn_table WHERE `DN#` = %s",(dn,))
  result = cursor.fetchone()
  supplier_domain = ""
  supplier_name = ""
  date = ""
  if result:
    cursor.execute("SELECT `business_unit_id`,`vendor_name` FROM vendor_master WHERE `id` = %s",(result[0],))
    result = cursor.fetchone()
    supplier_domain = result[0]
    supplier_name = result[1]
    ax09_header = get_header_from_ax09(supplier_domain,po)
    ax09_result = get_data_from_ax09(supplier_domain,po)
    ocr_result = get_ocr_result(dn,po)
    
    

    ax09_header_result = []
    ax09_body_result = []
    if ax09_header[0]["RETURN_MESSAGE"] == "":
      ax09_header_result = ax09_header[0]
    else:
      return []
    
    if ax09_result[0]["RETURN_MESSAGE"] == "":
      ax09_body_result = ax09_result[0]["lines"]
    else:
      return []

  final_result = []
  if len(ax09_body_result) == 1 and len(ocr_result) == 1:
    entry = ocr_result[0].copy()
    entry["Data Area Id"] = supplier_domain
    entry["PO Number"] = po
    entry["PO Line Number"] = ax09_body_result[0]["LINE_NUMBER"]
    entry["Open PO Quantity"] = ax09_body_result[0]["PO_QUANTITY"]
    entry["Unit Price"] = ax09_body_result[0]["UNIT_PRICE"]
    entry["Vendor Code"] = ax09_header_result["ERP_VENDOR_ID"]
    entry["Vendor Name"] = supplier_name
    entry["No of Item code in PO"] = 1
    entry["Prepared By"] = ax09_header_result["PO_REQUESTOR_EMAIL"]
    final_result.append(entry)
  elif len(ax09_body_result) >=2 and len(ocr_result) == 1:
    # print(ax09_body_result)
    shipped_quantity = ocr_result[0]["Shipped Quantity"]
    # print(shipped_quantity)
    for index,ax_data in enumerate(ax09_body_result):
      entry = ocr_result[0].copy()
      ax_data["PO_QUANTITY"] = extract_number(ax_data["PO_QUANTITY"])
      entry["Data Area Id"] = supplier_domain
      entry["PO Number"] = po
      entry["PO Line Number"] = ax_data["LINE_NUMBER"]
      entry["Unit Price"] = ax_data["UNIT_PRICE"]
      entry["Vendor Code"] = ax09_header_result["ERP_VENDOR_ID"]
      entry["Vendor Name"] = supplier_name
      entry["No of Item code in PO"] = len(ax09_body_result)
      entry["Prepared By"] = ax09_header_result["PO_REQUESTOR_EMAIL"]
      entry["Open PO Quantity"] = ax_data["PO_QUANTITY"]

      if shipped_quantity - ax_data["PO_QUANTITY"] >0:
        if index >= len(ax09_body_result)-1:
          entry["Shipped Quantity"] = shipped_quantity
        else:
          entry["Shipped Quantity"] = ax_data["PO_QUANTITY"]
        shipped_quantity = shipped_quantity - ax_data["PO_QUANTITY"]
      else:
        entry["Shipped Quantity"] = shipped_quantity
        shipped_quantity = 0
      final_result.append(entry)
  
  elif len(ax09_body_result) ==1 and len(ocr_result) >= 2:
    ax_data = ax09_body_result[0]
    ordered_quantity = ax_data["PO_QUANTITY"](ax09_body_result[0]["PO_QUANTITY"])
    for index,ocr_data in enumerate(ocr_result):
      entry = ocr_data.copy()
      entry["Data Area Id"] = supplier_domain
      entry["PO Number"] = po
      entry["PO Line Number"] = ax_data["LINE_NUMBER"]
      entry["Unit Price"] = ax_data["UNIT_PRICE"]
      entry["Vendor Code"] = ax09_header_result["ERP_VENDOR_ID"]
      entry["Vendor Name"] = supplier_name
      entry["No of Item code in PO"] = len(ocr_result)
      entry["Prepared By"] = ax09_header_result["PO_REQUESTOR_EMAIL"]

      if ordered_quantity - entry["Shipped Quantity"] >= 0:
        entry["Open PO Quantity"] = entry["Shipped Quantity"]
        ordered_quantity = ordered_quantity - entry["Shipped Quantity"]
      elif ordered_quantity < 0:
        entry["Open PO Quantity"] = ordered_quantity
        ordered_quantity = 0
      final_result.append(entry)
      
  cursor.close()
  conn.close()
  print(final_result)
  return final_result



@ax_bp.route('/get_percent', methods=['POST'])
def get_percent():
  data = request.get_json()
  dn = data.get("DN#")
  po = data.get("PO#")
  
  conn = get_db_connection()
  cursor = conn.cursor()
  cursor.execute("SELECT `percent` FROM dn_table WHERE `DN#` = %s AND `PO#` = %s",(dn,po,))
  result = cursor.fetchone()
  percent = 0.1
  if result:
     if result[0]:
       percent = result[0]
  cursor.close()
  conn.close()
  
  return jsonify({"percent": percent})

@ax_bp.route('/update_percent', methods=['POST'])
def update_percent():
    data = request.get_json()
    dn = data.get("DN#")
    po = data.get("PO#")
    
    try:
        percent = float(data.get("percent", 0)) / 100
    except ValueError:
        return jsonify({"error": "Invalid percent value"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE dn_table SET `percent` = %s WHERE `DN#` = %s AND `PO#` = %s", (percent, dn, po))
    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({"success": True})


@ax_bp.route('/dn_table_data', methods=['POST'])
def dn_table_data():
  data = request.get_json()
  dn = data.get("DN#")
  po = data.get("PO#")

  date_format = ""
  conn = get_db_connection()
  cursor = conn.cursor()

  # cursor.execute("SELECT `date_format` FROM attachment_table WHERE `DN#`=%s",(dn,))
  # result = cursor.fetchone()
  # if result:
  #    date_format = result[0]

  cursor.execute("SELECT `Item Code`,`Packing Slip#`,`Quantity`,`Batch#`,`Manufacturing Date`,`Expiry Date`,`Document Date`,`Incoterms`,`Item Description`,`id` FROM dn_table WHERE `DN#` = %s AND `PO#` = %s",(dn,po,))
  results = cursor.fetchall()
  final_result = []
  if results:
     for result in results:
        entry = {
          "Item Code":result[0],
          "Packing Slip#":result[1],
          'Quantity':result[2],
          "Batch#":result[3],
          # "Manufacturing Date":normalize_date(result[4],date_format),
          # "Expiry Date":normalize_date(result[5],date_format),
          # "Document Date":normalize_date(result[6],date_format),
          "Manufacturing Date":normalize_date(result[4]),
          "Expiry Date":normalize_date(result[5]),
          "Document Date":normalize_date(result[6]),
          "Incoterms":result[7],
          "Item Description":result[8],
          "id":result[9],
        }
        final_result.append(entry)
  cursor.close()
  conn.close()
  print(final_result)
  return final_result


@ax_bp.route('/inv_table_data', methods=['POST'])
def inv_table_data():
  data = request.get_json()
  dn = data.get("DN#")
  po = data.get("PO#")
  conn = get_db_connection()
  cursor = conn.cursor()
  
  # date_format = ""
  # cursor.execute("SELECT `date_format` FROM attachment_table WHERE `DN#`=%s",(dn,))
  # result = cursor.fetchone()
  # if result:
  #    date_format = result[0]

  cursor.execute("SELECT `Packing Slip#`,`Quantity`,`Batch#`,`Manufacturing Date`,`Item Code`,`Expiry Date`,`Document Date`,`INV NO#`,`Incoterms`,`Item Description`,`id` FROM inv_table WHERE `DN#` = %s AND `PO#` = %s",(dn,po,))
  results = cursor.fetchall()
  final_result = []
  if results:
     for result in results:
        entry = {
          "Packing Slip#":result[0],
          "Quantity":result[1],
          "Batch#":result[2],
          "Manufacturing Date":normalize_date(result[3]),
          "Item Code":result[4],
          "Expiry Date":normalize_date(result[5]),
          "Document Date":normalize_date(result[6]),
          "INV NO#":result[7],
          "Incoterms":result[8],
          "Item Description":result[9],
          'id':result[10],
        }
        final_result.append(entry)
  cursor.close()
  conn.close()
  
  print(final_result)
  return final_result


@ax_bp.route('/coa_table_data', methods=['POST'])
def coa_table_data():
  data = request.get_json()
  dn = data.get("DN#")
  po = data.get("PO#")
  conn = get_db_connection()
  cursor = conn.cursor()

  
  # date_format = ""
  # cursor.execute("SELECT `date_format` FROM attachment_table WHERE `DN#`=%s",(dn,))
  # result = cursor.fetchone()
  # if result:
  #    date_format = result[0]

  cursor.execute("SELECT `Item Description`,`Manufacturing Date`,`Expiry Date`,`id`,`Batch#` FROM coa_table WHERE `DN#` = %s AND `flag`=%s",(dn,1,))
  results = cursor.fetchall()
  final_result = []
  if results:
     for result in results:
        entry = {
          "Item Description":result[0],
          "Manufacturing Date":normalize_date(result[1]),
          "Expiry Date":normalize_date(result[2]),
          "id":result[3],
          "Batch#":result[4],
        }
        final_result.append(entry)
  cursor.close()
  conn.close()
  
  print(final_result)
  return final_result



@ax_bp.route('/bol_table_data', methods=['POST'])
def bol_table_data():
  data = request.get_json()
  dn = data.get("DN#")
  conn = get_db_connection()
  cursor = conn.cursor()

  
  # date_format = ""
  # cursor.execute("SELECT `date_format` FROM attachment_table WHERE `DN#`=%s",(dn,))
  # result = cursor.fetchone()
  # if result:
  #    date_format = result[0]

  cursor.execute("SELECT `Posting Date`,`id` FROM bol_table WHERE `DN#` = %s",(dn,))
  results = cursor.fetchall()
  final_result = []
  if results:
     for result in results:
        entry = {
          "Incoterms":"",
          "Posting Date":normalize_date(result[0]),
          "id":result[1]
        }
        final_result.append(entry)
  cursor.close()
  conn.close()
  
  print(final_result)
  return final_result




# @ax_bp.route('/get_status', methods=['POST'])
# def get_status():
#   data = request.get_json()
#   dn = data.get("DN#")
#   po = data.get("PO#")
#   conn = get_db_connection()
#   cursor = conn.cursor()

#   date_format = ""
#   incoterms = ""
#   item_count = 0
#   coa_count = 0
#   cursor.execute("SELECT `date_format`,`Incoterms`,`Item Count`,`COA Count` FROM dn_table WHERE `DN#`=%s",(dn,))
#   result = cursor.fetchone()
#   error_message = ""
#   if result:
#     date_format = result[0]
#     incoterms = result[1]
#     item_count = result[2]
#     coa_count = result[3]
  
#   print("----------")
#   print(item_count)
#   print(coa_count)
#   if date_format == "":
#      error_message = "Not identity Date Format."
#   if incoterms == "":
#      error_message = "Not identity Incoterm."
#   if item_count > coa_count:
#      error_message = "Not enough COA."

#   cursor.close()
#   conn.close()
  
#   return error_message


@ax_bp.route('/get_status', methods=['POST'])
def get_status():
    data = request.get_json()
    dn = data.get("DN#")
    po = data.get("PO#")

    conn = get_db_connection()
    cursor = conn.cursor()

    incoterms = ""
    item_count = 0
    coa_count = 0
    document_date = ""

    # Query to fetch incoterms and document date from dn_table
    cursor.execute("""
        SELECT `Incoterms`
        FROM dn_table
        WHERE `DN#` = %s
        LIMIT 1
    """, (dn,))
    
    result = cursor.fetchone()
    if result:
        incoterms = result[0]
    # Query to count unique Item Code entries from dn_table
    cursor.execute("""
        SELECT COUNT(DISTINCT `Item Code`)
        FROM dn_table
        WHERE `DN#` = %s
    """, (dn,))
    item_count = cursor.fetchone()[0]

    # Query to count COA rows from coa_table
    cursor.execute("""
        SELECT COUNT(*)
        FROM coa_table
        WHERE `DN#` = %s
    """, (dn,))
    coa_count = cursor.fetchone()[0]
    error_message = ""
    if incoterms == "":
      error_message = "Not identity Incoterm."
    if item_count > coa_count:
      error_message = "Not enough COA."

    cursor.close()
    conn.close()

    return error_message


# Data Area Id
# PO Number
# PO Line Number
# Item Code
# Item Name
# Open PO Quantity
# Unit Price
# Packing Slip No
# Invoice No
# Posting Date
# Shipped Quantity
# Batch Number
# Manufacturing Date
# Expiry Date
# Vendor Code
# Vendor Name
# No of Item code in PO
# Incoterm
# Document Date
# Prepared By