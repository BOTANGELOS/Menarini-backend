from flask import Blueprint, jsonify
import mysql.connector
import os
import re
import ast
from datetime import datetime
from pathlib import Path
from .google_drive import google_drive_supplier_name_change
import os
import re
import ast
from datetime import datetime
import json
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime

ax_header1_test = """
              <RecordSet>
                  <BUSINESS_UNIT_ID>S304</BUSINESS_UNIT_ID>
                  <PO_NUMBER>PO1-005763</PO_NUMBER>
                  <ERP_VENDOR_ID>VTAMEN0007</ERP_VENDOR_ID>
                  <VENDOR_SITE_ID>XX</VENDOR_SITE_ID>
                  <VENDOR_SITE_CODE>XX</VENDOR_SITE_CODE>
                  <PO_STATUS>Open order</PO_STATUS>
                  <CURRENCY_CODE>EUR</CURRENCY_CODE>
                  <PO_REQUESTOR_EMAIL>lan.huynh@menariniapac.com</PO_REQUESTOR_EMAIL>
                  <RETURN_MESSAGE></RETURN_MESSAGE>
              </RecordSet>
              """

ax_header_fail_test = """<RecordSet>
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

ax_body1_test = """
<RecordSet>
    <lines>
        <line>
            <BUSINESS_UNIT_ID>S304</BUSINESS_UNIT_ID>
            <PO_NUMBER>PO1-005763</PO_NUMBER>
            <LINE_NUMBER>1.0</LINE_NUMBER>
            <MATERIAL_NO>1300001</MATERIAL_NO>
            <MATERIAL_GROUP>SERVICES</MATERIAL_GROUP>
            <DESCRIPTION>PREPAID INSURANCE</DESCRIPTION>
            <PO_QUANTITY>60000</PO_QUANTITY>
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


ax_body_fail_test = """<RecordSet>
    <lines />
    <RETURN_MESSAGE> Error: PO number P01-001004 is invalid in Business unit Id S302 </RETURN_MESSAGE>
</RecordSet>
"""

def parse_recordsets_to_json_test(xml_string: str) -> list:
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

def get_data_from_ax09_test(domain,po):
    if po == "PO1-005763":
       return parse_recordsets_to_json_test(ax_body1_test)
    else:
      return parse_recordsets_to_json_test(ax_body_fail_test)

def get_header_from_ax09_test(domain,po):
  if po=="PO1-005985":
      return parse_recordsets_to_json_test(ax_header1_test)
  else:
     return parse_recordsets_to_json_test(ax_header_fail_test)

def get_po_quantity_test(domain, po_number):
    # Call the function to get data
    data = get_data_from_ax09_test(domain, po_number)
    
    # Check if data exists and has lines
    if not data or len(data) == 0:
        print(f"No data found for PO: {po_number}")
        return None
    
    record = data[0]  # Get first record
    
    # Check for error message
    if record.get('RETURN_MESSAGE'):
        print(f"Error: {record['RETURN_MESSAGE']}")
        return None
    
    # Extract PO number, line number, and quantities from all lines
    if 'lines' in record and record['lines']:
        po_details = []
        for line in record['lines']:
            po_num = line.get('PO_NUMBER', po_number)
            line_num = line.get('LINE_NUMBER', '0')
            po_quantity = line.get('PO_QUANTITY', '0')
            item_no = line.get('MATERIAL_NO','')
            
            po_details.append({
                'po_number': po_num,
                'line_number': line_num,
                'quantity': int(po_quantity) if po_quantity.isdigit() else 0,
                'item_no':item_no
            })
        return po_details
    else:
        print(f"No line items found for PO: {po_number}")
        return None


def complete_flag(dn):
    
  conn = get_db_connection()
  cursor = conn.cursor()
  query = """SELECT `complete` FROM attachment_table WHERE `DN#` = %s;""" 
  cursor.execute(query, (dn,))
  results = cursor.fetchone()
  if results:
    results = results[0]
  cursor.close()
  conn.close()
  
  return results

def get_db_connection():
    return mysql.connector.connect(
        host='mysql-server-test.mysql.database.azure.com',
        user='menarini',
        password='menarini@2025',  # no password
        database='menarini-backend'
    )
    
def get_all_emailId():
  conn = get_db_connection()
  cursor = conn.cursor()
  
  cursor.execute("SELECT `email_id`, `DN#` FROM email_check")  # adjust column/table name as needed
  results = cursor.fetchall()
  
  cursor.close()
  conn.close()
  
  email_dn_check = {}
  for row in results:
      email_dn_check[row[0]] = row[1]
  return email_dn_check

def get_email_data_with_role(role,email):
  conn = get_db_connection()
  cursor = conn.cursor()
  
  total = []
  results = []
  if role == 1:    
    cursor.execute("SELECT `email_id`,`DN#`,`subject`,`body`,`attachments`,`sender`,`date` FROM email_check")
    results = cursor.fetchall()
  else:
    cursor.execute("SELECT `email_id`,`DN#`,`subject`,`body`,`attachments`,`sender`,`date` FROM email_check WHERE admin_email = %s", (email,))
    results = cursor.fetchall()
  if results:
    for result in results:
      if result:
        email_id = result[0]
        dn = result[1]
        subject = result[2]
        body = result[3]
        raw_attachments = result[4]  # e.g., your weird character list
        joined = ''.join(raw_attachments)  # Join into one string
        print(joined)
        attachments_list = ast.literal_eval(joined)  # Parse as actual list
        attachments = [os.path.basename(a) for a in attachments_list]
        raw_sender = result[5]
        match = re.search(r'<(.+?)>', raw_sender)
        sender = match.group(1) if match else raw_sender
        date = result[6]
        excerpt = body[:30] + '...' if len(body) > 30 else body
        # if dn:
        entry =  {"id":email_id,"subject":subject,"sender":sender,"body":body,"attachments":attachments, "date":date,"excerpt":excerpt, "status":complete_flag(dn)}
        total.append(entry)
  return total

def get_all_email(email):
  conn = get_db_connection()
  cursor = conn.cursor()
  cursor.execute("SELECT `admin_email` FROM user WHERE email = %s", (email,))
  emails = cursor.fetchone()
  admin_email = emails[0]
  cursor.execute("SELECT `role` FROM admin_users WHERE email = %s", (admin_email,))
  result = cursor.fetchone()
  if result:
    if result[0] == 1 or result[0] == '1':
      result = get_email_data_with_role(1,admin_email)
    else:
      result = get_email_data_with_role(2,admin_email)
  
  return result
  

def get_gmail_password(email):
  conn = get_db_connection()
  cursor = conn.cursor()
  
  cursor.execute("SELECT `gmail_password` FROM user WHERE `email` = %s",(email,))  # adjust column/table name as needed
  result = cursor.fetchone()[0]
  
  cursor.close()
  conn.close()
  
  return result

def get_dn_from_emailID(emailID):
  conn = get_db_connection()
  cursor = conn.cursor()
  query = """SELECT `DN#` FROM email_check WHERE `email_id` = %s;""" 
  cursor.execute(query, (emailID,))
  results = cursor.fetchone()[0]
  
  cursor.close()
  conn.close()
  
  return results

def get_attachment_list_from_dn(dn):

  conn = get_db_connection()
  cursor = conn.cursor()
  query = """SELECT `DN`, `INV`, `Bill of Lading`, `Air Waybill`, `COA` FROM attachment_table WHERE `DN#` = %s;"""
  cursor.execute(query, (dn,))
  dn,inv,bill_of_lading,air_waybill,coa = cursor.fetchone()
  
  cursor.close()
  conn.close()
  return dn,inv,bill_of_lading,air_waybill,coa

def get_attachment_list_from_email(emailID):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """SELECT `DN`, `INV`, `Bill of Lading`, `Air Waybill`, `COA` FROM email_attachment WHERE `email_id` = %s;"""
    cursor.execute(query, (emailID,))
    result = cursor.fetchone()

    cursor.close()
    conn.close()

    if result:
        return result  # returns (dn, inv, bill_of_lading, air_waybill, coa)
    else:
        return None  # or return default values like (0, 0, 0, 0, 0)

def get_email_error(emailID):
  conn = get_db_connection()
  cursor = conn.cursor()
  print(emailID)
  if emailID:
    print('inisde if', emailID)
    query = """SELECT `error`, `type` FROM email_error_table WHERE `email_id` = %s;""" 
    cursor.execute(query, (emailID,))
  else:
    print('inisde else')
    query = """SELECT `error`, `type`, `DN#`, `PO#`, `supplier_name`, `compliance_error` FROM email_error_table;"""
    cursor.execute(query)
  results = cursor.fetchall()
  json_result = []
  for result in results:
    if emailID:
       new_entry = {
        "error":result[0],
        "error_type" : result[1],
      }
    else:
      new_entry = {
        "error":result[0],
        "error_type" : result[1],
        "DN#":result[2],
        "PO#":result[3],
        "supplier_name":result[4],
        "compliance_error":result[5]
      }
    json_result.append(new_entry)
  cursor.close()
  conn.close()
  return json_result

# Get all logs (for the new testing page)
from email.utils import parsedate_to_datetime
from datetime import datetime

def get_all_logs(email):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT `admin_email` FROM user WHERE email = %s", (email,))
    emails = cursor.fetchone()
    admin_email = emails[0]

    cursor.execute("""
        SELECT `id`, `event_datetime`, `event_type`, `event_key`, `event_message`, `dn_num`, `sender` 
        FROM event_log 
        ORDER BY `id` DESC
    """)

    final = cursor.fetchall()
    final_data = []

    for data in final:
        raw_date = data[1]
        formatted_date = ""

        try:
            if isinstance(raw_date, str):
                parsed_date = parsedate_to_datetime(raw_date)
                if parsed_date:
                    formatted_date = parsed_date.strftime("%d/%m/%Y")
            elif isinstance(raw_date, datetime):
                formatted_date = raw_date.strftime("%d/%m/%Y")
        except Exception as e:
            print(f"Date parse error: {e} | Raw date: {repr(raw_date)}")

        entry = {
            "id": data[0],
            "datetime": raw_date,
            "formatted_date": formatted_date,
            "type": data[2],
            "key": data[3],
            "message": data[4],
            "dn#": data[5],
            "email": data[6]
        }
        final_data.append(entry)

    return final_data


def get_document_error_with_email(emailID):
  conn = get_db_connection()
  cursor = conn.cursor()
  query = """SELECT `message`, `type` FROM error_table WHERE `email_id` = %s;""" 
  cursor.execute(query, (emailID,))
  results = cursor.fetchall()
  json_result = []
  for result in results:
    new_entry = {
      "error":result[0],
      "error_type" : result[1]
    }
    json_result.append(new_entry)
  cursor.close()
  conn.close()
  return json_result


def get_document_error_with_id(log_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """SELECT `id`, `event_datetime`, `event_type`, `event_key`, `event_message`, `dn_num`, `sender` 
               FROM event_log 
               WHERE id = %s 
               ORDER BY id DESC"""
    cursor.execute(query, (log_id,))
    results = cursor.fetchall()

    json_result = []
    for data in results:
        raw_date = data[1]
        formatted_date = ""

        try:
            if isinstance(raw_date, str):
                # Try email.utils first
                parsed_date = parsedate_to_datetime(raw_date)
                if parsed_date:
                    formatted_date = parsed_date.strftime("%d/%m/%Y")
            elif isinstance(raw_date, datetime):
                formatted_date = raw_date.strftime("%d/%m/%Y")
        except Exception as e:
            print(f"Date parse error: {e} | Raw date: {repr(raw_date)}")

        entry = {
            "id": data[0],
            "datetime": raw_date,
            "formatted_date": formatted_date,
            "type": data[2],
            "key": data[3],
            "message": data[4],
            "dn#": data[5],
            "email": data[6]
        }
        json_result.append(entry)

    cursor.close()
    conn.close()
    return json_result



def get_threshold_status(dn_number, threshold_percent=10):
    try:
        # Get DN details from database
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        query = "SELECT `DN#`, `Item Code`, `PO#`, `Batch#`, `Quantity` FROM dn_table WHERE `DN#` = %s"
        cursor.execute(query, (dn_number,))
        dn_details = cursor.fetchall()
        
        if not dn_details:
            return {"error": f"No data found for DN: {dn_number}"}
        
        comparison_results = []
        
        for dn_item in dn_details:
            po_number = dn_item.get('PO#')
            dn_quantity = int(dn_item.get('Quantity', 0))
            
            # Get PO details from AX09
            po_details = get_po_quantity_test("", po_number)
            
            if not po_details:
                comparison_results.append({
                    'error': f"Could not retrieve PO details for PO: {po_number}"
                })
                continue
            
            for po_line in po_details:
                po_quantity = po_line['quantity']
                line_number = po_line['line_number']
                
                if po_quantity <= 0:
                    continue
                
                # Calculate balance
                balance_to_fulfill = dn_quantity - po_quantity
                deviation = abs(dn_quantity - po_quantity) / po_quantity * 100
                
                # Determine status
                status = "Within Threshold" if deviation <= threshold_percent else "Exceeds Threshold"
                
                result = {
                    'po_line_number': line_number,
                    'po_number': po_number,
                    'ordered_quantity': po_quantity,
                    'shipped_quantity': dn_quantity,
                    'balance_to_be_fulfilled': balance_to_fulfill,
                    'threshold_percent': threshold_percent,
                    'deviation_percent': round(deviation, 2),
                    'status': status
                }
                
                comparison_results.append(result)
        
        return comparison_results

    except mysql.connector.Error as error:
        return {"error": f"Database error: {error}"}
    except Exception as e:
        return {"error": f"Unexpected error: {str(e)}"}
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def get_th_first_table(dn_number):
    try:
        # Get DN details from the database
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        query = "SELECT `DN#`, `Item Code`, `PO#`, `Batch#`, `Quantity` FROM dn_table WHERE `DN#` = %s"
        cursor.execute(query, (dn_number,))
        dn_details = cursor.fetchall()

        if not dn_details:
            return {"error": f"No data found for DN: {dn_number}"}

        results = []

        for dn_item in dn_details:
            po_number = dn_item.get('PO#')
            item_no = dn_item.get('Item Code')

            # Get PO details from AX09
            po_details = get_po_quantity_test("", po_number)

            if not po_details:
                continue

            for po_line in po_details:
                po_quantity = po_line.get('quantity')
                line_number = po_line.get('line_number')

                results.append({
                    'item_no': item_no,
                    'po_quantity': po_quantity,
                    'po_number': po_number,
                    'po_line_number': line_number
                })

        return results

    except mysql.connector.Error as error:
        return {"error": f"Database error: {error}"}
    except Exception as e:
        return {"error": f"Unexpected error: {str(e)}"}
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def get_th_second_table(dn_number):
    try:
        # Connect to the database
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        query = "SELECT `DN#`, `Item Code`, `PO#`, `Batch#`, `Quantity` FROM dn_table WHERE `DN#` = %s"
        cursor.execute(query, (dn_number,))
        dn_details = cursor.fetchall()

        if not dn_details:
            return {"error": f"No data found for DN: {dn_number}"}

        # Convert field names to user-friendly keys
        formatted_data = []
        for row in dn_details:
            formatted_data.append({
                "dn_number": row["DN#"],
                "item_code": row["Item Code"],
                "po_number": row["PO#"],
                "batch_number": row["Batch#"],
                "quantity": row["Quantity"]
            })

        return formatted_data

    except mysql.connector.Error as error:
        return {"error": f"Database error: {error}"}
    except Exception as e:
        return {"error": f"Unexpected error: {str(e)}"}
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()


def get_threshold_from_db():
    try:
        # Get database connection
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Query to get threshold value
        query = "SELECT threshold FROM threshold_table LIMIT 1"
        cursor.execute(query)
        result = cursor.fetchone()
        
        if result:
            threshold = float(result['threshold'])
            print(f"Threshold value: {threshold}")
            return threshold
        else:
            # Return default if no record found
            return 10.0
            
    except mysql.connector.Error as error:
        print(f"Database error while fetching threshold: {error}")
        return 10.0  # Return default threshold on error
    except Exception as e:
        print(f"Unexpected error while fetching threshold: {str(e)}")
        return 10.0  # Return default threshold on error
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()


def get_multi_doc(emailID):
  conn = get_db_connection()
  cursor = conn.cursor()
  query = """SELECT `vendor_id`,`doc_list`, `DN#`,`file_name` FROM multi_doc_intervention WHERE `email_id` = %s;""" 
  cursor.execute(query, (emailID,))
  results = cursor.fetchall()
  json_result = []
  for result in results:
    new_entry = {
      "vendor_id":result[0],
      "doc_list" : result[1],
      "DN#" : result[2],
      "file_name" : result[3]
    }
    json_result.append(new_entry)
  cursor.close()
  conn.close()
  return json_result

def get_supplier_from_email(emailID):
  conn = get_db_connection()
  cursor = conn.cursor()
  
  query = """SELECT `DN#` FROM email_check WHERE `email_id` = %s;""" 
  cursor.execute(query, (emailID,))
  results = cursor.fetchone()[0]
  
  query = """SELECT `vendor_id`, `DN#` FROM supplier_name_intervention WHERE `DN#` = %s;""" 
  cursor.execute(query, (results,))
  result = cursor.fetchone()
   
  vendor_id = ""
  dn = ""
  if result:
      vendor_id = result[0] 
      dn = result[1] 
  query = """SELECT `domain`,`vendor_name` FROM supplier_table WHERE `id` = %s;""" 
  cursor.execute(query, (vendor_id,))
  result = cursor.fetchone()
  
  domain = ""
  vendor_name = ""
  if result:
    domain = result[0]
    vendor_name = result[1]
  cursor.close()
  conn.close()
  return domain, vendor_name, dn
  
  
def update_multi_doc(old_doc_list,new_doc_list, emailID, dn):
  conn = get_db_connection()
  cursor = conn.cursor()
  update_query = """UPDATE `multi_doc_intervention` SET `doc_list` = %s WHERE email_id = %s and `doc_list` = %s"""  
  cursor.execute(update_query, (new_doc_list, emailID,old_doc_list ))
  conn.commit()

  doc_types = [doc.strip() for doc in new_doc_list.split("&")]

    # Map shorthand doc types to actual column names
  column_map = {
      "DN": "DN",
      "INV": "INV",
      "COA": "COA",
      "BOL": "`Bill of Lading`",
      "AWB": "`Air Waybill`"
  }

  old_doc_types = [doc.strip() for doc in old_doc_list.split("&")]
  new_doc_types = [doc.strip() for doc in new_doc_list.split("&")]
  for doc_type in old_doc_types:
      column = column_map.get(doc_type)
      if column:
          update_query = f"UPDATE email_attachment SET {column} = %s WHERE `email_id` = %s"
          cursor.execute(update_query, (0, emailID))

# Set only the ones in new_doc_list to 1
  for doc_type in doc_types:
      column = column_map.get(doc_type)
      if column:
          update_query = f"UPDATE email_attachment SET {column} = %s WHERE `email_id` = %s"
          cursor.execute(update_query, (1, emailID))
  conn.commit()
  

  # Set old doc fields to 0
  for doc_type in old_doc_types:
      column = column_map.get(doc_type)
      if column:
          update_query = f"UPDATE attachment_table SET {column} = %s WHERE `DN#` = %s"
          cursor.execute(update_query, (0, dn))

  # Set new doc fields to 1
  for doc_type in new_doc_types:
      column = column_map.get(doc_type)
      if column:
          update_query = f"UPDATE attachment_table SET {column} = %s WHERE `DN#` = %s"
          cursor.execute(update_query, (1, dn))

  conn.commit()
  
  
  cursor.close()
  conn.close()
  return 
  
def update_supplier_name(vendor_domain, old_vendor_name, new_vendor_name,dn):
  conn = get_db_connection()
  cursor = conn.cursor()
  query = """SELECT `id` FROM supplier_table WHERE `domain` = %s AND `vendor_name` = %s"""
  cursor.execute(query, (vendor_domain,new_vendor_name, ))
  result = cursor.fetchone()

  supplier_id = ""
  if result:
    supplier_id = result[0]
  else:
    print("No supplier found for given domain and vendor name.")
    supplier_id = None
      
  
  if supplier_id:
    query = """UPDATE `attachment_table` SET `Supplier ID` = %s WHERE `DN#` = %s"""  
    cursor.execute(query, (supplier_id, dn, ))
    conn.commit()
    
    
    query = """UPDATE `multi_doc_intervention` SET `vendor_id` = %s WHERE `DN#` = %s"""  
    cursor.execute(query, (supplier_id, dn, ))
    conn.commit()
    
    query = """UPDATE `supplier_name_intervention` SET `vendor_id` = %s WHERE `DN#` = %s"""  
    cursor.execute(query, (supplier_id, dn, ))
    conn.commit()
  else:
    update_query = """UPDATE `supplier_table` SET `vendor_name` = %s WHERE `domain` = %s AND `vendor_name` = %s"""  
    cursor.execute(update_query, (new_vendor_name, vendor_domain,old_vendor_name ))
    conn.commit()

  
  cursor.close()
  conn.close()
  return 
  
def get_all_supplier_name(email):
  
  conn = get_db_connection()
  cursor = conn.cursor()
  
  query = """SELECT `DN#` FROM email_check WHERE `email_id` = %s;""" 
  cursor.execute(query, (email,))
  results = cursor.fetchone()[0]
  
  query = """SELECT `vendor_id` FROM supplier_name_intervention WHERE `DN#` = %s;""" 
  cursor.execute(query, (results,))
  result = cursor.fetchone()
   
  vendor_id = ""
  if result:
      vendor_id = result[0] 
  query = """SELECT `domain` FROM supplier_table WHERE `id` = %s;""" 
  cursor.execute(query, (vendor_id,))
  result = cursor.fetchone()
  
  vendor_domain = ""
  if result:
    vendor_domain = result[0] 
  
  # if vendor_domain == "":
  #   results = []
  #   return results
  
  cursor.execute("SELECT `vendor_name` FROM supplier_table WHERE domain = %s" , (vendor_domain,))  # adjust column/table name as needed
  results = cursor.fetchall()
  
  cursor.close()
  conn.close()
  
  return results

def new_logsheet(log_type, email,detail):
  conn = get_db_connection()
  cursor = conn.cursor()
  
  log_subject = log_type
  log_detail = ""
  email = email
  now = datetime.now()
  date = now.strftime("%Y-%m-%d %H:%M:%S")
  color = ""
  if log_type == "New DN# Case":
    log_detail = f"The new DN# - {detail} has created."
    color = "primary"
  elif log_type == "Multi Document Intervention":
    color = "warning"
    if detail:
      log_detail = "DN - " + detail[0].get("DN#") + "\n"
      for multi_doc in detail:
        old_doc_list = multi_doc.get("old_doc_list")
        new_doc_list = multi_doc.get("doc_list")
        dn = multi_doc.get("DN#")
        log_detail = log_detail + old_doc_list + "=>" + new_doc_list + "\n"
  elif log_type == "Update Supplier Name":
    color = "warning"
    old_vendor_name = detail.get("old_vendor_name", "")  # Example field
    new_vendor_name = detail.get("vendor_name", "")  # Example field
    dn = detail.get("DN#", "")  # Example field
    log_detail = "DN - " + dn +"\n" + old_vendor_name + "=>" + new_vendor_name
    
  query = """INSERT INTO logsheet (`log`,`email`,`color`,`date`,`detail`) VALUES (%s,%s,%s,%s,%s)"""  
  cursor.execute(query, (log_type, email, color,date, log_detail,))
  conn.commit()
  
  cursor.close()
  conn.close()
  return []

def get_all_data(email):
  conn = get_db_connection()
  cursor = conn.cursor()
  
  cursor.execute("SELECT `admin_email` FROM user WHERE email = %s", (email,))
  emails = cursor.fetchone()
  admin_email = emails[0]

  cursor.execute("SELECT `role` FROM admin_table WHERE email = %s", (admin_email,))
  result = cursor.fetchone()
  final = []
  if result:
    if result[0] == 1 or result[0] == '1':
      cursor.execute("SELECT `id`,`log`,`email`,`color`,`date`,`detail` FROM logsheet ORDER BY `id` DESC")
      final = cursor.fetchall()
    else:
      cursor.execute("SELECT `id`,`log`,`email`,`color`,`date`,`detail` FROM logsheet WHERE `email`=%s ORDER BY `id` DESC",(admin_email,))
      final = cursor.fetchall()
  
  final_data = []
  if final:
    for data in final:
      entry = {
        "id":data[0],
        "color":data[3],
        "title":data[1],
        "email":data[2],
        "datef":data[4],
        "detail":data[5],
        "deleted":False,
      }
      final_data.append(entry)
  print(final_data)
  return final_data
  # {
  #   id: 4,
  #   color: 'success',
  #   title:
  #     'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.',
  #   datef: '2023-06-03T23:28:56.782Z',
  #   deleted: false,
  # },
  
def get_logo_with_email(email):
  
  conn = get_db_connection()
  cursor = conn.cursor()
  
  cursor.execute("SELECT `email_id`,`logo`,`img`,`DN#` FROM logo_table WHERE `email_id`=%s",(email,))
  results = cursor.fetchall()
  print(results)
  final_result = []
  if results:
    for result in results:
      if result:
        id = result[1]
        cursor.execute("SELECT `domain`, `vendor_name` FROM supplier_table WHERE `id`=%s",(id,))
        supplier = cursor.fetchone()
        supplier_domain = ''
        supplier_name = ''
        if supplier:
          supplier_domain = supplier[0] 
          supplier_name = supplier[1]
        entry = {
          "img":result[2],
          "supplier_domain":supplier_domain,
          "supplier_name":supplier_name,
          "logo":result[1]
        }
        final_result.append(entry)
  cursor.close()
  conn.close()
  return final_result

def insert_google_drive_change(supplier_domain, supplier_name, dn):
  conn = get_db_connection()
  cursor = conn.cursor()
  
  query = """INSERT INTO google_drive_change (`supplier_domain`,`supplier_name`,`dn`) VALUES (%s,%s,%s)"""  
  cursor.execute(query, (supplier_domain,supplier_name,dn,))
  conn.commit() 
  cursor.close()
  conn.close()
  return []
def get_logo_filenames(LOGO_DIR):
    p = Path(LOGO_DIR)
    if not p.is_dir():
        return []
    return [f.name for f in p.iterdir() if f.suffix.lower() in {'.png', '.jpg', '.jpeg', '.svg', '.gif'}]
  
def get_all_logo_info():
  
  conn = get_db_connection()
  cursor = conn.cursor()
  
  BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
  LOGO_DIR = os.path.join(BASE_DIR, 'logo')
  results = get_logo_filenames(LOGO_DIR)
  final_result = []
  if results:
    for result in results:
      if result:
        id = os.path.splitext(result)[0]
        cursor.execute("SELECT `domain`, `vendor_name` FROM supplier_table WHERE `id`=%s",(id,))
        supplier = cursor.fetchone()
        supplier_domain = ''
        supplier_name = ''
        if supplier:
          supplier_domain = supplier[0] 
          supplier_name = supplier[1]
          entry = {
            "img":result,
            "supplier_domain":supplier_domain,
            "supplier_name":supplier_name,
            "logo":id
          }
          final_result.append(entry)
  cursor.close()
  conn.close()
  return final_result

  
def update_logo_info(data, email):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)  # Use dictionary cursor for named columns
        
        # 1. Fetch current logo info
        
        dn = ""
        query = "SELECT `DN#` FROM logo_table WHERE email_id = %s"
        cursor.execute(query, (email,))
        result = cursor.fetchall()
        
        old_supplier_domain = ""
        old_supplier_name = ""
        
        print(result)
        if result:
          dn = result[0]["DN#"]
          print("===================")
          query = "SELECT `Supplier ID` FROM attachment_table WHERE `DN#` = %s"
          cursor.execute(query,(result[0]["DN#"],))
          result = cursor.fetchone()
          # 
        
        print(result)
        if result:
            # 2. Fetch supplier details - MUST consume all results
            
            
            query = "SELECT `domain`, `vendor_name` FROM supplier_table WHERE `id` = %s"
            cursor.execute(query, (result["Supplier ID"],))
            domain_result = cursor.fetchone()
            # Ensure we consume any remaining results
            
            while cursor.fetchone() is not None:
                pass
            
            if domain_result:
                old_supplier_domain = domain_result['domain']
                old_supplier_name = domain_result['vendor_name']
        
        # 3. Update logo info
        
        print(old_supplier_domain)
        print(old_supplier_name)
        print(email)
        update_query = """
            UPDATE `logo_table` 
            SET `logo` = %s, `img` = %s 
            WHERE email_id = %s
        """
        cursor.execute(update_query, (data["logo"], data["img"], email))
        
        conn.commit()
        # 4. Get DN# and update related tables
        
        if dn:
            
            # Update attachment table
            update_query = """
                UPDATE `attachment_table` 
                SET `Supplier ID` = %s 
                WHERE `DN#` = %s
            """
            cursor.execute(update_query, (data["logo"], dn,))
            conn.commit()
            
            # Update intervention table
            update_query = """
                UPDATE `supplier_name_intervention` 
                SET `vendor_id` = %s 
                WHERE `DN#` = %s
            """
            cursor.execute(update_query, (data["logo"], dn,))
            conn.commit()
            
            update_query = """
                UPDATE `multi_doc_intervention` 
                SET `vendor_id` = %s 
                WHERE `DN#` = %s
            """
            cursor.execute(update_query, (data["logo"], dn,))
            conn.commit()
            
            # Get new supplier info
            query = "SELECT `domain`, `vendor_name` FROM supplier_table WHERE `id` = %s"
            cursor.execute(query, (data["logo"],))
            domain_result = cursor.fetchone()
            
            new_supplier_domain = ""
            new_supplier_name = ""
            
            if domain_result:
                new_supplier_domain = domain_result['domain']
                new_supplier_name = domain_result['vendor_name']
            
            # Log supplier change
            google_drive_supplier_name_change(old_supplier_domain, old_supplier_name,new_supplier_domain, new_supplier_name,dn)
        
        conn.commit()
        return {"status": "success", "message": "Logo info updated successfully"}
        
    except Exception as e:
        if conn:
            conn.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_all_supplier():
  conn = get_db_connection()
  cursor = conn.cursor()
  
  query = """SELECT `domain`,`vendor_name` FROM supplier_table"""  
  cursor.execute(query)
  results = cursor.fetchall()
  final_info = []
  if results:
    for result in results:
      entry = {
        "domain":result[0],
        "name":result[1]
      }
      final_info.append(entry)
  cursor.close()
  conn.close()
  return final_info

def get_supplier_id_with_vendor_name(domain,name):
  conn = get_db_connection()
  cursor = conn.cursor()
  query = """SELECT `id` FROM vendor_master WHERE `business_unit_id` = %s AND `vendor_name` = %s"""
  cursor.execute(query, (domain,name, ))
  result = cursor.fetchone()

  supplier_id = ""
  if result:
    supplier_id = result[0]
  else:
    print("No supplier found for given domain and vendor name.")
    supplier_id = None
  
  cursor.close()
  conn.close()
  
  return supplier_id


def get_supplier_domain_and_name_with_id(id):
  conn = get_db_connection()
  cursor = conn.cursor()
  vendor_domain = ""
  vendor_name = ""
  cursor = conn.cursor()
  vendor_query = """SELECT `domain`, `vendor_name` FROM supplier_table WHERE `id` = %s;"""  
  cursor.execute(vendor_query, (id,))
  vendor_result = cursor.fetchone()
  
  if vendor_result:  # Check if vendor exists
      vendor_domain, vendor_name = vendor_result
  cursor.close()
  conn.close()
  return vendor_domain, vendor_name


def get_all_notification(email):
  conn = get_db_connection()
  cursor = conn.cursor()
  
  cursor.execute("SELECT `admin_email` FROM user WHERE email = %s", (email,))
  emails = cursor.fetchone()
  admin_email = emails[0]

  cursor.execute("SELECT `role` FROM admin_table WHERE email = %s", (admin_email,))
  result = cursor.fetchone()
  final = []
  role = 1
  if result:
    if result[0] == 1 or result[0] == '1':
      role = 1
    else:
      role = 2
  cursor.execute("SELECT `id`,`header`,`message`,`key`,`type`,`date` FROM notification_table ORDER BY `id` DESC")
  final = cursor.fetchall()
  final_data = []
  print(final)
  if final:
    for data in final:
      if role == 2:
        if data[4] == 'date-format' or data[4] == "incoterms":
          query = """SELECT `id` FROM attachment_table WHERE admin_email=%s AND `DN#`=%s"""
          cursor.execute(query,(email,data[3], ))
          result = cursor.fetchone()
          if result:
            entry = {
              "_id":data[0],
              "id":data[3],
              'type':data[4],
              'header':data[1],
              'color':'error',
              'message':data[2],
              'date':data[5],
            }
            final_data.append(entry)
        else:
          query = """SELECT `id` FROM email_check WHERE admin_email=%s AND `email_id`=%s"""
          cursor.execute(query,(email,data[3], ))
          result = cursor.fetchone()
          if result:
            entry = {
              "_id":data[0],
              "id":data[3],
              'type':data[4],
              'header':data[1],
              'color':'warning',
              'message':data[2],
              'date':data[5],
            }
            final_data.append(entry)
      else:
        if data[4] == 'date-format' or data[4] == "incoterms":
          entry = {
            "_id":data[0],
            "id":data[3],
            'type':data[4],
            'header':data[1],
            'color':'error',
            'message':data[2],
            'date':data[5],
          }
          final_data.append(entry)
        else:
          entry = {
            "_id":data[0],
            "id":data[3],
            'type':data[4],
            'header':data[1],
            'color':'warning',
            'message':data[2],
            'date':data[5],
          }
          final_data.append(entry)
  print(final_data)
  return final_data

def update_notification(dn,type,incoterm,dateFormat):
  print(dn)
  conn = get_db_connection()
  cursor = conn.cursor()
  if type == "incoterms":
    query = """UPDATE `attachment_table` SET `incoterms` = %s WHERE `DN#` = %s"""  
    cursor.execute(query, (incoterm, dn, ))
  else:
    query = """UPDATE `attachment_table` SET `date_format` = %s WHERE `DN#` = %s"""  
    cursor.execute(query, (dateFormat, dn, ))
  conn.commit()
  cursor.close()
  conn.close()
  return []
  