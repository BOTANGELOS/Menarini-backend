from flask import Blueprint, request, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
from flask_jwt_extended import create_access_token,jwt_required, get_jwt_identity
from .google_drive import delete_file_from_drive
import mysql.connector
import os
import re
import ast
from datetime import datetime
from collections import OrderedDict
import json
from dateutil import parser

dn_bp = Blueprint('dn', __name__)

original_connect = mysql.connector.connect
def fixed_connect(*args, **kwargs):
    """Automatically add use_pure=True to all mysql connections"""
    if 'use_pure' not in kwargs:
        kwargs['use_pure'] = True
    return original_connect(*args, **kwargs)

# Replace the original connect function globally
mysql.connector.connect = fixed_connect

def get_db_connection():
    return mysql.connector.connect(
        host='mysql-server-test.mysql.database.azure.com',
        user='menarini',
        password='menarini@2025',  # no password
        database='menarini-backend'
    )

# def get_dn_data_with_role(role, email):
#     conn = get_db_connection()
#     cursor = conn.cursor()

#     doc_type_map = {
#         "DN":"DN",
#         "INV":"INV",
#         "COA":"COA",
#         "BOL":"BOL",
#         "AWB":"AWB",
#         "Certificate of Analysis":"COA",
#         "Bill of Lading":"BOL",
#         "Air Waybill":"AWB"
#     }

#     base_query = """
#         SELECT 
#             at.`DN#`,
#             at.`DN`,
#             at.`INV`,
#             at.`Bill of Lading`,
#             at.`Air Waybill`,
#             at.`COA`,
#             at.`complete`,
#             at.`Supplier ID`,
#             at.`date`,
#             at.`final_complete`,
#             at.`id`,
#             GROUP_CONCAT(DISTINCT dn.`PO#`) AS POs
#         FROM 
#             attachment_table at
#         LEFT JOIN 
#             dn_table dn ON dn.`DN#` = at.`DN#`
#     """

#     if role == 1:
#         base_query += " GROUP BY at.`DN#`, at.`DN`, at.`INV`, at.`Bill of Lading`, at.`Air Waybill`, at.`COA`, at.`complete`, at.`Supplier ID`, at.`date`, at.`final_complete`, at.`id`"
#         cursor.execute(base_query)
#     else:
#         base_query += " WHERE at.`admin_email` = %s GROUP BY at.`DN#`, at.`DN`, at.`INV`, at.`Bill of Lading`, at.`Air Waybill`, at.`COA`, at.`complete`, at.`Supplier ID`, at.`date`, at.`final_complete`, at.`id`"
#         cursor.execute(base_query, (email,))

#     results = cursor.fetchall()
#     final_result = []

#     if results:
#         for result in results:
#             dn_number = result[0]
#             cursor.execute("SELECT `id` FROM coa_table WHERE `DN#`=%s", (dn_number,))
#             count_result = cursor.fetchall()

#             dn = result[1]
#             inv = result[2]
#             bol = result[3]
#             awb = result[4]
#             coa = result[5]
#             complete = result[6]
#             supplier_id = result[7]
#             date = result[8]

#             clean_date = date.split(" (")[0]
#             parsed_date = datetime.strptime(clean_date, "%a, %d %b %Y %H:%M:%S %z")
#             formatted_date = f"{parsed_date.day}/{parsed_date.month}/{parsed_date.year}"

#             cursor.execute("SELECT `vendor_name` FROM supplier_table WHERE id = %s", (supplier_id,))
#             supplier = cursor.fetchone()
#             supplier_name = supplier[0] if supplier else ""

#             final_complete = result[9]
#             id = result[10]
#             po_string = result[11]
#             pos = po_string.split(",") if po_string else []

#             entry = {
#                 "id": id,
#                 "DN#": dn_number,
#                 "DN": dn,
#                 "INV": inv,
#                 "COA": coa,
#                 "BOL": bol,
#                 "AWB": awb,
#                 "Date": date,
#                 "formatted_date": formatted_date,
#                 "Supplier": supplier_name,
#                 "coa_count": len(count_result),
#                 "Status": "Incomplete",
#                 "Progress": 0,
#                 "POs": pos
#             }

#             cursor.execute("SELECT `attachment_error`,`reference` FROM error_table WHERE `DN#` = %s", (dn_number,))
#             error_list = cursor.fetchall()

#             if error_list:
#                 for error in error_list:
#                     if error[0] == -2:
#                         entry["DN"] = -2
#                         entry["Status"] = "Error"

#             progress = 0
#             if entry["DN"] == 1:
#                 progress += 20
#             if entry["INV"] == 1:
#                 progress += 20
#             if entry["COA"] == 1:
#                 progress += 20
#             if entry["BOL"] == 1 or entry["AWB"] == 1:
#                 progress += 20
#             if final_complete == 1:
#                 progress += 20
#                 entry["Status"] = "Posted"
#             if progress == 80:
#                 entry["Status"] = "Complete"

#             entry["Progress"] = progress
#             final_result.append(entry)

#     return final_result

def get_dn_data_with_role(role, email):
    conn = get_db_connection()
    cursor = conn.cursor()

    doc_type_map = {
        "DN":"DN",
        "INV":"INV",
        "COA":"COA",
        "BOL":"BOL",
        "AWB":"AWB",
        "Certificate of Analysis":"COA",
        "Bill of Lading":"BOL",
        "Air Waybill":"AWB"
    }

    base_query = """ 
        SELECT  
            dn.`DN#`, 
            CASE 
                WHEN MAX(dn.`status`) IN ('VALIDATED', 'VALIDATION_FAILED') THEN 1
                ELSE 0
            END as DN,
            
            -- Corrected INV count
            (
                SELECT COUNT(DISTINCT SUBSTRING_INDEX(inv.id, '-', 1)) 
                FROM inv_table inv 
                WHERE inv.`DN#` = dn.`DN#` 
                AND inv.status IN ('VALIDATED', 'VALIDATION_FAILED','INITIATED')
            ) as INV,

            -- Corrected Bill of Lading count
            (
                SELECT COUNT(DISTINCT SUBSTRING_INDEX(bl.id, '-', 1)) 
                FROM blawb_table bl 
                WHERE bl.`DN#` = dn.`DN#` 
                AND bl.document_subtype = 'bl' 
                AND bl.status IN ('VALIDATED', 'VALIDATION_FAILED','INITIATED')
            ) as `Bill of Lading`,

            -- Corrected Air Waybill count
            (
                SELECT COUNT(DISTINCT SUBSTRING_INDEX(awb.id, '-', 1)) 
                FROM blawb_table awb 
                WHERE awb.`DN#` = dn.`DN#` 
                AND awb.document_subtype = 'awb' 
                AND awb.status IN ('VALIDATED', 'VALIDATION_FAILED','INITIATED')
            ) as `Air Waybill`,

            -- Corrected COA count
            (
                SELECT COUNT(DISTINCT SUBSTRING_INDEX(coa.id, '-', 1)) 
                FROM coa_table coa 
                WHERE coa.`DN#` = dn.`DN#` 
                AND coa.status IN ('VALIDATED', 'VALIDATION_FAILED','INITIATED')
            ) as COA,

            MAX(dn.`vendor_id`) as `Supplier ID`, 
            MAX(dn.`dn_date`) as `date`, 
            MAX(dn.`status`) as `status`,
            MAX(dn.`id`) as `id`, 
            GROUP_CONCAT(DISTINCT dn.`PO#`) AS POs 

        FROM  
            dn_table dn 
        WHERE 
            dn.`status` IN ('VALIDATED', 'VALIDATION_FAILED','INITIATED')
        GROUP BY 
            dn.`DN#`
    """


    cursor.execute(base_query)
        
    # Fetch results
    results = cursor.fetchall()

    final_result = []
    if results:
        for result in results:
            # Correct indices based on SELECT order:
            # 0: DN#, 1: DN, 2: INV, 3: Bill of Lading, 4: Air Waybill, 5: COA, 
            # 6: Supplier ID, 7: date, 8: status, 9: id, 10: POs
            dn_number = result[0]
            dn = result[1]
            inv = result[2] 
            bol = result[3]
            awb = result[4]
            coa = result[5]
            supplier_id = result[6]
            date = result[7]
            status = result[8]
            id = result[9]
            po_string = result[10]
            
            # Get additional COA count (if needed for different purpose)
            cursor.execute("SELECT `id` FROM coa_table WHERE `DN#`=%s", (dn_number,))
            count_result = cursor.fetchall()

            # Format date
            clean_date = date.split(" (")[0] if isinstance(date, str) else str(date)
            try:
                parsed_date = datetime.strptime(clean_date, "%a, %d %b %Y %H:%M:%S %z")
                formatted_date = f"{parsed_date.day}/{parsed_date.month}/{parsed_date.year}"
            except:
                # Handle different date formats or direct date objects
                if isinstance(date, str):
                    formatted_date = date
                else:
                    formatted_date = date.strftime("%d/%m/%Y") if hasattr(date, 'strftime') else str(date)

            # Get supplier name
            cursor.execute("SELECT `vendor_name` FROM vendor_master WHERE id = %s", (supplier_id,))
            supplier = cursor.fetchone()
            supplier_name = supplier[0] if supplier else ""

            # Parse POs
            pos = po_string.split(",") if po_string else []

            entry = {
                "id": id,
                "DN#": dn_number,
                "DN": dn,
                "INV": inv,
                "COA": coa,
                "BOL": bol,
                "AWB": awb,
                "Date": date,
                "formatted_date": formatted_date,
                "Supplier": supplier_name,
                "coa_count": len(count_result),
                "Status": "Incomplete",
                "Progress": 0,
                "POs": pos
            }

            # Check for errors
            cursor.execute("SELECT `event_type`,`event_message` FROM event_log WHERE `dn_num` = %s", (dn_number,))
            error_list = cursor.fetchall()

            if error_list:
                for error in error_list:
                    if error[0] == -2:
                        entry["DN"] = -2
                        entry["Status"] = "Error"

            # Calculate progress
            progress = 0
            if entry["DN"] == 1:
                progress += 20
            if entry["INV"] > 0: 
                progress += 20
            if entry["COA"] > 0:  
                progress += 20
            if entry["BOL"] > 0 or entry["AWB"] > 0:  # Changed from == 1 to > 0 since they're counts
                progress += 20
            if status == "VALIDATED":
                progress += 20
                entry["Status"] = "Posted"
            if progress == 80:
                entry["Status"] = "Complete"

            entry["Progress"] = progress
            final_result.append(entry)

    cursor.close()
    conn.close()
    return final_result
  
# @dn_bp.route('/all_dn', methods=['POST'])
# def all_dn_data():
#   data = request.get_json()
#   email = data.get("email")
#   role = data.get("role")
  
#   conn = get_db_connection()
#   cursor = conn.cursor()
#   cursor.execute("SELECT `admin_email` FROM user WHERE email = %s", (email,))
#   emails = cursor.fetchone()
#   admin_email = emails[0]

#   cursor.execute("SELECT `role` FROM admin_table WHERE email = %s", (admin_email,))
#   result = cursor.fetchone()
#   if result:
#     if result[0] == 1 or result[0] == '1':
#       result = get_dn_data_with_role(1,admin_email)
#     else:
#       result = get_dn_data_with_role(2,admin_email)
#   # print(result)
#   return result

@dn_bp.route('/all_dn', methods=['POST'])
def all_dn_data():
  data = request.get_json()
  email = data.get("email")
  role = data.get("role")
  
  conn = get_db_connection()
  cursor = conn.cursor()
#   cursor.execute("SELECT `admin_email` FROM user WHERE email = %s", (email,))
#   emails = cursor.fetchone()
#   admin_email = emails[0]

#   result = get_dn_data_with_role(1, admin_email)
  result = get_dn_data_with_role(1, email)
  # print(result)
  return result

@dn_bp.route('/attachment_info', methods=['POST'])
def dn_attachment_data():
    import re
    from collections import defaultdict
    from datetime import datetime

    data = request.get_json()
    doc_type = data.get("Doc Type")
    dn = data.get("DN#")

    conn = get_db_connection()
    cursor = conn.cursor()

    query = ""
    columns = []

    if doc_type == "DN":
        columns = [
            "PO#", "Item Code", "Packing Slip#", "Quantity", "Batch#",
            "Manufacturing Date", "Expiry Date", "Document Date",
            "Incoterms", "Item Description", "Document", "id"
        ]
        table_name = "dn_table"
    elif doc_type == "INV":
        columns = [
            "PO#", "Packing Slip#", "Quantity", "Batch#",
            "Manufacturing Date", "Item Code", "Expiry Date",
            "Document Date", "INV NO#", "Incoterms", "Item Description",
            "Document", "id"
        ]
        table_name = "inv_table"
    elif doc_type == "COA":
        columns = [
            "Item Description", "Manufacturing Date",
            "Expiry Date", "Document", "id", "Batch#", "flag"
        ]
        table_name = "coa_table"
    elif doc_type in ["BOL", "AWB"]:
        columns = ["hs_code", "blawb_number", "shipped_date", "shipping_reference", "document_subtype", "Document", "id"]
        table_name = "blawb_table"
    else:
        return {"error": "Unsupported Doc Type"}, 400

    query = f"""
        SELECT {', '.join([f'`{col}`' for col in columns])}
        FROM {table_name}
        WHERE `DN#` = %s
        AND `status` IN ('VALIDATED', 'VALIDATION_FAILED')
    """
    cursor.execute(query, (dn,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    grouped = defaultdict(list)

    for row in rows:
        row_dict = {}
        for i in range(len(columns)):
            col_name = columns[i]
            val = row[i]

            # Convert datetime to string (only date part)
            if isinstance(val, datetime):
                val = val.strftime('%Y-%m-%d')

            row_dict[col_name] = val

        raw_id = row_dict["id"]
        base_id = re.sub(r'-\d+$', '', raw_id)
        grouped[base_id].append(row_dict)

    final_response = []
    for base_id, items in grouped.items():
        line_items = []
        attachment_url = items[0].get("Document", "")

        for item in items:
            line_item = {k: v for k, v in item.items() if k not in ["id", "Document"]}

            if doc_type in ["BOL", "AWB"]:
                mapped_item = {
                    "BL#": line_item.get("blawb_number", ""),
                    "doc_sub_type": line_item.get("document_subtype", ""),
                    "HS Code": line_item.get("hs_code", ""),
                    "Posting Date": line_item.get("shipped_date", ""),
                    "Shipping Reference": line_item.get("shipping_reference", ""),
                    "Incoterm": ""
                }
                line_items.append(mapped_item)
            else:
                line_items.append(line_item)

        final_response.append({
            "attachment_id": base_id,
            "attachment_url": attachment_url,
            "line_items": line_items
        })

    return final_response

@dn_bp.route('/check_coa_flag', methods=['POST'])
def check_coa_flag():
    data = request.get_json()
    id = data.get("id")
    conn = get_db_connection()
    cursor = conn.cursor()
    print("---------------------------")
    flag_status = 0
    
    query  = "SELECT `flag` FROM coa_table WHERE `id`=%s"
    cursor.execute(query, (id,))
    flag_status_result = cursor.fetchone() 

    if flag_status_result:
      flag_status = flag_status_result[0]
    
    if flag_status == 0:
      flag_status = 1
    else:
      flag_status = 0
    cursor.execute("UPDATE coa_table SET `flag`=%s WHERE id = %s",(flag_status,id,))
    conn.commit()
    
    cursor.close()
    conn.close()
    return []


@dn_bp.route('/coa_attachment_info', methods=['POST'])
def coa_attachment_info():
    data = request.get_json()
    doc_type = data.get("Doc Type")
    dn = data.get("DN#")
    index = data.get("index")
    index = int(index)
    index = index - 1
    conn = get_db_connection()
    cursor = conn.cursor()
    
    columns = [
            "Item Description", "Manufacturing Date", "Expiry Date","Document","id","Batch#"
    ]
    query = f"""
        SELECT {', '.join([f'`{col}`' for col in columns])}
        FROM coa_table
        WHERE `DN#`=%s
    """
    cursor.execute(query, (dn,))
    rows = cursor.fetchall() 

    response = []
    if rows:
      for (ind,row) in enumerate(rows):
        if ind == index:
          result_dict = {}
          for i in range(len(columns)):
              result_dict[columns[i]] = row[i]  # Maintain the order in which they appear
          response.append(result_dict)
    
    cursor.close()
    conn.close()
    return response

@dn_bp.route('/error_info', methods=['POST'])
def dn_error_info():
    data = request.get_json()
    doc_type = data.get("Doc Type")
    dn = data.get("DN#")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT `message`,`email_id` FROM error_table WHERE `DN#` = %s", (dn,))
    results = cursor.fetchall()

    final = []
    if results:
      for result in results:
        entry = {"message":result[0], "emailID":result[1]}
        final.append(entry)
    
    cursor.close()
    conn.close()
    return final


# @dn_bp.route('/ocr_info', methods=['POST'])
# def ocr_info():
#     data = request.get_json()
#     doc_type = data.get("Doc Type")
#     dn = data.get("DN#")
#     print("------------")
#     conn = get_db_connection()
#     cursor = conn.cursor()

#     cursor.execute("SELECT `index`,`key`,`x`,`y`,`width`,`height`,`page_width`,`page_height`,`page`,`pdf_path`,`id` FROM ocr_table WHERE `DN#` = %s and `doc_type` = %s", (dn,doc_type,))
#     results = cursor.fetchall()

#     final = []
#     if results:
#       for result in results:
#         entry = {
#           "index":result[0],
#           "key":result[1],
#           "x":result[2],
#           "y":result[3],
#           "width":result[4],
#           "height":result[5],
#           "page_width":result[6],
#           "page_height":result[7],
#           "page":result[8],
#           "pdf_path":result[9],
#           "id":result[10],
#         }
#         final.append(entry)
#     cursor.close()
#     conn.close()
#     return final

@dn_bp.route('/ocr_info', methods=['POST'])
def ocr_info():
    data = request.get_json()
    doc_type = data.get("Doc Type")
    doc_id = data.get("id")  

    if not all([doc_type, doc_id]):
        return {"error": "Missing required fields: 'Doc Type' or 'id'"}, 400

    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
        SELECT `index`, `key`, `x`, `y`, `width`, `height`,
               `page_width`, `page_height`, `page`, `pdf_path`, `id`
        FROM ocr_table
        WHERE `doc_type` = %s AND `id` = %s
    """
    cursor.execute(query, ( doc_type, doc_id))
    results = cursor.fetchall()

    final = []
    for result in results:
        entry = {
            "index": result[0],
            "key": result[1],
            "x": result[2],
            "y": result[3],
            "width": result[4],
            "height": result[5],
            "page_width": result[6],
            "page_height": result[7],
            "page": result[8],
            "pdf_path": result[9],
            "id": result[10],
        }
        final.append(entry)

    cursor.close()
    conn.close()

    return final


@dn_bp.route('/coa_ocr_info', methods=['POST'])
def coa_ocr_info():
    data = request.get_json()
    doc_type = data.get("Doc Type")
    dn = data.get("DN#")
    file_path = data.get("document")
    print("------------")
    document = os.path.basename(file_path)
    print(document)
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT `index`,`key`,`x`,`y`,`width`,`height`,`page_width`,`page_height`,`page`,`pdf_path`,`id` FROM ocr_table WHERE `DN#` = %s and `doc_type` = %s and `pdf_path`=%s", (dn,doc_type,document,))
    results = cursor.fetchall()

    final = []
    if results:
      for result in results:
        entry = {
          "index":result[0],
          "key":result[1],
          "x":result[2],
          "y":result[3],
          "width":result[4],
          "height":result[5],
          "page_width":result[6],
          "page_height":result[7],
          "page":result[8],
          "pdf_path":result[9],
          "id":result[10],
        }
        final.append(entry)
    cursor.close()
    conn.close()
    print("((((((()))))))")
    print(final)
    return final

@dn_bp.route('/create_field_position', methods=['POST'])
def create_field_position():
    data = request.get_json()
    doc_type = data.get("Doc Type")
    dn = data.get("DN#")
    pdf_path = data.get("pdf_path")
    key = data.get("key")
    index = data.get("index")
    x = data.get("x")
    y = data.get("y")
    width = data.get("width")
    height = data.get("height")
    page = data.get("page")
    page_width = data.get("page_width")
    page_height = data.get("page_height")
    print("------------")
    print(data)
    conn = get_db_connection()
    cursor = conn.cursor()

    # cursor.execute(" `index`,`key`,`x`,`y`,`width`,`height`,`page_width`,`page_height`,`page`,`pdf_path`,`id` FROM ocr_table WHERE `DN#` = %s and `doc_type` = %s", (dn,doc_type,))
    cursor.execute("INSERT INTO ocr_table (`DN#`,`doc_type`,`index`,`key`,`x`,`y`,`width`,`height`,`page_width`,`page_height`,`page`,`pdf_path`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(dn,doc_type, index, key, x,y,width,height, page_width, page_height, page, pdf_path,))
    conn.commit()

    cursor.close()
    conn.close()
    return []
  
  
@dn_bp.route('/update_field_position', methods=['POST'])
def update_field_position():
    data = request.get_json()
    id = data.get("id")
    x = data.get("x")
    y = data.get("y")
    width = data.get("width")
    height = data.get("height")
    page = data.get("page")
    print("------------")
    print(data)
    conn = get_db_connection()
    cursor = conn.cursor()

    # cursor.execute(" `index`,`key`,`x`,`y`,`width`,`height`,`page_width`,`page_height`,`page`,`pdf_path`,`id` FROM ocr_table WHERE `DN#` = %s and `doc_type` = %s", (dn,doc_type,))
    cursor.execute("UPDATE ocr_table SET `x`=%s,`y`=%s,`width`=%s,`height`=%s,`page`=%s WHERE id = %s",(x,y,width,height,page,id))
    conn.commit()

    cursor.close()
    conn.close()
    return []
  
@dn_bp.route('/update_fields', methods=['POST'])
def update_fields():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    data = request.get_json()
    update_data = data.get("data")
    dn = data.get("DN#")
    doc_type = data.get("DocType")
    
    # [{'id': 37, 'changes': {'Document Date': '25/03/2521', 'Expiry Date': '10-May-202`129', 'GIC Code': '12312', 'Incoterms': 'FCA453', 'Quantity': '14,990jjj'}}]
    if update_data:
      for data in update_data:
        update_id = data.get("id")
        changed_data = data.get("changes")
        table_name = ""
        if doc_type == "DN":
          table_name = "dn_table"
        elif doc_type == "INV":
          table_name = "inv_table"
        elif doc_type == "COA":
          table_name = "coa_table"
        else:
          table_name = "blawb_table"
        
        set_clause = ", ".join(f"`{key}` = %s" for key in changed_data.keys())
        values = list(changed_data.values())
        sql = f"UPDATE `{table_name}` SET {set_clause} WHERE id = %s"
        values.append(update_id)
        print(values)
        cursor.execute(sql, values)
        conn.commit()
    cursor.close()
    conn.close()
    return []

@dn_bp.route('/get_po_list', methods=['POST'])
def get_po_list():
    data = request.get_json()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    supplier_name = ""
    date = ""
    
    cursor.execute("""
        SELECT 
            dnt.`PO#`, 
            MAX(dnt.`DN#`) AS `DN#`, 
            vm.`vendor_name`, 
            MAX(dnt.`created_at`) AS `created_at`
        FROM 
            `dn_table` dnt
        JOIN 
            `vendor_master` vm 
        ON 
            dnt.`vendor_id` = vm.`id`
        WHERE 
            dnt.`PO#` IS NOT NULL
        GROUP BY 
            dnt.`PO#`, vm.`vendor_name`;
    """)


    results = cursor.fetchall()
    final = []
    if results:
      for result in results:
          status = "Incomplete"
        #   cursor.execute("SELECT `id` FROM staging_table WHERE `PO#` = %s", (result[0],))
        #   flag = cursor.fetchone()
        #   parsed_date = parser.parse(result[3])
        #   formatted_date = f"{parsed_date.day}/{parsed_date.month}/{parsed_date.year}"
          formatted_date = result[3].strftime("%d/%m/%Y")
        #   if flag:
        #      status = "Complete"
          entry = {
             "PO#":result[0],
             "DN#":result[1],
             "status":status,
             "supplier_name":result[2],
             "date":result[3],
             "formatted_date": formatted_date
          }
          print("entry", entry)
          final.append(entry)
    
    cursor.close()
    conn.close()
    return final


@dn_bp.route('/update_dn_table', methods=['POST'])
def update_dn_table():
    data = request.get_json()
    data = data.get("data")
    dn_change = data.get("dn")
    inv_change = data.get("inv")
    coa_change = data.get("coa")
    bol_change = data.get("bol")
    conn = get_db_connection()
    cursor = conn.cursor()
    if dn_change:
       for data in dn_change.values():
          row_id = data['id']
          key_to_update = next(k for k in data if k != 'id')  # e.g., 'Expiry Date'
          new_value = data[key_to_update]
          query = f"UPDATE dn_table SET `{key_to_update}` = %s WHERE id = %s"
          cursor.execute(query, (new_value, row_id))

    if inv_change:
       for data in inv_change.values():
          row_id = data['id']
          key_to_update = next(k for k in data if k != 'id')  # e.g., 'Expiry Date'
          new_value = data[key_to_update]
          query = f"UPDATE inv_table SET `{key_to_update}` = %s WHERE id = %s"
          cursor.execute(query, (new_value, row_id))

    if coa_change:
       for data in inv_change.values():
          row_id = data['id']
          key_to_update = next(k for k in data if k != 'id')  # e.g., 'Expiry Date'
          new_value = data[key_to_update]
          query = f"UPDATE coa_table SET `{key_to_update}` = %s WHERE id = %s"
          cursor.execute(query, (new_value, row_id))

    conn.commit()
    cursor.close()
    conn.close()
    return []


# @dn_bp.route('/duplicated_test', methods=['POST'])
# def duplicated_test():
#     data = request.get_json()
#     doc_type = data.get("Doc Type")
#     dn = data.get("DN#")
    
#     conn = get_db_connection()
#     cursor = conn.cursor()
    
#     cursor.execute("SELECT `id`,`drive_file_id`,`source` FROM duplicated_attachment WHERE `DN#` = %s AND `doc_type`=%s", (dn,doc_type))
#     results = cursor.fetchall()

#     final = []
#     if results:
#       if doc_type!='COA':
#         for result in results:
#           entry = {
#              'id':result[0],
#              'DN#':dn,
#              'doc_type':doc_type,
#              'drive_file_id':result[1],
#              'source':result[2]
#           }
#           final.append(entry)
#     cursor.close()
#     conn.close()
#     return final

from urllib.parse import unquote, urlparse
import os

@dn_bp.route('/duplicated_test', methods=['POST'])
def duplicated_test():
    data = request.get_json()
    doc_type = data.get("Doc Type")
    dn = data.get("DN#")

    conn = get_db_connection()
    cursor = conn.cursor()

    final = []
    query = ""

    # Build query based on doc_type
    if doc_type == "DN":
        query = """
            SELECT `id`, `Document`
            FROM dn_table
            WHERE `DN#` = %s AND `is_partially_duplicate` = 1
        """
    elif doc_type == "INV":
        query = """
            SELECT `id`, `Document`
            FROM inv_table
            WHERE `DN#` = %s AND `is_partially_duplicate` = 1
        """
    elif doc_type == "COA":
        query = """
            SELECT `id`, `Document`
            FROM coa_table
            WHERE `DN#` = %s AND `is_partially_duplicate` = 1
        """
    elif doc_type in ["BOL", "AWB"]:
        subtype = "bl" if doc_type == "BOL" else "awb"
        query = """
            SELECT `id`, `Document`
            FROM blawb_table
            WHERE `DN#` = %s AND `document_subtype` = %s AND `is_partially_duplicate` = 1
        """
    else:
        cursor.close()
        conn.close()
        return {"error": "Invalid Doc Type"}, 400

    # Execute the query
    if doc_type in ["BOL", "AWB"]:
        cursor.execute(query, (dn, subtype))
    else:
        cursor.execute(query, (dn,))

    results = cursor.fetchall()

    # Helper function to extract filename from URL
    def extract_filename_from_url(url):
        if not url:
            return ""
        decoded_url = unquote(url)
        path = urlparse(decoded_url).path
        filename = os.path.basename(path)
        return filename

    for result in results:
        drive_file_url = result[1]
        entry = {
            "id": result[0],
            "DN#": dn,
            "doc_type": doc_type,
            "drive_file_id": drive_file_url,
            "Document": extract_filename_from_url(drive_file_url)
        }
        final.append(entry)

    cursor.close()
    conn.close()
    return final


def delete_database_data(doc_type,dn):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        if doc_type=="DN":
            query = """DELETE FROM dn_table WHERE `DN#` = %s""" 
            cursor.execute(query, (dn,))
        if doc_type=="INV":
            query = """DELETE FROM inv_table WHERE `DN#` = %s""" 
            cursor.execute(query, (dn,))
        if doc_type=="COA":
            query = """DELETE FROM coa_table WHERE `DN#` = %s""" 
            cursor.execute(query, (dn,))
        if doc_type=="BOL":
            query = """DELETE FROM bol_table WHERE `DN#` = %s""" 
            cursor.execute(query, (dn,))
        conn.commit() 


    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None  # Ensure function always returns a value

    finally:
        # Close the database connection
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection closed.")

def get_google_drive_document_info(dn, doc_type):
    conn = get_db_connection()
    cursor = conn.cursor()
    result = []
    if doc_type=="DN":
        query = """SELECT `Document` FROM dn_table WHERE `DN#` = %s""" 
        cursor.execute(query, (dn,))
        result = cursor.fetchone()
    if doc_type=="INV":
        query = """SELECT `Document` FROM inv_table WHERE `DN#` = %s""" 
        cursor.execute(query, (dn,))
        result = cursor.fetchone()
    if doc_type=="COA":
        query = """SELECT `Document` FROM coa_table WHERE `DN#` = %s""" 
        cursor.execute(query, (dn,))
        result = cursor.fetchone()
    if doc_type=="BOL":
        query = """SELECT `Document` bol_table WHERE `DN#` = %s""" 
        cursor.execute(query, (dn,))
        result = cursor.fetchone()
    if result:
      path = result[0]
      parts = os.path.normpath(path).split(os.sep)
      # Extract desired parts
      ro = parts[0] if len(parts) > 0 else None
      supplier_name = parts[1] if len(parts) > 1 else None
      shipment_id = parts[2] if len(parts) > 2 else None
      return {"supplier_domain": ro, "supplier_name" :supplier_name, "dn":shipment_id}
    cursor.close()
    conn.close()
    print("MySQL connection closed.")
   


def new_log_sheet(log_type, email,detail):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        now = datetime.now()
        date = now.strftime("%Y-%m-%d %H:%M:%S")
        query = """INSERT INTO logsheet (`log`,`email`,`color`,`date`,`detail`) VALUES (%s,%s,%s,%s,%s)"""  
        cursor.execute(query, (log_type, email, 'error',date, detail,))
        conn.commit()
        return
    
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None  # Ensure function always returns a value

    finally:
        # Close the database connection
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection closed.")
            

@dn_bp.route('/update_duplicated_state', methods=['POST'])
def update_duplicated_state():
    data = request.get_json()
    duplicatedDocument = data.get("duplicatedDocument")
    doc_type = data.get("Doc Type")
    dn = data.get("DN#")
    email = data.get("email")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT `id`,`drive_file_id`,`doc_type`,`source` FROM duplicated_attachment WHERE `doc_type`=%s and `DN#` = %s" ,(doc_type,dn,))
    results = cursor.fetchall()
    final = []
    id_list = []
    source_list = []
    supplier_domain = ""
    supplier_name = ""
    drive_dn = ""
    
    cursor.execute("SELECT `Supplier ID` FROM attachment_table WHERE `DN#`=%s",(dn,))
    result = cursor.fetchone()
    if result:
      cursor.execute("SELECT `domain`,`vendor_name` FROM supplier_table WHERE `id`=%s",(result[0],))
      vendor_result = cursor.fetchone()
      if vendor_result:
        supplier_domain = vendor_result[0]
        supplier_name = vendor_result[1]
        drive_dn = dn
      
    if not supplier_domain:
      return "no domain"
    
    if results:
      for result in results:
        if result[0] != duplicatedDocument:
            # if supplier_domain == "":
            #   updated_supplier_info = get_google_drive_document_info(dn,doc_type)
            #   supplier_domain = updated_supplier_info["supplier_domain"]
            #   supplier_name = updated_supplier_info["supplier_name"]
            #   drive_dn = updated_supplier_info["dn"]
            id_list.append(result[0])
            source_list.append(result[3])
            delete_file_from_drive(result[1])  # Corrected line
            delete_database_data(doc_type, dn)
    if id_list:
      query = """INSERT INTO google_drive_change (`supplier_domain`,`supplier_name`,`dn`) VALUES (%s,%s,%s)"""  
      cursor.execute(query, (supplier_domain,supplier_name,drive_dn,))
      for id in id_list:
        query = """DELETE FROM duplicated_attachment WHERE `id` = %s""" 
        cursor.execute(query, (id,))
      for delete_file in source_list:
        query = """DELETE FROM ocr_table WHERE `pdf_path` = %s""" 
        cursor.execute(query, (id,))
        
      conn.commit() 
    
    sheet_error = "The DN - "+dn+" updated "+ doc_type +"document. Check it."
    new_log_sheet("Attachment Error",email,sheet_error)
    cursor.close()
    conn.close()
    return "success"
  
  

@dn_bp.route('/re_ocr', methods=['POST'])
def re_ocr():
    data = request.get_json()
    dn = data.get("DN#")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT `Supplier ID` FROM attachment_table WHERE `DN#`=%s",(dn,))
    result = cursor.fetchone()
    if result:
      cursor.execute("SELECT `domain`,`vendor_name` FROM supplier_table WHERE `id`=%s",(result[0],))
      vendor_result = cursor.fetchone()
      if vendor_result:
        supplier_domain = vendor_result[0]
        supplier_name = vendor_result[1]
        drive_dn = dn
      
    if not supplier_domain:
      return "no domain"
    
    query = """DELETE FROM dn_table WHERE `DN#` = %s""" 
    cursor.execute(query, (dn,))
    
    query = """DELETE FROM inv_table WHERE `DN#` = %s""" 
    cursor.execute(query, (dn,))
    
    query = """DELETE FROM coa_table WHERE `DN#` = %s""" 
    cursor.execute(query, (dn,))
    
    query = """DELETE FROM bol_table WHERE `DN#` = %s""" 
    cursor.execute(query, (dn,))
    
    query = """DELETE FROM ocr_table WHERE `DN#` = %s""" 
    cursor.execute(query, (dn,))
    
    query = """INSERT INTO google_drive_change (`supplier_domain`,`supplier_name`,`dn`) VALUES (%s,%s,%s)"""  
    cursor.execute(query, (supplier_domain,supplier_name,drive_dn,))
    conn.commit()
    cursor.close()
    conn.close()
    return "success"
  
  

@dn_bp.route('/re_check', methods=['POST'])
def re_check():
    data = request.get_json()
    dn = data.get("DN#")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """DELETE FROM dn_table WHERE `DN#` = %s""" 
    cursor.execute(query, (dn,))
    conn.commit()
    
    query = """DELETE FROM inv_table WHERE `DN#` = %s""" 
    cursor.execute(query, (dn,))
    conn.commit()
    
    query = """DELETE FROM coa_table WHERE `DN#` = %s""" 
    cursor.execute(query, (dn,))
    conn.commit()
    
    query = """DELETE FROM bol_table WHERE `DN#` = %s""" 
    cursor.execute(query, (dn,))
    conn.commit()
    
    query = """DELETE FROM ocr_table WHERE `DN#` = %s""" 
    cursor.execute(query, (dn,))
    conn.commit()
    
    query = """DELETE FROM attachment_table WHERE `DN#` = %s""" 
    cursor.execute(query, (dn,))
    conn.commit()
    
    query = """DELETE FROM duplicated_attachment WHERE `DN#` = %s""" 
    cursor.execute(query, (dn,))
    conn.commit()
    
    query = """DELETE FROM email_attachment WHERE `DN#` = %s""" 
    cursor.execute(query, (dn,))
    conn.commit()
    
    query = """DELETE FROM email_check WHERE `DN#` = %s""" 
    cursor.execute(query, (dn,))
    conn.commit()
    
    query = """DELETE FROM error_table WHERE `DN#` = %s""" 
    cursor.execute(query, (dn,))
    conn.commit()
    
    query = """DELETE FROM logo_table WHERE `DN#` = %s""" 
    cursor.execute(query, (dn,))
    conn.commit()
    
    query = """DELETE FROM supplier_name_intervention WHERE `DN#` = %s""" 
    cursor.execute(query, (dn,))
    conn.commit()
    cursor.close()
    conn.close()
    return "success"