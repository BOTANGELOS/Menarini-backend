from flask import Blueprint, jsonify, request, current_app,send_from_directory
from .database_handler import insert_google_drive_change,get_supplier_domain_and_name_with_id,update_notification,get_all_notification,get_supplier_id_with_vendor_name,get_all_supplier,update_logo_info,get_all_logo_info,get_logo_with_email,get_all_data,new_logsheet,get_all_email,get_gmail_password,get_all_supplier_name,update_multi_doc,update_supplier_name,get_all_logs,get_email_error, get_attachment_list_from_dn,get_attachment_list_from_email,get_all_emailId,complete_flag, get_dn_from_emailID,get_document_error_with_email,get_th_first_table,get_th_second_table,get_threshold_status,get_threshold_from_db,get_document_error_with_id, get_supplier_from_email, get_multi_doc
from .email_handler import connect_to_email, fetch_emails, extract_email_content
import os
main_bp = Blueprint('main', __name__)
from flask import send_from_directory
from flask_mail import Message
from werkzeug.utils import secure_filename
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from .auth import get_user_info
from .google_drive import rename_matching_drive_files, google_drive_supplier_name_change
# USERNAME = "robertedyoung@gmail.com"
# PASSWORD = "reva gpan ieaa eujj"
IMAP_SERVER = "imap.gmail.com"
# arxy nebl rodd xnbq

UPLOAD_FOLDER = '../logo'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@main_bp.route('/upload-logo', methods=['POST'])
def upload_logo():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']
    domain = request.form.get('domain')
    name = request.form.get('name')
    email = request.form.get('email')

    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if file and domain and name:
        file_name = get_supplier_id_with_vendor_name(domain,name)
        _, ext = os.path.splitext(file.filename)
        final_filename = f"{file_name}{ext}"
        filepath = os.path.join(UPLOAD_FOLDER, final_filename)
        data={"logo":file_name,"img":final_filename}
        update_logo_info(data,email)
        file.save(filepath)
        return jsonify({'message': 'File uploaded successfully', 'filename': file_name}), 200
    else:
        return jsonify({'error': 'Missing fields'}), 400
      
@main_bp.route('/email/all_email', methods=['POST'])
def all_email():
  
  data = request.get_json()
  email = data.get("email")
  role = data.get("role")

  email_list = get_all_email(email)
    
  # password = get_gmail_password(email)
  # email_id_list = get_all_emailId()

  # mail = connect_to_email(email, password, IMAP_SERVER)
  # print("Connected to the email server.")
  # Fetch emails
  # emails = fetch_emails(mail)
  # email_list = []
  # for email in emails:
  #   if role == "admin":
  #     value = email_id_list.get(email["Message-ID"])
  #     if value is not None:
  #       subject, sender, body, attachments = extract_email_content(email)
  #       excerpt = body[:30] + '...' if len(body) > 30 else body
  #       email_list.append({"id":email["Message-ID"],"subject":subject,"sender":sender,"body":body,"attachments":attachments, "date":email["date"],"excerpt":excerpt, "status":complete_flag(email_id_list[email["Message-ID"]])})
  #   else:
  #     subject, sender, body, attachments = extract_email_content(email)
  #     excerpt = body[:30] + '...' if len(body) > 30 else body
  #     email_list.append({"id":email["Message-ID"],"subject":subject,"sender":sender,"body":body,"attachments":attachments, "date":email["date"],"excerpt":excerpt})
  return jsonify({"data": email_list})

@main_bp.route('/email/get_attachment_list', methods=['POST'])
def get_attachment_list():
  data = request.get_json()
  email_id = data.get("EmailID")  # Example field
  dn = get_dn_from_emailID(email_id)
  dn,inv,bill_of_lading,air_waybill,coa = get_attachment_list_from_dn(dn)
  result = {
    "DN":dn,
    "INV":inv,
    "COA":coa,
    "Bill of Lading":bill_of_lading,
    "Air Waybill":air_waybill,
  }
  print("Received DN:", result)
  return jsonify({"data": result})


@main_bp.route('/email/get_attachment_list_from_email', methods=['POST'])
def get_attachment_list_fromEmail():
    data = request.get_json()
    email_id = data.get("EmailID")
    
    result_tuple = get_attachment_list_from_email(email_id)

    if result_tuple is None:
        return jsonify({"error": "No attachments found for this email ID."}), 404

    dn, inv, bill_of_lading, air_waybill, coa = result_tuple
    result = {
        "DN": dn,
        "INV": inv,
        "COA": coa,
        "Bill of Lading": bill_of_lading,
        "Air Waybill": air_waybill,
    }
    print("Received DN:", result)
    return jsonify({"data": result})


@main_bp.route('/email/get_error', methods=['POST'])
def get_error():
  data = request.get_json()
  email_id = data.get("EmailID")  # Example field
  result = get_email_error(email_id)
  return jsonify({"data": result})

@main_bp.route('/logs/get_all_data', methods=['POST'])
def get_all_notification_data():
  data = request.get_json()
  email = data.get("email")
  result = get_all_logs(email)
  return result


@main_bp.route('/email/get_document_error_with_email', methods=['POST'])
def get_document_error():
  data = request.get_json()
  email_id = data.get("EmailID")  # Example field
  result = get_document_error_with_email(email_id)
  return jsonify({"data": result})

@main_bp.route('/email/get_document_error_with_email_logs_table', methods=['POST'])
def get_document_error_logs_table():
  data = request.get_json()
  logID = data.get("logID")  # Example field
  result = get_document_error_with_id(logID)
  return jsonify({"data": result})

@main_bp.route('/dn/get_threshold_status', methods=['POST'])
def get_threshold_status_route():
    data = request.get_json()
    dn_number = data.get("dn_number")
    threshold_percent = get_threshold_from_db()
    result = get_threshold_status(dn_number, threshold_percent)
    return jsonify({"data": result})

@main_bp.route('/th/get_first_table', methods=['POST'])
def get_first_table_data():
    data = request.get_json()
    dn_number = data.get("dn_number")
    result = get_th_first_table(dn_number)
    return jsonify({"data": result})

@main_bp.route('/th/get_second_table', methods=['POST'])
def get_second_table_data():
    data = request.get_json()
    dn_number = data.get("dn_number")
    result = get_th_second_table(dn_number)
    return jsonify({"data": result})

@main_bp.route('/intervention/supplier_intervention', methods=['POST'])
def supplier_intervention():
  data = request.get_json()
  email_id = data.get("EmailID")  # Example field
  domain, vendor_name, dn = get_supplier_from_email(email_id)
  result = {
    "domain":domain,
    "vendor_name":vendor_name,
    "DN#":dn,
  }
  return jsonify({"data": result})

@main_bp.route('/intervention/multi_doc_intervention', methods=['POST'])
def multi_doc_intervention():
  data = request.get_json()
  email_id = data.get("EmailID")  # Example field
  result = get_multi_doc(email_id)
  return jsonify({"data": result})


@main_bp.route('/intervention/set_multi_doc', methods=['POST'])
def set_multi_doc():
  data = request.get_json()
  email_id = data.get("EmailID") 
  data = data.get("data")
  if data:
    for multi_doc in data:
      old_doc_list = multi_doc.get("old_doc_list")
      new_doc_list = multi_doc.get("doc_list")
      vendor_id = multi_doc.get("vendor_id")
      dn = multi_doc.get("DN#")
      result = update_multi_doc(old_doc_list,new_doc_list, email_id,dn)
      supplier_domain, supplier_name = get_supplier_domain_and_name_with_id(vendor_id)
      rename_matching_drive_files(supplier_domain, supplier_name, dn, old_doc_list, new_doc_list)
      
      insert_google_drive_change(supplier_domain,supplier_name,dn)
  return jsonify({"data": result})

# DN# : "20250210"
# doc_list: "INV & COA & BOL"
# file_name: "FLURAZEPAM MONOHYDROCHLORIDE - Shipping Documents PO PO1-005689 rel.1.pdf"
# old_doc_list: " & INV & COA"
# vendor_id: "26"

@main_bp.route('/intervention/set_supplier_name', methods=['POST'])
def set_supplier_name():
  data = request.get_json()
  vendor_domain = data.get("vendor_domain")  # Example field
  old_vendor_name = data.get("old_vendor_name")  # Example field
  new_vendor_name = data.get("new_vendor_name")  # Example field
  dn = data.get("DN#")  # Example field
  result = update_supplier_name(vendor_domain,old_vendor_name,new_vendor_name,dn)
  google_drive_supplier_name_change(vendor_domain,old_vendor_name,vendor_domain,new_vendor_name,dn)
  return jsonify({"data": result})

@main_bp.route('/intervention/get_logo_intervention', methods=['POST'])
def get_logo_intervention():
  data = request.get_json()
  email = data.get("emailID")  # Example field
  result = get_logo_with_email(email)
  return result


@main_bp.route('/intervention/get_all_logo', methods=['POST'])
def get_all_logo():
  data = request.get_json()
  result = get_all_logo_info()
  return result

@main_bp.route('/intervention/set_updated_logo_info', methods=['POST'])
def set_updated_logo_info():
  data = request.get_json()
  info = data.get("data")
  email = data.get("email")
  result = update_logo_info(info,email)
  return result

@main_bp.route('/info/get_all_supplier_info', methods=['POST'])
def get_all_supplier_info():
  result = get_all_supplier()
  return result

@main_bp.route('/supplier/supplier_name', methods=['POST'])
def supplier_name():
  data = request.get_json()
  email = data.get("EmailID")
  result = get_all_supplier_name(email)
  return jsonify({"data": result})

@main_bp.route('/download/<filename>')
def download_file(filename):
    downloads_path = os.path.join(os.getcwd(), 'downloads')
    return send_from_directory(downloads_path, filename)
  
@main_bp.route('/ocr_downloads/<filename>')
def ocr_download_file(filename):
    downloads_path = os.path.join(os.getcwd(), '../Downloads')
    return send_from_directory(downloads_path, filename)
  
@main_bp.route('/logos/<filename>')
def serve_logo(filename):
  BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
  LOGO_DIR = os.path.join(BASE_DIR, 'logo')
  return send_from_directory(LOGO_DIR, filename)
  
@main_bp.route('/email/send-email', methods=['POST'])
def send_email():
    try:  # ✅ moved here
        user_data = request.form
        files = request.files.getlist('attachments')

        # Validate required fields
        if not all([user_data.get('to'), user_data.get('subject'), user_data.get('message')]):
            return jsonify({'success': False, 'message': 'Missing required fields'}), 400
        # Configure mail for current user
        user_info = user_data.get('user')
        user_password = get_gmail_password(user_info)
        mail = current_app.extensions.get('mail')
        mail.server = 'smtp.gmail.com'
        mail.port = 587
        mail.username = user_info
        mail.password = user_password
        mail.use_tls = True
        mail.use_ssl = False
        # Create email message
        msg = Message(
            subject=user_data['subject'],
            recipients=[user_data['to']],
            body=user_data.get('message'),
            sender=user_info
        )

        # Handle attachments
        for file in files:
            if file.filename != '':
                msg.attach(
                    secure_filename(file.filename),
                    file.content_type,
                    file.read()
                )
        print(user_password)
        mail.send(msg)
        print("----------")
        return jsonify({'success': True, 'message': 'Email sent successfully'}), 200

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500
      
      

GOOGLE_CLIENT_ID = "879058051169-g9kgb1fcnqurp8tv09scgv1d6631gdi6.apps.googleusercontent.com"
@main_bp.route('/accounts/google', methods=['POST'])
def google_sso():
    print("entering /api/accounts/google")
    data = request.get_json()
    auth_token = data['auth_token']
    sso_user_credentials = id_token.verify_oauth2_token(
        auth_token,
        google_requests.Request(),
        clock_skew_in_seconds=300  # allows 5 minutes of skew
    )
    if sso_user_credentials['aud'] != GOOGLE_CLIENT_ID:
        return jsonify({"data": "invalid request"}), 400
    
    response_data = {}
    first_name, last_name = get_sso_user_fullname(sso_user_credentials)
    response_data["email"] = sso_user_credentials['email'].lower()
    response_data["first_name"] = first_name
    response_data["last_name"] = last_name
    
    result = get_user_info(response_data["email"],response_data["first_name"], response_data["last_name"])
    return result


def get_sso_user_fullname(sso_user_credentials):
        first_name = ""
        last_name = ""
        full_name_list = []
        try:
            first_name = sso_user_credentials["given_name"]
        except Exception as e:
            print(e)
            temp1 = sso_user_credentials["name"]
            full_name_list = temp1.split(" ")
            first_name = full_name_list[0]
        try:
            last_name = sso_user_credentials["family_name"]
        except Exception as e:
                print(e)
                try:
                    last_name = full_name_list[1]
                except Exception as e:
                    print(e)
                    last_name = ""

        return first_name, last_name

@main_bp.route('/logsheet/new_logsheet', methods=['POST'])
def new_log_sheet():
  data = request.get_json()
  log_type = data.get("type")
  email = data.get("email")
  detail = data.get("detail")
  print(detail)
  result = new_logsheet(log_type,email,detail)
  
  return jsonify({"data": result})

@main_bp.route('/logsheet/get_all_data', methods=['POST'])
def get_all_sheet_data():
  data = request.get_json()
  email = data.get("email")
  result = get_all_data(email)
  return result


@main_bp.route('/notification/get_all_data', methods=['POST'])
def get_all_notification_data_logs_table():
  data = request.get_json()
  email = data.get("email")
  print(email)
  result = get_all_notification(email)
  return result


@main_bp.route('/notification/user_intervention', methods=['POST'])
def user_intervention():
  data = request.get_json()
  type = data.get("type")
  incoterm = data.get("incoterm")
  dateFormat = data.get("dateFormat")
  dn = data.get("DN#")
  result = update_notification(dn,type,incoterm,dateFormat)
  return result