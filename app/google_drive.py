import os
import io
import re
import time
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from googleapiclient.errors import HttpError

from app.env import SERVICE_ACCOUNT_FILE,FOLDER_ID
# SERVICE_ACCOUNT_FILE
# from app.email_handle.new_email_handler import google_drive_check_for_new_turn
# If modifying the scopes, delete the file token.json

# SERVICE_ACCOUNT_FILE = r'E:\monte_working\Menarini-backend-main\Menarini-backend-main\app\email_handle\neon-rite-449718-m4-0a3e2d4992a8.json'  # Path to your service account JSON file
# FOLDER_ID = '1dIgQL8iZKT2EMT_bBDtJCI1PSlfei0FG' 


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, SERVICE_ACCOUNT_FILE)
FOLDER_ID = FOLDER_ID

SCOPES = ['https://www.googleapis.com/auth/drive.file']
def authenticate_gdrive():
    """Authenticate and return the Google Drive API service using a service account."""
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    # Build the Drive API service
    service = build('drive', 'v3', credentials=creds)
    return service


def get_files_from_drive(supplier_domain_folder, supplier_folder_name, DN_folder_name):
    service = authenticate_gdrive()

    # Get the list of folders
    results = service.files().list(q="mimeType = 'application/vnd.google-apps.folder'").execute()
    items = results.get('files', [])

    # Find the supplier folder ID
    supplier_domain_id = None
    for item in items:
        if item['name'] == supplier_domain_folder:
            supplier_domain_id = item['id']
            break
    if not supplier_domain_id:
        print("Not found Supplier Domain")
        return []
    
    results = service.files().list(q=f"'{supplier_domain_id}' in parents and mimeType = 'application/vnd.google-apps.folder'").execute()
    items = results.get('files', [])
    supplier_folder_id = None
    for item in items:
        if item['name'] == supplier_folder_name:
            supplier_folder_id = item['id']
            break

    if not supplier_folder_id:
        print(f"Supplier folder '{supplier_folder_name}' not found.")
        return []

    # Find the DN folder inside the supplier folder
    results = service.files().list(q=f"'{supplier_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder'").execute()
    items = results.get('files', [])
    DN_folder_id = None
    for item in items:
        if item['name'] == DN_folder_name:
            DN_folder_id = item['id']
            break

    if not DN_folder_id:
        print(f"DN folder '{DN_folder_name}' not found in '{supplier_folder_name}'.")
        return []

    # Get files in the DN folder
    results = service.files().list(q=f"'{DN_folder_id}' in parents").execute()
    items = results.get('files', [])

    # Return file details as an array
    if not items:
        print(f"No files found in '{DN_folder_name}'.")
        return []

    file_details = []
    for item in items:
        file_details.append({
            'name': item['name'],
            'id': item['id'],
            'mimeType': item['mimeType']
        })

    return file_details


def sanitize_filename(filename):
    """Removes special characters from filenames."""
    return re.sub(r'[<>:"/\\|?*]', '_', filename)

def delete_file_from_drive(file_id):
    """Deletes a file from Google Drive by file ID."""
    service = authenticate_gdrive()
    try:
        service.files().delete(fileId=file_id).execute()
        print(f"✅ File with ID {file_id} deleted successfully.")
        return True
    except HttpError as error:
        print(f"❌ An error occurred while deleting file ID {file_id}: {error}")
        return False

def download_file_from_drive(file_id, filename,DOWNLOAD_FOLDER):
    """Downloads a file from Google Drive given its file_id."""
    try:
        service = authenticate_gdrive()
        request = service.files().get_media(fileId=file_id)
        file_data = io.BytesIO()

        downloader = MediaIoBaseDownload(file_data, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        # Ensure download folder exists
        os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

        # Sanitize filename
        sanitized_filename = sanitize_filename(filename)

        # Set the local path for saving the file
        local_path = os.path.join(DOWNLOAD_FOLDER, sanitized_filename)

        # Save the downloaded file
        with open(local_path, "wb") as f:
            f.write(file_data.getvalue())

        print(f"File downloaded successfully: {local_path}")
    except Exception as e:
        print(f"Error downloading file {file_id}: {e}")
        




def get_parent_folder_name(service, file_id):
    """
    Given a file or folder ID, return the name of its immediate parent folder.
    """
    file_metadata = service.files().get(fileId=file_id, fields="parents").execute()

    parent_ids = file_metadata.get('parents', [])
    if not parent_ids:
        print(f"No parent found for file ID: {file_id}")
        return None

    # Get the immediate parent folder ID
    parent_id = parent_ids[0]

    # Fetch the parent folder details
    parent_metadata = service.files().get(fileId=parent_id, fields="name").execute()
    
    return parent_metadata.get('name', None)


def get_grandparent_folder_name(service, folder_id):
    """
    Given a folder ID, return the name of its parent folder's parent folder.
    """
    # Get the first parent folder (just like before)
    parent_folder_name = get_parent_folder_name(service, folder_id)
    
    if not parent_folder_name:
        return None  # No parent folder found, so no grandparent folder
    
    # Now, get the parent of the parent folder
    return get_parent_folder_name(service, parent_folder_name)
def process_uploaded_item(service, uploaded_item_id):
    """
    Process the uploaded file or folder and extract relevant information.
    """
    try:
        # Get the file or folder metadata
        uploaded_item = service.files().get(fileId=uploaded_item_id, fields="id, name, mimeType, parents").execute()
        if uploaded_item['mimeType'] == 'application/vnd.google-apps.folder':
            # If it's a folder, get the folder's name and its parent folder
            folder_name = uploaded_item['name']
            parent_folder_id = uploaded_item.get('parents', [None])[0]  # Get immediate parent folder ID
            parent_folder_name = get_parent_folder_name(service, uploaded_item_id)
            return parent_folder_name, folder_name
        else:
            # If it's a file, get the file's name and its parent folder
            file_name = uploaded_item['name']
            parent_folder_id = uploaded_item.get('parents', [None])[0]  # Get immediate parent folder ID
            dn_folder_name = get_parent_folder_name(service, uploaded_item_id)

            if parent_folder_id:
                folder = service.files().get(
                    fileId=parent_folder_id,
                    fields='id, name, parents'
                ).execute()
                supplier_name_folder_id = folder.get('parents', [None])[0]
                supplier_name_folder = get_parent_folder_name(service, parent_folder_id)  # Get grandparent
                if supplier_name_folder_id:   
                    supplier_domain_name_folder = get_parent_folder_name(service, supplier_name_folder_id)
                return supplier_domain_name_folder, supplier_name_folder, dn_folder_name
            else:
                print(f"File '{file_name}' has no parent folder.")
    except HttpError as error:
        if error.resp.status == 404:
            print(f"File or folder with ID {uploaded_item_id} not found.")
        else:
            print(f"Error processing item with ID {uploaded_item_id}: {error}")


def get_start_page_token(service):
    """Retrieve the start page token for tracking changes in Google Drive."""
    response = service.changes().getStartPageToken().execute()
    return response.get('startPageToken')

def detect_drive_changes(service, page_token):
    """
    Detect any changes in Google Drive.
    """
    if not page_token:
        page_token = get_start_page_token(service)  # Get initial token

    results = service.changes().list(
        pageToken=page_token,
        spaces='drive',
        includeItemsFromAllDrives=True,
        supportsAllDrives=True
    ).execute()

    processed_items = []
    changes = results.get('changes', [])

    if not changes:
        print("No changes found.")
    else:
        for change in changes:
            file = change.get('file')
            removed = change.get('removed', False)
            file_id = change.get('fileId')

            if removed:
                print(f"File was removed: {file_id}")
                continue

            if file:
                uploaded_item_id = file['id']
                result = process_uploaded_item(service, uploaded_item_id)
                if result:
                    processed_items.append(result)
            else:
                print(f"File metadata not found for ID: {file_id}. It might have been deleted or is inaccessible.")

    # Return next page token for future requests
    new_page_token = results.get('newStartPageToken', page_token)
    return (processed_items if processed_items else (None, None)), new_page_token

def detect_and_process_uploads():
    """
    Detect Google Drive changes and process uploads every 5 minutes.
    """
    # service = authenticate_gdrive()
    # print("-------------------------------------------")
    # page_token = None
    # while True:
    #     print("Checking for changes in Google Drive...")
    #     # If page_token is None, the first call will be fine, otherwise continue with the next page
    #     result = detect_drive_changes(service, page_token)
    #     print(result)
    #     # Wait 5 minutes (300 seconds) before checking again
    #     time.sleep(60)  # Sleep for 5 minutes

def rename_matching_drive_files(supplier_domain, supplier_name, dn, old_doc_list, new_doc_list):
    try:
        service = authenticate_gdrive()
        print("-----------------")
        # Traverse folder structure: supplier_domain → supplier_name → dn
        folder_names = [supplier_domain, supplier_name, dn]
        parent_id = None

        for folder_name in folder_names:
            query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}'"
            if parent_id:
                query += f" and '{parent_id}' in parents"
            results = service.files().list(q=query, spaces='drive', fields="files(id, name)").execute()
            folders = results.get('files', [])
            if not folders:
                print(f"Folder '{folder_name}' not found.")
                return
            parent_id = folders[0]['id']

        # parent_id now points to the `dn` folder
        folder_id = parent_id

        # List all files in the DN folder
        file_results = service.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            fields="files(id, name)"
        ).execute()
        files = file_results.get('files', [])

        for file in files:
            file_name = file["name"]

            # Find document part between the first pair of !
            match = re.search(r'!(.*?)!', file_name)
            if match:
                doc_part = match.group(1).strip()  # Normalize spaces in the extracted doc part

                # Normalize old_doc_list by trimming spaces and removing surrounding ampersands
                old_doc_list_normalized = old_doc_list.strip().replace(" ", "").lstrip("&").rstrip("&")

                # Normalize doc_part by trimming spaces and removing surrounding ampersands
                doc_part_normalized = doc_part.strip().replace(" ", "").lstrip("&").rstrip("&")

                # Print debug info
                print(f"Found: '{doc_part_normalized}' → Normalized: '{doc_part_normalized}' | Expected: '{old_doc_list_normalized}'")

                if doc_part_normalized == old_doc_list_normalized:
                    # Replace old doc list with new doc list
                    new_name = re.sub(r'!(.*?)!', f"!{new_doc_list}!", file_name)  # Only replace the document part

                    # Rename the file
                    try:
                        service.files().update(
                            fileId=file["id"],
                            body={"name": new_name}
                        ).execute()
                        print(f"Renamed: '{file_name}' → '{new_name}'")
                    except HttpError as error:
                        if error.resp.status == 404:
                            print(f"File with ID {file['id']} not found for renaming.")
                        else:
                            print(f"Error renaming file {file['id']}: {error}")
                else:
                    print(f"Skipping (no match): {file_name} (Found: '{doc_part_normalized}' → Normalized: '{doc_part_normalized}' | Expected: '{old_doc_list_normalized}')")
            else:
                print(f"No doc type section found in: {file_name}")

    except HttpError as error:
        print(f"Google Drive API error: {error}")


def rename_supplier_folder(service, domain, old_vendor_name, new_vendor_name):
    try:
        # Step 1: Find domain folder
        domain_query = f"mimeType='application/vnd.google-apps.folder' and name='{domain}' and trashed=false"
        domain_results = service.files().list(q=domain_query, spaces='drive', fields="files(id, name)").execute()
        domain_folders = domain_results.get('files', [])
        if not domain_folders:
            print(f"Domain folder '{domain}' not found.")
            return
        domain_id = domain_folders[0]['id']

        # Step 2: Find old_vendor_name folder under domain
        vendor_query = f"mimeType='application/vnd.google-apps.folder' and name='{old_vendor_name}' and '{domain_id}' in parents and trashed=false"
        vendor_results = service.files().list(q=vendor_query, spaces='drive', fields="files(id, name)").execute()
        vendor_folders = vendor_results.get('files', [])
        if not vendor_folders:
            print(f"Vendor folder '{old_vendor_name}' not found under domain '{domain}'.")
            return
        vendor_folder = vendor_folders[0]

        # Step 3: Rename the folder
        service.files().update(
            fileId=vendor_folder['id'],
            body={"name": new_vendor_name}
        ).execute()
        print(f"Renamed folder: '{old_vendor_name}' → '{new_vendor_name}'")
    except HttpError as error:
        print(f"Google Drive API error: {error}")


def get_specific_file(service, supplier_domain, supplier_name, dn, target_filename):
    """
    Get the specific file in the path: supplier_domain / supplier_name / dn / target_filename
    """
    files = get_files_from_drive(supplier_domain, supplier_name, dn)
    for file in files:
        if file["name"] == target_filename:
            print(f"File found: {file['name']}")
            return file  # contains 'id', 'name', 'mimeType'
    print(f"File '{target_filename}' not found in path {supplier_domain}/{supplier_name}/{dn}")
    return None


def google_drive_supplier_name_change(old_domain, old_name, new_domain, new_name, dn):
    """
    Moves a DN folder from old location to new location in Google Drive.
    Creates new domain and name folders if they don't exist.
    
    Args:
        old_domain (str): The old domain folder name
        old_name (str): The old supplier name folder
        new_domain (str): The new domain folder name
        new_name (str): The new supplier name folder
        dn (str): The DN folder name to move
    """
    service = authenticate_gdrive()
    
    print("-------------------------------")
    # Step 1: Find the old DN folder
    # Get the old domain folder ID
    old_domain_query = f"name='{old_domain}' and mimeType='application/vnd.google-apps.folder' and '{FOLDER_ID}' in parents"
    print(service.files)
    old_domain_results = service.files().list(q=old_domain_query, fields="files(id,name)").execute()
    old_domain_folders = old_domain_results.get('files', [])
    
    if not old_domain_folders:
        print(f"Old domain folder '{old_domain}' not found.")
        return False
    
    old_domain_id = old_domain_folders[0]['id']
    
    
    
    # Get the old name folder ID
    old_name_query = f"name='{old_name}' and mimeType='application/vnd.google-apps.folder' and '{old_domain_id}' in parents"
    old_name_results = service.files().list(q=old_name_query, fields="files(id,name)").execute()
    old_name_folders = old_name_results.get('files', [])
    
    if not old_name_folders:
        print(f"Old name folder '{old_name}' not found in domain '{old_domain}'.")
        return False
    
    old_name_id = old_name_folders[0]['id']
    
    # Get the DN folder ID
    dn_query = f"name='{dn}' and mimeType='application/vnd.google-apps.folder' and '{old_name_id}' in parents"
    dn_results = service.files().list(q=dn_query, fields="files(id,name)").execute()
    dn_folders = dn_results.get('files', [])
    
    if not dn_folders:
        print(f"DN folder '{dn}' not found in '{old_name}'.")
        return False
    
    dn_id = dn_folders[0]['id']
    
    # Step 2: Create new domain and name folders if they don't exist
    # Check if new domain exists, create if not
    new_domain_query = f"name='{new_domain}' and mimeType='application/vnd.google-apps.folder' and '{FOLDER_ID}' in parents"
    new_domain_results = service.files().list(q=new_domain_query, fields="files(id,name)").execute()
    new_domain_folders = new_domain_results.get('files', [])
    
    if new_domain_folders:
        new_domain_id = new_domain_folders[0]['id']
    else:
        # Create new domain folder
        new_domain_metadata = {
            'name': new_domain,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [FOLDER_ID]
        }
        new_domain_folder = service.files().create(body=new_domain_metadata, fields='id').execute()
        new_domain_id = new_domain_folder['id']
        print(f"Created new domain folder '{new_domain}' with ID: {new_domain_id}")
    
    # Check if new name exists in new domain, create if not
    new_name_query = f"name='{new_name}' and mimeType='application/vnd.google-apps.folder' and '{new_domain_id}' in parents"
    new_name_results = service.files().list(q=new_name_query, fields="files(id,name)").execute()
    new_name_folders = new_name_results.get('files', [])
    
    if new_name_folders:
        new_name_id = new_name_folders[0]['id']
    else:
        # Create new name folder
        new_name_metadata = {
            'name': new_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [new_domain_id]
        }
        new_name_folder = service.files().create(body=new_name_metadata, fields='id').execute()
        new_name_id = new_name_folder['id']
        print(f"Created new name folder '{new_name}' with ID: {new_name_id}")
    
    # Step 3: Move the DN folder to the new location
    try:
        # First remove the old parent
        service.files().update(
            fileId=dn_id,
            addParents=new_name_id,
            removeParents=old_name_id,
            fields='id,parents'
        ).execute()
        
        print(f"Moved DN folder '{dn}' from '{old_domain}/{old_name}' to '{new_domain}/{new_name}'")
        return True
    except Exception as e:
        print(f"Error moving DN folder: {str(e)}")
        return False
   