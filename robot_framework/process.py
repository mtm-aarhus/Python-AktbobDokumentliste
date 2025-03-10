"""This module contains the main process of the robot."""


###### dræb excel inden det sættes i gang - se på om excel holdes åbent ##########################

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement
import os
import pandas as pd
import re
import xml.etree.ElementTree as ET
import requests
import json
from urllib.parse import quote
from datetime import datetime, timedelta
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.sharing.links.kind import SharingLinkKind
from office365.sharepoint.webs.web import Web
from requests_ntlm import HttpNtlmAuth
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.styles import Alignment, Font, Protection
import smtplib
from email.message import EmailMessage
from PIL import ImageFont, ImageDraw, Image
import pytz
import uuid

# pylint: disable-next=unused-argument
def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement | None = None) -> None:
    """Do the primary process of the robot."""
    orchestrator_connection.log_trace("Running process.")

    log = True
    send_email = True

    #Getting credentials
    API_url = orchestrator_connection.get_constant("AktbobSharePointURL").value
    API_credentials = orchestrator_connection.get_credential("AktbobAPIKey")
    API_username = API_credentials.username
    API_password = API_credentials.password

    #Define developer mail
    UdviklerMail = orchestrator_connection.get_constant("balas").value


    #Get Robot Credentials
    RobotCredentials = orchestrator_connection.get_credential("Robot365User")
    RobotUsername = RobotCredentials.username
    RobotPassword = RobotCredentials.password

    queue_json = json.loads(queue_element.data)

    # Retrieve elements from queue_json
    SagsID = str(queue_json["SagsNummer"])
    MailModtager = str(queue_json["Email"])
    PodioID = str(queue_json["PodioID"])
    DeskProID = str(queue_json["DeskproID"])
    DeskProTitel = str(queue_json["Titel"])
    orchestrator_connection.log_info(f'Processing {SagsID} in {DeskProTitel}')
    #Determining if it is a Nova-case or not
    pattern = r"^[A-Z]{3}-\d{4}-\d{6}"

    if re.match(pattern, SagsID):
        GeoSag = True
        NovaSag = False  
        GOAPILIVECRED = orchestrator_connection.get_credential("GOAktApiUser")
        GOAPILIVECRED_username = GOAPILIVECRED.username
        GOAPILIVECRED_password = GOAPILIVECRED.password
        GOAPI_URL = orchestrator_connection.get_constant('GOApiURL').value
    else:
        NovaSag = True
        GeoSag = False  

        def GetKMDToken(orchestrator_connection: OrchestratorConnection):
        
            TokenTimeStamp = orchestrator_connection.get_constant("KMDTokenTimestamp").value
            KMD_access = orchestrator_connection.get_credential("KMDAccessToken")
            KMD_access_token = KMD_access.password
            KMD_URL = KMD_access.username
        
            # Define Danish timezone
            danish_timezone = pytz.timezone("Europe/Copenhagen")
        
            # Parse the old timestamp to a datetime object
            old_time = datetime.strptime(TokenTimeStamp.strip(), "%d-%m-%Y %H:%M:%S")
            old_time = danish_timezone.localize(old_time)  # Localize to Danish timezone
            print('Old timestamp: ' + old_time.strftime("%d-%m-%Y %H:%M:%S"))
        
            # Get the current timestamp in Danish timezone
            current_time = datetime.now(danish_timezone)
            print('current timestamp: '+current_time.strftime("%d-%m-%Y %H:%M:%S"))
            str_current_time = current_time.strftime("%d-%m-%Y %H:%M:%S")
        
            # Calculate the difference between the two timestamps
            time_difference = current_time - old_time
            print(time_difference)
        
            # Check if the difference is over 1 hour and 30 minutes
            GetNewTimeStamp = time_difference > timedelta(hours=1, minutes=30)
        
            # Output for the boolean
            print("GetNewTimeStamp:", GetNewTimeStamp)
        
            # Example of using it in an if-statement
            if GetNewTimeStamp:
                print("The difference is over 1 hour and 30 minutes. Fetch a new timestamp!")
                # Replace these values with your actual keys
                client_id = 'aarhus_kommune'
                client_secret = 'lottNjMyx07BBfEzkVx5P2HwPWpvz2sG'
                scope = 'client'
                grant_type = 'client_credentials'
        
        
                # Data to be sent in the POST request
                keys = {
                    'client_id': client_id,
                    'client_secret': client_secret,
                    'scope': scope,
                    'grant_type': grant_type,  # Specify the grant type you're using
                }
        
                # Sending POST request to get the access token
                response = requests.post(KMD_URL, data=keys)
        
                # Check if the request was successful (status code 200)
                if response.status_code == 200:
                    KMD_access_token = response.json().get('access_token')
                    print("Access token granted")
                    orchestrator_connection.update_credential("KMDAccessToken",KMD_URL,KMD_access_token)
                    orchestrator_connection.update_constant("KMDTokenTimestamp",str_current_time)
            
                    return KMD_access_token
                else:
                    print("Failed to get the access token")
        
            else:
                print("No need to fetch a new timestamp - using old timestamp")
                return KMD_access_token
        KMD_access_token = GetKMDToken(orchestrator_connection)   
        NOVA_URL = orchestrator_connection.get_constant("KMDNovaURL").value

    #Assigning different URL's depending on case type
    if NovaSag:
        Document_url = NOVA_URL + "/Document/GetList?api-version=2.0-Case"
        Case_url = NOVA_URL + "/Case/GetList?api-version=2.0-Case"
        id = str(uuid.uuid4())
    else:
        url = GOAPI_URL + "/_goapi/Cases/Metadata/" + SagsID

    if log:
        orchestrator_connection.log_info("Process starter")

    # Create session with NTLM authentication
    session = requests.Session()
    if GeoSag:
        session.auth = HttpNtlmAuth(GOAPILIVECRED_username, GOAPILIVECRED_password)
        session.headers.update({"Content-Type": "application/json"})
        response = session.get(url, timeout=500)
        response.raise_for_status()

        # Process the response content directly (assuming response.status_code == 200)
        SagMetaData = response.text
        json_obj = json.loads(SagMetaData)

        # Extract the "Metadata" field from the JSON response
        metadata_xml = json_obj.get("Metadata")
        if metadata_xml:
            # Parse the XML string
            xdoc = ET.fromstring(metadata_xml)

            # Extract attributes
            SagsURL = xdoc.attrib.get("ows_CaseUrl")
            SagsTitel = xdoc.attrib.get("ows_Title")

            # Process SagsURL
            if SagsURL and "cases/" in SagsURL:
                # Split SagsURL by "cases/" and take the second part
                Akt = SagsURL.split("cases/")[1].split("/")[0]
            else:
                print("Error: 'cases/' not found in SagsURL or SagsURL is missing.")
        else:
            print("Error: 'Metadata' field is missing in the JSON response.")
    if NovaSag:
        payload = json.dumps({
        "common": {
            "transactionId": "6630880c-e5e9-4b9f-b348-884af571a69b"
        },
        "paging": {
            "startRow": 1,
            "numberOfRows": 5
        },
        "CASEATTRIBUTES": {
            "USERFRIENDLYCASENUMBER": SagsID
        },
        "caseGetOutput": {
            "caseAttributes": {
            "title": True,
            "userFriendlyCaseNumber": True,
            "numberOfDocuments": True
            }
        }
        })
        headers = {
        "Authorization": f"Bearer {KMD_access_token}",
        'Content-Type': 'application/json'
        }

        response = requests.request("PUT", Case_url, headers=headers, data=payload)
        response.raise_for_status()

        # Process the response content directly (assuming response.status_code == 200)
        SagMetaData = response.text
        json_obj = json.loads(SagMetaData)

        SagsTitel = json_obj['cases'][0]['caseAttributes']['title']
        SagsURL = "" #SagsURL is nothing for now due to the setup in nova - potentially add later

    # Send GET request    
    if log:
        orchestrator_connection.log_info("Getting metadata")

    # Removal of illegal characters and double spaces
    pattern = r'[~#%&*{}\:\\<>?/+|\"\'\t\[\]`^@=!$();\€£¥₹]'
    SagsTitel = re.sub(pattern, '', str(SagsTitel))
    SagsTitel = " ".join(SagsTitel.split())

    # Define the structure of the DataTable
    columns = [
        "Akt ID", "Dok ID", "Dokumenttitel", "Dokumentkategori", "Dokumentdato", 
        "Bilag til Dok ID", "Bilag", "Link til dokument", 
        "Omfattet af ansøgningen? (Ja/Nej)", "Gives der aktindsigt i dokumentet? (Ja/Nej/Delvis)", 
        "Begrundelse hvis nej eller delvis"
    ]

    # Create an empty DataFrame with these columns
    data_table = pd.DataFrame(columns=columns)

    if log:
        orchestrator_connection.log_info("Sagsurl" + SagsURL)
    if GeoSag:
        orchestrator_connection.log_info("Processing GEO case")
        Akt = SagsURL.split("/")[1]  
        
        if log:
            orchestrator_connection.log_info("Akt" + Akt)# Constructing the URL
        # Replacing '-' with '%2D' in SagsID
        encoded_sags_id = SagsID.replace("-", "%2D")
        ListURL = f"%27%2Fcases%2F{Akt}%2F{encoded_sags_id}%2FDokumenter%27"
        
        if log:
            orchestrator_connection.log_info("ListURL: " + ListURL)
        
        # Initialize variables
        ViewId = None
        view_ids_to_use = []  # To handle combined views
        response = session.get(f"{GOAPI_URL}/{SagsURL}/_goapi/Administration/GetLeftMenuCounter")
        ViewsIDArray = json.loads(response.text) # Parse the JSON

        # Check for "UdenMapper.aspx"
        for item in ViewsIDArray:
            if item["ViewName"] == "UdenMapper.aspx":
                ViewId = item["ViewId"]
                break
            elif item["ViewName"] == "Ikkejournaliseret.aspx":
                ikke_journaliseret_id = item["ViewId"]
            elif item["ViewName"] == "Journaliseret.aspx":
                journaliseret_id = item["ViewId"]

        # If "UdenMapper.aspx" doesn't exist, combine views
        if ViewId is None:
            view_ids_to_use = [ikke_journaliseret_id, journaliseret_id]

        # Iterate through views
        for current_view_id in ([ViewId] if ViewId else view_ids_to_use):
            firstrun = True
            MorePages = True

            while MorePages:
                if log:
                    orchestrator_connection.log_info("Henter dokumentlister")

                # If not the first run, fetch the next page
                if not firstrun:
                    orchestrator_connection.log_info("Henter næste side i dokumentet")
                    url = f"{GOAPI_URL}/{SagsURL}/_api/web/GetList(@listUrl)/RenderListDataAsStream"
                    url_with_query = f"{url}?@listUrl={ListURL}{NextHref.replace('?', '&')}"

                    response = session.post(url_with_query, timeout=500)
                    response.raise_for_status()
                    Dokumentliste = response.text  # Extract the content
                else:
                    # If first run, fetch the first page for the current view
                    url = f"{GOAPI_URL}/{SagsURL}/_api/web/GetList(@listUrl)/RenderListDataAsStream"
                    query_params = f"?@listUrl={ListURL}&View={current_view_id}"
                    full_url = url + query_params

                    response = session.post(full_url, timeout=500)
                    response.raise_for_status()
                    Dokumentliste = response.text  # Extract the content

                # Deserialize response
                dokumentliste_json = json.loads(Dokumentliste)
                dokumentliste_rows = dokumentliste_json.get("Row", [])

                # Check for additional pages
                NextHref = dokumentliste_json.get("NextHref")
                MorePages = "NextHref" in dokumentliste_json

                # Process each row
                for item in dokumentliste_rows:
                    # Extract and prepare data
                    DokumentURL = GOAPI_URL + quote(item.get("FileRef", ""), safe="/")
                    AktID = item.get("CaseRecordNumber", "").replace(".", "")
                    DokumentDato = str(item.get("Dato"))
                    Dokumenttitel = item.get("Title", "")
                    DokID = str(item.get("DocID"))
                    DokumentKategori = str(item.get("Korrespondance"))

                    if len(Dokumenttitel) < 2:
                        Dokumenttitel = item.get("FileLeafRef.Name", "")

                    if log:
                        orchestrator_connection.log_info(f"AktID: {AktID}")

                    # Fetch parents and children data
                    parents_response = session.get(f"{GOAPI_URL}/_goapi/Documents/Parents/{DokID}", timeout=500)
                    parents_object = json.loads(parents_response.text)
                    ParentArray = parents_object.get("ParentsData", [])
                    Bilag = ", ".join(str(currentItem.get("DocumentId", "")) for currentItem in ParentArray)

                    children_response = session.get(f"{GOAPI_URL}/_goapi/Documents/Children/{DokID}", timeout=500)
                    children_object = json.loads(children_response.text)
                    ChildrenArray = children_object.get("ChildrenData", [])
                    BilagChild = ", ".join(str(currentItem.get("DocumentId", "")) for currentItem in ChildrenArray)

                    # Append data to DataFrame
                    if "tunnel_marking" in Dokumenttitel.lower() or "memometadata" in Dokumenttitel.lower():
                        data_table = pd.concat([data_table, pd.DataFrame([{
                            "Akt ID": AktID,
                            "Dok ID": DokID,
                            "Dokumenttitel": Dokumenttitel,
                            "Dokumentkategori": DokumentKategori,
                            "Dokumentdato": DokumentDato,
                            "Bilag": BilagChild,
                            "Bilag til Dok ID": Bilag,
                            "Link til dokument": DokumentURL,
                            "Omfattet af ansøgningen? (Ja/Nej)": "Ja",
                            "Gives der aktindsigt i dokumentet? (Ja/Nej/Delvis)": "Nej",
                            "Begrundelse hvis nej eller delvis": "Tavshedsbelagte oplysninger - om private forhold"
                        }])], ignore_index=True)
                    else:
                        data_table = pd.concat([data_table, pd.DataFrame([{
                            "Akt ID": AktID,
                            "Dok ID": DokID,
                            "Dokumenttitel": Dokumenttitel,
                            "Dokumentkategori": DokumentKategori,
                            "Dokumentdato": DokumentDato,
                            "Bilag": BilagChild,
                            "Bilag til Dok ID": Bilag,
                            "Link til dokument": DokumentURL,
                            "Omfattet af ansøgningen? (Ja/Nej)": "Ja",
                            "Gives der aktindsigt i dokumentet? (Ja/Nej/Delvis)": "",
                            "Begrundelse hvis nej eller delvis": ""
                        }])], ignore_index=True)

                firstrun = False
    else:
        orchestrator_connection.log_info("Processing NOVA case")
        payload = json.dumps({
        "common": {
            "transactionId": id
        },
        "paging": {
            "startRow": 0,
            "numberOfRows": 10000,
            "calculateTotalNumberOfRows": True
        },
        "caseNumber": SagsID,
        "getOutput": {
            "documentType": True,
            "title": True,
            "caseWorker": True,
            "description": True, 
            "fileExtension": True,
            "approved": True, 
            "acceptReceived": True,
            "documentDate": True
        }
        })
        headers = {
        "Authorization": f"Bearer {KMD_access_token}",
        'Content-Type': 'application/json'
        }

        response = requests.request("PUT", Document_url, headers=headers, data=payload)
        response.raise_for_status
        aktid_number = 1
        documents = json.loads(response.text)['documents']
        # Process each row
        for i in range(len(documents)):
            # Extract and prepare data
            DokumentURL = ""
            AktID = aktid_number
            DokumentDato = str(documents[i]['documentDate'])
            date_object = datetime.strptime(DokumentDato, "%Y-%m-%dT%H:%M:%S")
            formatted_date = str(date_object.strftime("%d-%m-%Y"))
            Dokumenttitel = documents[i]['title']
            DokID = documents[i]['documentNumber']
            DokumentKategori = documents[i]['documentType']

            # Append data to DataFrame
            if "tunnel_marking" in Dokumenttitel.lower() or "memometadata" in Dokumenttitel.lower() or "fletteliste" in Dokumenttitel.lower():
                data_table = pd.concat([data_table, pd.DataFrame([{
                    "Akt ID": AktID,
                    "Dok ID": DokID,
                    "Dokumenttitel": Dokumenttitel,
                    "Dokumentkategori": DokumentKategori,
                    "Dokumentdato": formatted_date,
                    "Bilag": "",
                    "Bilag til Dok ID": "",
                    "Link til dokument": DokumentURL,
                    "Omfattet af ansøgningen? (Ja/Nej)": "Ja",
                    "Gives der aktindsigt i dokumentet? (Ja/Nej/Delvis)": "Nej",
                    "Begrundelse hvis nej eller delvis": "Tavshedsbelagte oplysninger - om private forhold"
                }])], ignore_index=True)
            else:
                data_table = pd.concat([data_table, pd.DataFrame([{
                    "Akt ID": AktID,
                    "Dok ID": DokID,
                    "Dokumenttitel": Dokumenttitel,
                    "Dokumentkategori": DokumentKategori,
                    "Dokumentdato": formatted_date,
                    "Bilag": "",
                    "Bilag til Dok ID": "",
                    "Link til dokument": DokumentURL,
                    "Omfattet af ansøgningen? (Ja/Nej)": "Ja",
                    "Gives der aktindsigt i dokumentet? (Ja/Nej/Delvis)": "",
                    "Begrundelse hvis nej eller delvis": ""
                }])], ignore_index=True)
            aktid_number += 1

    ## Convert 'Akt ID' to string, strip spaces, then convert to numeric
    data_table['Akt ID'] = pd.to_numeric(data_table['Akt ID'].astype(str).str.strip(), errors='coerce')

    if not data_table.empty:
        data_table = data_table.sort_values(by='Akt ID', ascending=True, ignore_index=True)

    data_table.to_excel(excel_file_path, index=False, sheet_name="Sagsoversigt")

    # Save the pandas DataFrame to Excel
    excel_file_path = f"{SagsID}_{datetime.now().strftime('%d-%m-%Y')}.xlsx"
    data_table.to_excel(excel_file_path, index=False, sheet_name="Sagsoversigt")

    # Define the font path and size
    FONT_PATH = "calibri.ttf"  # Replace with the path to your Calibri or desired font
    FONT_SIZE = 11

    # Load the font
    try:
        font = ImageFont.truetype(FONT_PATH, FONT_SIZE)
    except OSError:
        raise FileNotFoundError(f"Font file not found at {FONT_PATH}. Please ensure the font file is available.")

    # Function to calculate text dimensions in Excel units
    def calculate_text_dimensions(text, font, max_width_in_pixels):
        # Create a dummy image for text measurement
        dummy_image = Image.new("RGB", (1, 1))
        draw = ImageDraw.Draw(dummy_image)

        # Measure text size
        bbox = draw.textbbox((0, 0), text, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]


        # Convert pixel width to approximate Excel column width
        excel_column_width = text_width / 5  # Adjust 7 as needed based on font and testing

        # Calculate row height based on text wrapping
        lines = max(1, text_width // max_width_in_pixels + 1)
        excel_row_height = lines * (text_height / 1.33)  # Approximate scaling for Excel row height

        return excel_column_width, excel_row_height

    # Open the Excel file for further formatting

    workbook = load_workbook(excel_file_path)
    worksheet = workbook["Sagsoversigt"]

    # Adjust column widths dynamically
    max_width_in_pixels = 382  # Adjust based on your target column width in pixels

    for col_idx, column_cells in enumerate(worksheet.columns, start=1):
        max_length = 0
        for cell in column_cells:
            if cell.value:  # Only consider cells with a value
                # Measure text size using Pillow
                text = str(cell.value)
                column_width, _ = calculate_text_dimensions(text, font, max_width_in_pixels)
                max_length = max(max_length, column_width)

        adjusted_width = min(max_length + 4, 50)  # Add padding and limit maximum width
        worksheet.column_dimensions[get_column_letter(col_idx)].width = adjusted_width

    COLUMN_C_INDEX = 3  # Column C (Dokumenttitel)
    COLUMN_G_INDEX = 7  # Column G
    MAX_COLUMN_C_WIDTH = 50  # Maximum width for Column C in Excel units
    PIXELS_PER_EXCEL_UNIT = 7  # Conversion factor from Excel width to pixels
    ROW_HEIGHT_PER_PIXEL = 0.75  # Conversion factor from pixels to Excel row height units

    # Convert maximum column width to pixels
    max_width_in_pixels = MAX_COLUMN_C_WIDTH * PIXELS_PER_EXCEL_UNIT

    # Set column width for Column C
    worksheet.column_dimensions[get_column_letter(COLUMN_C_INDEX)].width = MAX_COLUMN_C_WIDTH

    # Apply table formatting
    data_range = f"A1:K{worksheet.max_row}"  # Adjust the range to include all data
    table = Table(displayName="SagsoversigtTable", ref=data_range)

    style = TableStyleInfo(
        name="TableStyleMedium2",
        showFirstColumn=False,
        showLastColumn=False,
        showRowStripes=True,
        showColumnStripes=False
    )
    table.tableStyleInfo = style
    worksheet.add_table(table)
    # Define a white font for the header row
    header_font = Font(name="Calibri", size=11, bold=False, color="FFFFFF")  # White text

    # Apply header styling and lock all cells by default
    for row_idx, row in enumerate(worksheet.iter_rows(), start=1):
        for cell in row:
            if row_idx == 1:  # Header row
                cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)  # Align text
                cell.font = header_font  # Apply white font
            else:  # Non-header rows
                cell.alignment = Alignment(wrap_text=True)  # Wrap text for all rows
                current_font = cell.font
                cell.font = Font(
                    name=current_font.name if current_font.name else "Calibri",
                    size=11,
                    italic=current_font.italic,
                    vertAlign=current_font.vertAlign,
                    underline=current_font.underline,
                    strike=current_font.strike,
                    color=current_font.color
                )

            # Lock all cells by default
            cell.protection = Protection(locked=True)

        # Unlock cells with dropdown menus (columns I, J, and K)
        for col in ["I", "J", "K"]:
            for row_idx in range(2, worksheet.max_row + 1):  # Exclude the header row
                cell = worksheet[f"{col}{row_idx}"]
                cell.protection = Protection(locked=False)
        
    index = [4, 5, 7, 8, 9, 10, 11]
    words = [
            "Dokumentkategori  ", "Dokumentdato ", "Bilag   ",
            "Link til dokument", "Omfattet af ansøgningen? (Ja/Nej)",
            "Gives der aktindsigt i dokumentet (Ja/Nej/Delvis)",
            "Begrundelse hvis nej eller delvis"
        ]
    desired_widths = [len(word) + 3 for word in words]
    for i in range(len(index)):
        worksheet.column_dimensions[get_column_letter(index[i])].width = desired_widths[i]
    ROW_HEIGHT_PER_PIXEL = 1  # Approximate conversion factor for Excel row height units

    # Function to calculate row height dynamically
    def calculate_row_height(text, font, max_width_in_pixels, cg):
        # Create a dummy image for text measurement
        dummy_image = Image.new("RGB", (1, 1))
        draw = ImageDraw.Draw(dummy_image)

        # Measure text size
        bbox = draw.textbbox((0, 0), text, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        if cg == "c":
            max_width_in_pixels = 150
        else:
            max_width_in_pixels = 70
        # Calculate the number of lines needed to fit the text
        lines_required = max(1, (text_width / max_width_in_pixels)+1 )

        # Calculate the total height based on lines and single-line height
        total_height_in_pixels = lines_required * text_height

        # Convert pixels to Excel row height units
        return total_height_in_pixels * ROW_HEIGHT_PER_PIXEL

    # Iterate through rows and adjust row heights for Columns C and G
    for row_idx in range(2, worksheet.max_row + 1):  # Skip the header row
        row_height = 15  # Default row height

        # Process Column C
        cell_c = worksheet.cell(row=row_idx, column=COLUMN_C_INDEX)
        if cell_c.value:  # If cell has a value
            cell_c.alignment = Alignment(wrap_text=True)  # Enable text wrapping
            text_c = str(cell_c.value)
            height_c = calculate_row_height(text_c, font, max_width_in_pixels, "c")  # Calculate height
            row_height = max(row_height, height_c)
        
        # Process Column G
        cell_g = worksheet.cell(row=row_idx, column=COLUMN_G_INDEX)
        if cell_g.value and ',' in str(cell_g.value):  # If cell contains a comma
            cell_g.alignment = Alignment(wrap_text=True)  # Enable text wrapping
            text_g = '\n'.join(str(cell_g.value).split(', '))  # Replace commas with newlines
            cell_g.value = text_g  # Update cell value
            height_g = calculate_row_height(text_g, font, max_width_in_pixels, "g")  # Calculate height
            row_height = max(row_height, height_g)
        # Apply the calculated row height to the row
        worksheet.row_dimensions[row_idx].height = row_height

    # Add hyperlinks to URLs in column H
    for row_idx in range(2, worksheet.max_row + 1):  # Skip header row
        cell = worksheet.cell(row=row_idx, column=8)  # Column H
        url = cell.value
        if url:
            cell.value = "Dokumentlink"
            cell.hyperlink = url
            cell.style = "Hyperlink"

    # Add strict data validation for columns I, J, and K
    validation_i = DataValidation(type="list", formula1='"Ja,Nej"', allow_blank=False, showErrorMessage=True)
    validation_i.error = "Vælg venligt enten Ja eller Nej."
    validation_i.errorTitle = "Ugyldig værdi"
    worksheet.add_data_validation(validation_i)
    

    validation_j = DataValidation(type="list", formula1='"Ja,Delvis,Nej"', allow_blank=False, showErrorMessage=True)
    validation_j.error = "Vælg venligt enten Ja, Delvis eller Nej."
    validation_j.errorTitle = "Ugyldig værdi"
    worksheet.add_data_validation(validation_j)
    
    

    hidden_options = [
        "Internt dokument - ufærdigt arbejdsdokument",
        "Internt dokument - foreløbige og sagsforberedende overvejelser",
        "Internt dokument - del af intern beslutningsproces",
        "Andre dokumenter - korrespondance med sagkyndig rådgiver vedr. tvistsag",
        "Andre dokumenter - vedr. udførelse af sekretariatsopgave",
        "Andre dokumenter - Andet (uddybes i afgørelse)",
        "Tavshedsbelagte oplysninger - om private forhold",
        "Tavshedsbelagte oplysninger - om erhvervsmæssige forhold",
        "Tavshedsbelagte oplysninger - Andet (uddybes i afgørelsen)",
        " "
    ]

    hidden_sheet = workbook.create_sheet("VeryHidden")
    hidden_sheet.sheet_state = "veryHidden"
    for idx, option in enumerate(hidden_options, start=1):
        hidden_sheet.cell(row=idx, column=1, value=option)

    validation_k = DataValidation(
        type="list",
        formula1=f"=VeryHidden!$A$1:$A${len(hidden_options)}",
        allow_blank=False,
        showErrorMessage=True
    )
   
    validation_k.error = "Please select one of the provided options."
    validation_k.errorTitle = "Invalid Input"
    if worksheet.max_row > 2:
        validation_k.add(f"K2:K{worksheet.max_row}")
        validation_i.add(f"I2:I{worksheet.max_row}")
        validation_j.add(f"J2:J{worksheet.max_row}")
    
    worksheet.add_data_validation(validation_k)
    worksheet.protection.sheet = True
    worksheet.protection.password = "Aktbob"
    worksheet.protection.enable()

    # Save the formatted Excel file
    workbook.save(excel_file_path)

    Mappe1 = str(DeskProID) +" - " + str(DeskProTitel)
    Mappe2 = str(SagsID) + " - " + str(SagsTitel)

    # Authenticate to SharePoint using Office365 credentials
    credentials = UserCredential(RobotUsername, RobotPassword)
    ctx = ClientContext(API_url).with_credentials(credentials)

    # Function to sanitize folder names
    def sanitize_folder_name(folder_name):
        pattern = r'[.,~#%&*{}\[\]\\:<>?/+|$¤£€\"\t]'
        folder_name = re.sub(pattern, "", folder_name)
        folder_name = re.sub(r"\s+", " ", folder_name).strip()
        return folder_name

    # Sanitize folder names
    Mappe1 = sanitize_folder_name(Mappe1)
    Mappe2 = sanitize_folder_name(Mappe2)

    # Ensure folder names don't exceed length limits
    if len(Mappe1) > 99:
        Mappe1 = Mappe1[:95] + "(...)"
    if len(Mappe2) > 99:
        Mappe2 = Mappe2[:95] + "(...)"

    total_length = len(Mappe1) + len(Mappe2) + 17  # 17 is for folder structure overhead
    if total_length > 400:
        excess_length = total_length - 400
        half_excess = excess_length // 2
        Mappe1 = Mappe1[:len(Mappe1) - half_excess - 5] + "(...)"
        Mappe2 = Mappe2[:len(Mappe2) - half_excess - 5] + "(...)"

    parent_folder_name = API_url.split(".com")[-1] + "/Delte dokumenter/Dokumentlister" 

        # Create main folder
    root_folder = ctx.web.get_folder_by_server_relative_url(parent_folder_name)
    main_folder = root_folder.folders.add(Mappe1) 
    ctx.execute_query()

    # Create subfolder inside main folder
    subfolder = main_folder.folders.add(Mappe2)
    ctx.execute_query()

    file_path = excel_file_path  # Ensure it points to the created Excel file

    # Check if the file exists and upload it
    if os.path.exists(file_path):
        with open(file_path, "rb") as file_content:
            subfolder.upload_file(os.path.basename(file_path), file_content.read())
        ctx.execute_query()
        
    else:
        print(f"File '{file_path}' does not exist.")

    if log:
        orchestrator_connection.log_info("Folders created in sharepoint")

    # Step 2: Create a sharing link (e.g., Anonymous View Link)
    result = subfolder.share_link(SharingLinkKind.OrganizationEdit).execute_query()
    link_url = result.value.sharingLinkInfo.Url

    # Step 3: Verify the sharing link
    result = Web.get_sharing_link_kind(ctx, link_url).execute_query()

    # SMTP Configuration (from your provided details)
    SMTP_SERVER = "smtp.adm.aarhuskommune.dk"
    SMTP_PORT = 25
    SCREENSHOT_SENDER = "aktbob@aarhus.dk"

    def send_success_email(to_address: str | list[str], sags_id: str, deskpro_id: str, sharepoint_link: str):
        """
        Sends an email notification with the provided body and subject.

        Args:
            to_address (str | list[str]): Email address or list of addresses to send the notification.
            sags_id (str): The ID of the case (SagsID) used in the email subject.
            deskpro_id (str): The DeskPro ID for constructing the DeskPro link.
            sharepoint_link (str): The SharePoint link to include in the email body.
        """
        # Email subject
        subject = f"Robotten har nu oprettet en dokumentliste for {sags_id}"

        # Email body (HTML)
        body = f"""
        <html>
            <body>
                <p>Der er valgt 'Ja' ud fra alle dokumenter i kolonnen 'Omfattet af ansøgning', her skal du vælge nej hvis de ikke er omfattet. Hvis de er omfattet skal du herefter vælge om der gives aktindsigt i dokumentet, og vælge en begrundelse hvis du har valgt nej/delvis. Sæt herefter robotten igang med at flytte filerne til FilArkiv når du har udfyldt hele listen.</p>
                <br>
                <a href="{sharepoint_link}">Link til dokumentlisten</a>
                <br><br>
                <p>Excel filen er låst således at du kun kan ændre på de sidste 3 kolonner, og robotten tager kun de filer med hvor der står 'Ja' eller 'Delvis' i 'Gives der aktindsigt i dokumentet? (Ja/Nej/Delvis)' kolonnen.</p>
                <br>
                <a href="https://mtmsager.aarhuskommune.dk/app?return=#/t/ticket/{deskpro_id}">Link til aktindsigten i Deskpro</a>
            </body>
        </html>
        """

        # Create the email message
        msg = EmailMessage()
        msg['To'] = ', '.join(to_address) if isinstance(to_address, list) else to_address
        msg['From'] = SCREENSHOT_SENDER
        msg['Subject'] = subject
        msg.set_content("Please enable HTML to view this message.")
        msg.add_alternative(body, subtype='html')
        msg['Reply-To'] = UdviklerMail
        msg['Bcc'] = UdviklerMail

        # Send the email using SMTP
        try:
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as smtp:
                smtp.send_message(msg)
                
        except Exception as e:
            print(f"Failed to send success email: {e}")

    if log:
        orchestrator_connection.log_info("Sending email")

    # Encode folder names for URL safety
    Mappe1_encoded = quote(Mappe1)
    Mappe2_encoded = quote(Mappe2)

    # Construct the full SharePoint URL
    SharepointLink = f"{API_url}/Delte%20dokumenter/Dokumentlister/{Mappe1_encoded}/{Mappe2_encoded}"

    if send_email:
        send_success_email(MailModtager, SagsID, DeskProID, link_url)

    if log:
        orchestrator_connection.log_info("Tilføjer link til Podio")

    # API Headers
    headers = {
        "ApiKey": API_password,  
        "Content-Type": "application/json"
    }

    # 1. PUT Request to Update Podio Link
    put_url = f"{API_username}/Podio/{PodioID}/DokumentlisteField"
    put_body = {"value": SharepointLink}
    put_response = requests.put(put_url, headers=headers, json=put_body)

    if put_response.status_code != 200 and put_response.status_code != 204:
        print(f"PUT request failed: {put_response.status_code}, {put_response.text}")

    # Debugging: Print URLs and Headers
    get_ticket_url = f"{API_username}/Database/Tickets?deskproId={quote(DeskProID)}"
    get_case_url = f"{API_username}/Database/Cases?podioItemId={quote(PodioID)}"

    # 2. GET Request to Fetch Ticket ID based on DeskProId
    get_ticket_response = requests.get(get_ticket_url, headers=headers, json={})  # Added json={}
    get_ticket_response.raise_for_status()

    ticket_data = get_ticket_response.json()
    ticket_id = ticket_data[0]['id']
    patch_ticket_url = f"{API_username}/Database/Tickets/{ticket_id}"
    patch_ticket_body = {"sharepointFolderName": Mappe1}
    patch_ticket_response = requests.patch(patch_ticket_url, headers=headers, json=patch_ticket_body)
    patch_ticket_response.raise_for_status()

    # 4. GET Request to Fetch Case ID based on PodioID
    get_case_response = requests.get(get_case_url, headers=headers, json={}) 
    get_case_response.raise_for_status()
    case_data = get_case_response.json()
    case_id = case_data[0]['id']
                
    patch_case_url = f"{API_username}/Database/Cases/{case_id}"
    patch_case_body = {"sharepointFolderName": Mappe2}
    patch_case_response = requests.patch(patch_case_url, headers=headers, json=patch_case_body)
    patch_case_response.raise_for_status()

    if os.path.exists(excel_file_path):
        os.remove(excel_file_path)