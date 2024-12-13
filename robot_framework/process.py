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
from datetime import datetime
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from requests_ntlm import HttpNtlmAuth
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.datavalidation import DataValidation
import shutil
import smtplib
from email.message import EmailMessage
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment, Font, Protection
from PIL import ImageFont, ImageDraw, Image


# pylint: disable-next=unused-argument
def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement | None = None) -> None:
    """Do the primary process of the robot."""
    orchestrator_connection.log_trace("Running process.")

    log = True
    send_email = True

    #Getting credentials
    API_url = orchestrator_connection.get_constant("AktbobSharePointURL").value
    API_credentials = orchestrator_connection.get_credential("AktbobAPIKey")
    API_username = API_credentials.username ## Instead use endpoint
    API_password = API_credentials.password

    #Define developer mail
    UdviklerMail = orchestrator_connection.get_constant("jadt").value

    GOAPILIVECRED = orchestrator_connection.get_credential("GOAktApiUser")
    GOAPILIVECRED_username = GOAPILIVECRED.username
    GOAPILIVECRED_password = GOAPILIVECRED.password
    GOAPI_URL = orchestrator_connection.get_constant('GOApiURL').value

    #Get Robot Credentials
    RobotCredentials = orchestrator_connection.get_credential("Robot365User")
    RobotUsername = RobotCredentials.username
    RobotPassword = RobotCredentials.password

    # Define the JSON object (queue_json)
    queue_json = json.loads(queue_element.data)

    # Retrieve elements from queue_json
    SagsID = queue_json["SagsID"]
    MailModtager = queue_json["MailModtager"]
    PodioID = queue_json["PodioID"]
    DeskProID = queue_json["DeskProID"]
    DeskProTitel = queue_json["DeskProTitel"]
    url = GOAPI_URL + "/_goapi/Cases/Metadata/" + SagsID

    if log:
        orchestrator_connection.log_info("Process starter")

    # Create session with NTLM authentication
    session = requests.Session()
    session.auth = HttpNtlmAuth(GOAPILIVECRED_username, GOAPILIVECRED_password)
    session.headers.update({"Content-Type": "application/json"})

    # Send GET request
    if log:
        orchestrator_connection.log_info("Getting metadata")
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

    # Removal of illegal characters and double spaces
    pattern = r'[~#%&*{}\\:<>?/+|"\t]'
    SagsTitel = re.sub(pattern, '', str(SagsTitel))
    SagsTitel = " ".join(SagsTitel.split())

    if log:
        orchestrator_connection.log_info("Sagsurl" + SagsURL)
    Akt = SagsURL.split("/")[1]
    if log:
        orchestrator_connection.log_info("Akt" + Akt)

    # Replacing '-' with '%2D' in SagsID
    encoded_sags_id = SagsID.replace("-", "%2D")

    # Constructing the URL
    ListURL = f"%27%2Fcases%2F{Akt}%2F{encoded_sags_id}%2FDokumenter%27"
    if log:
        orchestrator_connection.log_info("ListURL: " + ListURL)

    # Define the structure of the DataTable
    columns = [
        "Akt ID", "Dok ID", "Dokumenttitel", "Dokumentkategori", "Dokumentdato", 
        "Bilag til Dok ID", "Bilag", "Link til dokument", 
        "Omfattet af ansøgning (Ja/Nej)", "Gives der aktindsigt i dokumentet? (Ja/Nej/Delvis)", 
        "Begrundelse hvis nej eller delvis"
    ]

    # Create an empty DataFrame with these columns
    data_table = pd.DataFrame(columns=columns)

    # Get the data from the server
    response = session.get(f"{GOAPI_URL}/{SagsURL}/_goapi/Administration/GetLeftMenuCounter")
    stuff = response.text  # Server response as text
    ViewsIDArray = json.loads(stuff)  # Parse the JSON

    # Iterate through the items to find the desired ViewId
    for item in ViewsIDArray:
        if item["ViewName"] == "UdenMapper.aspx":
            ViewId = item["ViewId"]
            break  # Stop searching once we find "UdenMapper.aspx"
        elif item["ViewName"] == "Journaliseret.aspx":
            ViewId = item["ViewId"]
        else:
            ViewId = None

    firstrun = True
    MorePages = True

    while MorePages == True:
        if log:
            orchestrator_connection.log_info("Henter dokumentlister")

        #If it is not the first run
        if firstrun == False:
            orchestrator_connection.log_info("Henter næste side i dokumentet")
            url = f"{GOAPI_URL}/{SagsURL}/_api/web/GetList(@listUrl)/RenderListDataAsStream"
            url_with_query = f"{url}?@listUrl={ListURL}{NextHref.replace('?', '&')}"

            # Make the POST request with NTLM authentication
            response = session.post(url_with_query, timeout=500)
            response.raise_for_status()

            # Handle the response
            Dokumentliste = response.text  # Extract the content
           
        #If it is the first run
        else:
            url = f"{GOAPI_URL}/{SagsURL}/_api/web/GetList(@listUrl)/RenderListDataAsStream"
            query_params = f"?@listUrl={ListURL}&View={ViewId}"
            full_url = url + query_params

            # Send a POST request with NTLM authentication
            response = session.post(full_url, timeout=500)
            response.raise_for_status()
            # Handle the response
            Dokumentliste = response.text  # Extract the response content
            

        #Deserialzie 
        dokumentliste_json = json.loads(Dokumentliste) 
        
        #Tag fat i row
        dokumentliste_rows = dokumentliste_json["Row"] 

        if "NextHref" in dokumentliste_json:
            MorePages = True
            NextHref = dokumentliste_json.get("NextHref")
        else:
            MorePages = False


        # Iterate over rows in dokumentliste_rows
        for item in dokumentliste_rows:
            # Construct DokumentURL
            DokumentURL = GOAPI_URL + quote(item.get("FileRef", ""), safe="/")
                
            # Extract fields from the item
            AktID = item.get("CaseRecordNumber").replace(".", "")
            DokumentDato = str(item.get("Dato"))  
            Dokumenttitel = item.get("Title", "") # "" is there to avoid an error if nothing in there
            DokID = str(item.get("DocID"))
            # Sagsbehandler = item.get("CaseOwner.title", "").split(" (")[0]
            DokumentKategori = str(item.get("Korrespondance"))
                
            # Fallback for Dokumenttitel
            if len(Dokumenttitel) < 2:
                Dokumenttitel = item.get("FileLeafRef.Name", "")
                
            # Log AktID (if logging is enabled)
            if log:
                orchestrator_connection.log_info(f"AktID: {AktID}")
                
            if log:
                orchestrator_connection.log_info("Henter parents")
                
            # Fetch Parents data from API
            response = session.get(f"{GOAPI_URL}/_goapi/Documents/Parents/{DokID}", timeout= 500)
            
            # Deserialize the JSON string into a Python dictionary
            parents_object = json.loads(response.text)
                
            # Extract ParentArray and count items
            ParentArray = parents_object.get("ParentsData", [])
                
            # Combine all DocumentIds into a single string
            Bilag = ", ".join(str(currentItem.get("DocumentId", "")) for currentItem in ParentArray)

            if log:
                orchestrator_connection.log_info("Henter children")
            Children = session.get(f"{GOAPI_URL}/_goapi/Documents/Children/{DokID}", timeout = 500)
            children_object = json.loads(Children.text)
            ChildrenArray = children_object.get("ChildrenData", [])
            BilagChild = ", ".join(str(currentItem.get("DocumentId", "")) for currentItem in ChildrenArray)

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
                    "Omfattet af ansøgning (Ja/Nej)": "Ja",
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
                    "Omfattet af ansøgning (Ja/Nej)": "Ja",
                    "Gives der aktindsigt i dokumentet? (Ja/Nej/Delvis)": "",
                    "Begrundelse hvis nej eller delvis": ""
                }])], ignore_index=True)
        firstrun = False


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
    max_width_in_pixels = 362  # Adjust based on your target column width in pixels

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
            "Link til dokument", "Omfattet af ansøgningen (Ja/Nej)",
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
    validation_i.add(f"I2:I{worksheet.max_row}")

    validation_j = DataValidation(type="list", formula1='"Ja,Delvis,Nej"', allow_blank=False, showErrorMessage=True)
    validation_j.error = "Vælg venligt enten Ja, Delvis eller Nej."
    validation_j.errorTitle = "Ugyldig værdi"
    worksheet.add_data_validation(validation_j)
    validation_j.add(f"J2:J{worksheet.max_row}")

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
    worksheet.add_data_validation(validation_k)
    validation_k.add(f"K2:K{worksheet.max_row}")

    worksheet.protection.sheet = True
    worksheet.protection.password = "Aktbob"
    worksheet.protection.enable()

    # Save the formatted Excel file
    workbook.save(excel_file_path)

    Mappe1 = DeskProID +" - " + DeskProTitel
    Mappe2 = SagsID + " - " + SagsTitel
   
    # Authenticate to SharePoint using Office365 credentials
    credentials = UserCredential(RobotUsername, RobotPassword)
    ctx = ClientContext(API_url).with_credentials(credentials)

    # Function to sanitize folder names
    def sanitize_folder_name(folder_name):
        pattern = r'[~#%&*{}\\:<>?/+|"\t]'
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
    
    # # Get the "Edit" role definition
    # role_def = ctx.web.role_definitions.get_by_name("Edit")
    # ctx.load(role_def)
    # ctx.execute_query()

    # # Ensure the user exists and get their ID
    # user = ctx.web.ensure_user(MailModtager)
    # ctx.load(user)
    # ctx.execute_query()

    # # Grant the user edit permissions to the folder
    # root_folder.list_item_all_fields.role_assignments.add(
    #     principal_id=user.id,
    #     role_def_id=role_def.id
    # )
    # ctx.execute_query()

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

    base_url = API_url 


    # Encode folder names for URL safety
    Mappe1_encoded = quote(Mappe1)
    Mappe2_encoded = quote(Mappe2)

    # Construct the full SharePoint URL
    SharepointLink = f"{base_url}/Delte%20dokumenter/Dokumentlister/{Mappe1_encoded}/{Mappe2_encoded}"

    if send_email:
        send_success_email(MailModtager, SagsID, DeskProID, SharepointLink)

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

    if put_response.status_code != 200:
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