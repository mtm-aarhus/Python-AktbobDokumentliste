from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
import uuid
import requests
import json
import os
from GetKmdAcessToken import GetKMDToken
from datetime import datetime,timedelta
import base64
from docx import Document
import io
import re
import pyodbc
from GetKmdAcessToken import GetKMDToken
def invoke_GenerateNovaCase(Sagsnummer, KMDNovaURL, KMD_access_token, AktSagsURL, IndsenderNavn, IndsenderMail, DeskProID, AktindsigtsDato, orchestrator_connection: OrchestratorConnection):
    Server = orchestrator_connection.get_constant('SqlServer').value
    DeskProAPI = orchestrator_connection.get_credential("DeskProAPI") #Credential
    DeskProAPIKey = DeskProAPI.password  
    AktindsigtsDato = AktindsigtsDato.rstrip('Z') # sletter bare Z 

    def store_case_uuid(deskpro_id, case_uuid):
        
        conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};" + f"SERVER={Server};DATABASE=PYORCHESTRATOR;Trusted_Connection=yes;")
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO dbo.AktBobNovaCases (DeskProID, CaseUuid, [Open/Closed])
            VALUES (?, ?, ?)
            """,
            deskpro_id,
            str(case_uuid),
            "Open"
        )
        conn.commit()
        cursor.close()
        conn.close()
    
     ### --- Henter caseinfo --- ###
    TransactionID = str(uuid.uuid4())

    # Define API URL
    Caseurl = f"{KMDNovaURL}/Case/GetList?api-version=2.0-Case"

    # Define headers
    headers = {
        "Authorization": f"Bearer {KMD_access_token}",
        "Content-Type": "application/json"
    }

    data = {
        "common": {
            "transactionId": TransactionID
        },
        "paging": {
            "startRow": 1,
            "numberOfRows": 500
        },
        "caseAttributes": {
            "userFriendlyCaseNumber": Sagsnummer
        },
        "caseGetOutput": { 
            "sensitivity": {
                "sensitivityCtrBy": True
            },
            "securityUnit": {
                "departmentCtrlBy": True
            },
            "responsibleDepartment": {
                "fkOrgIdentity": {
                    "fkUuid": True,
                    "type": True,
                    "fullName": True
                },
                "departmentCtrlBy": True
            },  
            "availability": {
                "availabilityCtrBy": True
            },   
            "caseParty": {
                "index": True,
                "identificationType": True,
                "identification": True,
                "partyRole": True,
                "partyRoleName": True,
                "name": True,
                "ParticipantRole": True
            },
            "caseworker": {
                "kspIdentity": {
                    "novaUserId": True,
                    "racfId": True,
                    "fullName": True
                }
            },
            "buildingCase": {
            "propertyInformation":{
                "bfeNumber": True,
                "cadastralId": True
         }
        }
    }
    }
    try:
        response = requests.put(Caseurl, headers=headers, json=data)
        
        # Handle response
        if response.status_code == 200:
            response_data = response.json()
            case = response_data["cases"][0]  # Assuming there's at least one case

            # Extract required case attributes
            sensitivityCtrBy = case["sensitivity"]["sensitivityCtrBy"]
            SecurityUnitCtrlBy = case["securityUnit"]["departmentCtrlBy"]
            ResponsibleDepartmentCtrlBy = case["responsibleDepartment"]["departmentCtrlBy"]
            availabilityCtrBy = case["availability"]["availabilityCtrBy"]
            
            # Extract bfeNumber from buildingCase -> propertyInformation
            bfeNumber = case["buildingCase"]["propertyInformation"]["bfeNumber"]
            CadastralId = case["buildingCase"]["propertyInformation"]["cadastralId"]

            primary_case_parties = [
                {
                    "index": party["index"],
                    "identificationType": party["identificationType"],
                    "identification": party["identification"],
                    "partyRole": party["partyRole"],
                    "partyRoleName": party["partyRoleName"],
                    "participantRole": party["participantRole"],
                    "name": party["name"]
                }
                for party in case.get("caseParties", []) if party["partyRole"] == "PRI"
            ]

            # If at least one primary case party exists, assign values to variables
            if primary_case_parties:
                index = primary_case_parties[0]["index"]
                identificationType = primary_case_parties[0]["identificationType"]
                identification = primary_case_parties[0]["identification"]
                partyRole = primary_case_parties[0]["partyRole"]
                partyRoleName = primary_case_parties[0]["partyRoleName"]
                participantRole = primary_case_parties[0]["participantRole"]
                name = primary_case_parties[0]["name"]
            else:
                raise Exception("No primary case parties found.")      
        else:
            raise Exception("Failed to send request. Status Code:", response.status_code,response.text)
    except Exception as e:
        raise Exception("Failed to fetch case data:", str(e))



    ### ---- Henter deskpro info: --- ####

    Deskprourl = f"https://mtmsager.aarhuskommune.dk/api/v2/tickets/{DeskProID}"

    headers = {
    'Authorization': DeskProAPIKey,
    'Cookie': 'dp_last_lang=da'
    }
    
    # Target field numbers as strings
    target_fields = {"61","62", "63", "74", "75", "78", "81", "85", "87", "90", "93", "96", "99", "102", "105"}

    
    # Regex pattern for old case numbers
    case_number_pattern = re.compile(r"^[A-Za-z]\d{4}-\d{1,10}$")
    old_case_numbers = []
    target_values = {}
    BFEMatch = False
    NovaCaseExists = False

    try:
        response = requests.get(Deskprourl, headers=headers)
        
        if response.status_code != 200:
            raise Exception(f"Request failed with status code {response.status_code}: {response.text}")
        
        data = response.json()
        fields = data.get("data", {}).get("fields", {})

        for field_key, field_data in fields.items():
            if field_key in target_fields:
                value = field_data.get("value")
                target_values[field_key] = value  # Save value

                # Check for case number pattern
                if isinstance(value, str):
                    if case_number_pattern.match(value):
                        old_case_numbers.append(value)

                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, str) and case_number_pattern.match(item):
                            old_case_numbers.append(item)
        
        # Now loop through the found case numbers
        for case_number in old_case_numbers:
            print("Found old case number:", case_number)
             ### --- Henter caseinfo --- ###
            TransactionID = str(uuid.uuid4())

            # Define API URL
            Caseurl = f"{KMDNovaURL}/Case/GetList?api-version=2.0-Case"

            # Define headers
            headers = {
                "Authorization": f"Bearer {KMD_access_token}",
                "Content-Type": "application/json"
            }

            data = {
                "common": {
                    "transactionId": TransactionID
                },
                "paging": {
                    "startRow": 1,
                    "numberOfRows": 100
                },
                "caseAttributes": {
                    "userFriendlyCaseNumber": case_number
                },
                "caseGetOutput": { 
                    "buildingCase": {
                        "propertyInformation":{
                            "bfeNumber": True,
                            "caseAddress":True
                            }
                        }
                    }
                }
            try:
                response = requests.put(Caseurl, headers=headers, json=data)

                if response.status_code == 200:
                    response_data = response.json()
                    response_data.get("cases")
                    case = response_data["cases"][0]
                    OldbfeNumber = case["buildingCase"]["propertyInformation"]["bfeNumber"]
                    OldCaseAdress = case["buildingCase"]["propertyInformation"]["caseAddress"]

                    if str(OldbfeNumber) == str(bfeNumber):
                        print(f"Match found: Old BFE ({OldbfeNumber}) == Current BFE ({bfeNumber})")
                        old_case_number = case_number
                        BFEMatch = True
                        break  # Exit loop after first match
                    else:
                        print(f"No match: Old BFE ({OldbfeNumber}) != Current BFE ({bfeNumber})")

                else:
                    raise Exception (f"KMD API call failed for {case_number}, status: {response.status_code}, message: {response.text}")

            except Exception as e:
                print(f"An error occurred while calling KMD API for {case_number}: {e}")

        if not BFEMatch:
            print("No matching BFE number found in any case.")
        else:
            print("BFE match confirmed!")
            TransactionID = str(uuid.uuid4())

            # Parse the string into a datetime object
            date_obj = datetime.strptime(AktindsigtsDato, "%Y-%m-%dT%H:%M:%S")
            #Skal ændres til 00:00:00 ellers kan vi risikere at tidspunktet er udløbet
            date_obj_midnight = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
            AktindsigtsDato_midnight = date_obj_midnight.strftime("%Y-%m-%dT%H:%M:%S")
            # tilføjer én dag for at tjekke om der er oprettet nogen sager i det tidsinterval
            new_date_obj = date_obj_midnight + timedelta(days=1)

            # Convert new_date_obj back to string
            new_date_str = new_date_obj.strftime("%Y-%m-%dT%H:%M:%S")

            # Define API URL
            Caseurl = f"{KMDNovaURL}/Case/GetList?api-version=2.0-Case"

            # Define headers
            headers = {
                "Authorization": f"Bearer {KMD_access_token}",
                "Content-Type": "application/json"
            }

            data = {
            "common": {
                "transactionId": TransactionID
            },
            "paging": {
                "startRow": 1,
                "numberOfRows": 100
            },
            "caseAttributes": {
                "title": f"Test gustav - Anmodning om aktindsigt i {old_case_number}", # skal ændres til "Anmodning om aktindsigt i...."
                "fromCaseDate": AktindsigtsDato_midnight,
                "toCaseDate": new_date_str

            },
            "states":{
                "states":[{
                    "progressState":"Opstaaet"
            }]
            },
            "caseGetOutput": { 
                "caseAttributes":{
                "userFriendlyCaseNumber": True
                },
            "buildingCase": {
                "propertyInformation":{
                    "caseAddress":True
                            }
                        }   
            }
            }
            # Make the request
            response = requests.put(Caseurl, headers=headers, json=data)

            # Check status and handle response
            if response.status_code == 200:
                response_data = response.json()
                if response_data.get("pagingInformation", {}).get("numberOfRows", 0) > 0:
                    case = response_data["cases"][0]
                    OldCaseUuid = case["common"]["uuid"]
                    OldCaseAdress = case["buildingCase"]["propertyInformation"]["caseAddress"]
                    NovaCaseExists = True
                else:
                    print("Tjekker om sagen er opdateret i forvejen")
                    data = {
                    "common": {
                        "transactionId": TransactionID
                    },
                    "paging": {
                        "startRow": 1,
                        "numberOfRows": 100
                    },
                    "caseAttributes": {
                        "title": f"Test gustav - Anmodning om aktindsigt i {OldCaseAdress}", # skal ændres til "Anmodning om aktindsigt i...."
                        "fromCaseDate": AktindsigtsDato_midnight,
                        "toCaseDate": new_date_str

                    },
                    "states":{
                        "states":[{
                            "progressState":"Opstaaet"
                    }]
                    },
                    "caseGetOutput": { 
                        "caseAttributes":{
                        "userFriendlyCaseNumber": True
                        },
                    "buildingCase": {
                        "propertyInformation":{
                            "caseAddress":True
                                    }
                                }   
                    }
                    }
                    # Make the request
                    response = requests.put(Caseurl, headers=headers, json=data)

                    if response.status_code == 200:
                        response_data = response.json()
                        if response_data.get("pagingInformation", {}).get("numberOfRows", 0) > 0:
                            case = response_data["cases"][0]
                            OldCaseUuid = case["common"]["uuid"]
                            OldCaseAdress = case["buildingCase"]["propertyInformation"]["caseAddress"]
                            NovaCaseExists = True
                        else:
                            NovaCaseExists = False
            else:
                raise Exception(f"API request failed with status {response.status_code}: {response.text}")
                        
    except Exception as e:
        NovaCaseExists = False
        print(f"An error occurred during ticket processing: {e}")


    if BFEMatch and NovaCaseExists:
        print("BFE matcher opdaterer sagen ")
        orchestrator_connection.log_info(f"Sagen er oprettet, det gamle CaseUuid ligger allerede i databasen: {OldCaseUuid}")

        # Define API URL
        Caseurl = f"{KMDNovaURL}/Case/Update?api-version=2.0-Case"
        TransactionID = str(uuid.uuid4())
        # Define headers
        headers = {
            "Authorization": f"Bearer {KMD_access_token}",
            "Content-Type": "application/json"
        }
        
        data = {
        "common": {
            "transactionId": TransactionID,
            "uuid": OldCaseUuid
        },
        "paging": {
            "startRow": 1,
            "numberOfRows": 100
        },
        "caseAttributes": {
            "title": f"Test gustav - Anmodning om aktindsigt i {OldCaseAdress}", # skal ændres til "Anmodning om aktindsigt i...."
            "caseDate": AktindsigtsDato, 
            "caseCategory": "BomByg"
        }
        }

        # Make the request
        response = requests.patch(Caseurl, headers=headers, json=data)

        # Check status and handle response
        if response.status_code == 200:
            print(f"Sagen er opdateret: {response.status_code}")
    
        else:
            raise Exception(f"API request failed with status {response.status_code}: {response.text}")
     
    else:
        print("No matching BFE number found opretter sagen på ny.")
        # ### ---  Opretter sagen --- ####   
        JournalDate = datetime.now().strftime("%Y-%m-%dT00:00:00")
        TransactionID = str(uuid.uuid4())
        CaseUuid = str(uuid.uuid4())
        JournalUuid = str(uuid.uuid4())
        Index_Uuid = str(uuid.uuid4())
        link_text = "GO Aktindsigtssag"
        print(f"Aktsagsurl: {AktSagsURL}")
        # Step 1: Create a new Word document
        doc = Document()
        doc.add_paragraph("Aktindsigtssag Link: " + AktSagsURL)  # Add content to the document


        # Step 2: Save document to a BytesIO stream
        doc_stream = io.BytesIO()
        doc.save(doc_stream)
        doc_stream.seek(0)  # Reset stream position

        # Step 3: Convert document to base64
        base64_JournalNote = base64.b64encode(doc_stream.read()).decode("utf-8")

        url = f"{KMDNovaURL}/Case/Import?api-version=2.0-Case"

        # Define headers
        headers = {
            "Authorization": f"Bearer {KMD_access_token}",
            "Content-Type": "application/json"
        }

        # Define JSON payload
        payload = {
            "common": {
                "transactionId": TransactionID,
                "uuid": CaseUuid  
            },
            "caseAttributes": {
                "title": f"Test gustav - Anmodning om aktindsigt i {Sagsnummer}", # skal ændres til "Anmodning om aktindsigt i...."
                "caseDate": AktindsigtsDato,
                "caseCategory": "BomByg"
            },
            "caseClassification": {
                "kleNumber": {"code": "02.00.00"}, 
                "proceedingFacet": {"code": "A53"}
            },
            "state": "Opstaaet", 
            "sensitivity": "Følsomme",
            "caseworker": { 
                "kspIdentity": {
                    "novaUserId": "78897bfc-2a36-496d-bc76-07e7a6b0850e",
                    "racfId": "AZX0075",
                    "fullName": "Aktindsigter Novabyg"
                }
            },
            "SensitivityCtrlBy": sensitivityCtrBy,
            "AvailabilityCtrlBy": availabilityCtrBy,
            "SecurityUnitCtrlBy": SecurityUnitCtrlBy,
            "ResponsibleDepartmentCtrlBy": ResponsibleDepartmentCtrlBy,
            "responsibleDepartment": {
                "fkOrgIdentity": {
                    "fkUuid": "15deb66c-1685-49ac-8344-cfbf84fe6d84",
                    "type": "Afdeling",
                    "fullName": "Digitalisering"
                }
            },
            "caseParties": [
                {
                    "index": index,
                    "identificationType": identificationType,
                    "identification": identification, 
                    "partyRole": partyRole,
                    "partyRoleName": partyRoleName, 
                    "participantRole": participantRole, 
                    "name": name 
                },
                {
                    "index": Index_Uuid,
                    "identificationType": "Frit",
                    "identification": IndsenderNavn,
                    "partyRole": "IND",
                    "partyRoleName": "Indsender",
                    "participantRole": "Sekundær",
                    "name": IndsenderNavn,
                    "participantContactInformation": IndsenderMail
                }
            ],
            "journalNotes": [
                {
                    "uuid": JournalUuid,
                    "approved": True,
                    "journalNoteAttributes":
                    {
                        "journalNoteDate": JournalDate, 
                        "title": link_text,
                        "editReasonApprovedJournalnote": "Oprettelse",
                        "journalNoteAuthor": "AKTBOB",
                        "author": {
                            "fkOrgIdentity": {
                                "fkUuid": "15deb66c-1685-49ac-8344-cfbf84fe6d84",
                                "type": "Afdeling",
                                "fullName": "Digitalisering"
                                }
                        },
                        "journalNoteType": "Bruger",
                        "format": "Ooxml",
                        "note":base64_JournalNote

                    }
                }
            ],
            "buildingCase": {
                "buildingCaseAttributes": {
                    "buildingCaseClassId": "2a33734b-c596-4edf-93eb-23daae4bfc3e",
                    "buildingCaseClassName": "Aktindsigt"
                },
                "propertyInformation":{
                    "cadastralId": CadastralId,
                    "bfeNumber": bfeNumber

                },
                "UserdefinedFields": [
                        {
                            "type": "1. Politisk kategori",
                            "value": "Aktindsigt"
                        }
                    ]
            }  
        }
        # Make the API request
        try:
            response = requests.post(url, headers=headers, json=payload)
            
            # Handle response
            if response.status_code == 200:
                print(response.text)
            else:
                print("Failed to send request. Status Code:", response.status_code)
                print("Response Data:", response.text)  # Print error response
        except Exception as e:
            raise Exception("Failed to fetch Sagstitel (Nova):", str(e))
        

        orchestrator_connection.log_info(f"Sender følgende CaseUuid videre: {CaseUuid}")
        #Logger til database:
        store_case_uuid(DeskProID, CaseUuid)

    return {
    "out_Text": "Aktindsigtssagen er oprettet i Nova"
    }