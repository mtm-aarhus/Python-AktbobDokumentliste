# Dokumentlistegenerator & Nova-sagsopretter – Procesbeskrivelse

Denne robot genererer en dokumentliste over sagens dokumenter (GEO eller Nova), uploader den til SharePoint, sender notifikation til sagsbehandler og opretter/ajourfører en tilknyttet sag i KMD Nova, hvis det er en Nova-sag.

---

## Procestrin

### 1. Identificér sagens type
- Hvis `SagsNummer` matcher Geo-format (`ABC-XXXX-XXXXXX`), håndteres den som GEO-sag.
- Ellers behandles den som Nova-sag og kræver token til KMD Nova.

### 2. Hent dokumentdata
- **GEO**: Via SharePoint/GOAPI hentes dokumentmetadata, inkl. over- og underbilag.
- **Nova**: Via Nova API (`Document/GetList`) hentes relevante dokumenter.
- Dokumenter som indeholder `"tunnel_marking"`, `"memometadata"` eller `"fletteliste"` markeres automatisk som *“Nej”* med standardbegrundelse.

### 3. Generér Excel-arbejdsark
- Dokumentmetadata lægges i en tabel i Excel.
- Robotten formaterer filen med:
  - Wrapped tekst, kolonnebredder og rækkehøjde
  - Drop-downs i kolonnerne *Omfattet*, *Gives der aktindsigt*, *Begrundelse*
  - Skjult ark med standardbegrundelser
  - Beskyttelse af celler (kun relevante kolonner er redigerbare)
  - Hyperlinks i dokumentlink-kolonnen

### 4. Opret mapper og upload til SharePoint
- Mapper navngives ud fra `{DeskProID} - {Titel}` → `{Sagsnummer} - {SagsTitel}`
- Mapperne oprettes i `Delte dokumenter/Dokumentlister`
- Excel-filen uploades til undermappen
- Delingslink (OrganisationEdit) genereres og kontrolleres

### 5. Send notifikation via e-mail
- Mail sendes til `Email` i køelementet med:
  - Link til dokumentlisten
  - Vejledning i kolonneudfyldning
  - Advarsel hvis sagen indeholder tunnel/memo/flette-dokumenter
- Ved tom sag sendes særskilt notifikation

### 6. Opret sag i Nova (kun hvis NovaSag = True)
- Robotten tjekker DeskPro-felter for gamle sagsnumre.
- Matcher BFE-nummer og dato → opdater eksisterende sag.
- Ellers:
  - Robotten opretter ny Nova-sag via `Case/Import`
  - Journalnotat tilføjes med link til oprindelig sag
  - Metadata såsom ejendomsoplysninger, parter og sagstype tilføjes

### 7. Opdater metadata i Podio og DeskPro
- SharePoint-foldernavn opdateres i:
  - `tickets/{ticket_id}` i DeskPro
  - `cases/{case_id}` i Podio
- Link til dokumentlisten tilføjes i Podio-felt

### 8. Ryd op og fjern lokalt Excel-ark
- Excel-filen slettes efter upload og deling.

### 9. Undgå dubletter via SQL-lås
- Robotten forsøger at indsætte en *global lock* i SQL.
- Hvis en anden robot allerede har startet en Nova-sagsoprettelse for samme sag, springes dette trin over.

---

