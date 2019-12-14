import csv, sqlite3, PyPDF2, re, os
import dasql

dasql.csvToDb("advanced_current_members_by_mshp_status.csv","contacts.db","Members")
dasql.csvToDb('lodges.csv','contacts.db','Lodges', outputToFile=False)
dasql.createTable('contacts.db', 'Ranks', 'Glref, Provincial, Unit, Rank, Start', 'Glref', 'Type', 'Provincial', 'Unit', 'Rank', 'Start', 'End')

con = sqlite3.connect('contacts.db')
cur = con.cursor()

glref_regex = re.compile(r'\d{5,8}(\D)?')
lodge_id_regex = re.compile(r'L\d{1,4}')
province_regex = re.compile(r'Bucks.\S')
date_regex = re.compile(r'\d{2}/\d{2}/\d{4}')

os.system('cls')

pdffileobj = open('L631 Career Summaries.PDF', 'rb')
pdfReader = PyPDF2.PdfFileReader(pdffileobj)
page_count = (pdfReader.numPages)
methuen_file = open('L631 Methuen.txt', 'w')
page_number = 0
while page_number < page_count:
    pageObj = pdfReader.getPage(page_number)
    page_content = pageObj.extractText()
    methuen_file.write(page_content)
    page_number += 1
methuen_file.close()
methuen_file = open('L631 Methuen.txt', 'r')
glref = ''
org = ''
pdf_section = ''
counter = 0
for line in methuen_file:
    if line.strip() == 'Provincial Ranks':
        pdf_section = 'Provincial Ranks'
    elif line.strip() == 'Lodge Career':
        pdf_section = 'Lodge Career'
    elif line.strip() == 'Lodge Offices':
        pdf_section = 'Lodge Offices'
    counter -= 1
    if glref_regex.search(line):
        line = line[:-2]
        stmt = 'SELECT "Gl ref" from Members where "Gl ref" = \'' + line +"'"
        cur.execute(stmt)
        con.commit()
        result = str(cur.fetchone())
        if result.upper() != 'NONE':
            glref = line
    # Process Provincial Ranks
    if line[:-1] == 'Craft Rank':
        org = 'Craft'
    elif line[:-1] == 'Royal Arch Rank':
        org = 'Chapter'
    if province_regex.search(line):
        counter = 4
        provincial_rank = line[:-1]
    if counter == 2:
        start_date = line.strip()
    if counter == 0:
        if date_regex.search(line):
            end_date = line[:-1]
        else:
            end_date = ''
        if pdf_section == 'Provincial Ranks':
            dasql.dataAdder('Provincial','contacts.db', 'Ranks', 'Glref',glref , Type=org, Provincial=provincial_rank, Start=start_date, End=end_date)
        provincial_rank = ''
        counter = -1
    if lodge_id_regex.search(line):
        line = line[:-1]
        stmt = 'SELECT "Lc number" from Lodges where "Lc number" = \'' + line +"'"
        cur.execute(stmt)
        con.commit()
        result = str(cur.fetchone())
        if result.upper() != 'NONE':
            lodge = line

dasql.output('contacts.db', 'Ranks', 'ranks.csv')

con.close()
