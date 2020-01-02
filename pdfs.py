import csv, PyPDF2, sqlite3, re, os
import dasql

dasql.csvToDb("advanced_current_members_by_mshp_status.csv","contacts.db","Members")
dasql.csvToDb('lodges.csv','contacts.db','Lodges', outputToFile=False)
dasql.createTable('contacts.db', 'Ranks', 'Glref, Provincial, UnitID, Rank, Start', 'Glref', 'Type', 'Provincial', 'UnitID', 'UnitName', 'Rank', 'Start', 'End')

con = sqlite3.connect('contacts.db')
cur = con.cursor()

glref_regex = re.compile(r'\d{5,8}(\D)?')
lodge_id_regex = re.compile(r'L\d{1,4}')
province_regex = re.compile(r'Bucks.\S')
date_regex = re.compile(r'\d{2}/\d{2}/\d{4}')

os.system('cls')
for root, dirs, files in os.walk(r".\Source"):
    for file in files:
        print("Converting", file)
        pdffileobj = open('.\\source\\' + file, 'rb')
        pdfReader = PyPDF2.PdfFileReader(pdffileobj)
        page_count = (pdfReader.numPages)
        lodge_pdf_txt = open('Lodge.txt', 'w')
        page_number = 0
        while page_number < page_count:
            pageObj = pdfReader.getPage(page_number)
            page_content = pageObj.extractText()
            lodge_pdf_txt.write(page_content)
            page_number += 1
        lodge_pdf_txt.close()

        # Open the file and initiate variables
        print('Processing', file)
        lodge_pdf_txt = open('Lodge.txt', 'r')
        glref = ''
        office_type = ''
        pdf_section = ''
        prov_counter = -1
        loffice_counter = -1
        for line in lodge_pdf_txt:
            if line.strip() == 'Provincial Ranks':
                pdf_section = 'Provincial Ranks'
            elif line.strip() == 'Lodge Career':
                pdf_section = 'Lodge Career'
            elif line.strip() == 'Lodge Offices':
                pdf_section = 'Lodge Offices'
            prov_counter -= 1
            loffice_counter -= 1

            # Get te Glref
            if glref_regex.search(line):
                line = line.strip()
                stmt = 'SELECT "Gl ref" from Members where "Gl ref" = \'' + line +"'"
                cur.execute(stmt)
                con.commit()
                result = str(cur.fetchone())
                if result.upper() != 'NONE':
                    glref = line

            # Process Provincial Ranks
            if line.strip() == 'Craft Rank':
                office_type = 'PROVCRAFTRANK'
            elif line.strip() == 'Royal Arch':
                office_type = 'PROVCHAPRANK'
            # Prepare Data for Provincial Rank
            if province_regex.search(line):
                prov_counter = 4
                provincial_rank = line.strip()
            if prov_counter == 2:
                start_date = line.strip()
            if prov_counter == 0:
                if date_regex.search(line):
                    end_date = line.strip()
                else:
                    end_date = ''
                if pdf_section == 'Provincial Ranks':
                    stmt = 'SELECT "Glref" from Ranks where "Glref" = \'' + glref + "' AND \"Provincial\" = '" + provincial_rank + "' AND \"UnitID\" = '' AND \"Rank\" = '' AND \"Start\" = '" + start_date +"'"
                    print ("L Office Statement",stmt)
                    cur.execute(stmt)
                    con.commit()
                    result = str(cur.fetchone())
                    print ("Result", result)
                    if result.upper() == 'NONE':
                        dasql.dataAdder('Provincial','contacts.db', 'Ranks', Glref=glref, Type=office_type, Provincial=provincial_rank, UnitID='', UnitName='', Rank='', Start=start_date, End=end_date)
                #office_type = ''
                provincial_rank = ''
                prov_counter = -1


            if pdf_section == "Lodge Offices" and lodge_id_regex.search(line):
                line = line.strip()
                stmt = 'SELECT "Lc name" from Lodges where "Lc number" = \'' + line +"'"
                cur.execute(stmt)
                con.commit()
                result = str(cur.fetchone())
                if result.upper() != 'NONE':
                    lodge_id = line
                    lodge_name = result[2:]
                    lodge_name = lodge_name[:-3]
                    lodge_name = dasql.fix_apostrophe(lodge_name)
                    if lodge_id[0:1] == 'L':
                        office_type = 'LODGEOFF'
                    elif lodge_id[0:1] == 'C':
                        office_type = 'CHAPOFF'
                    if lodge_name == 'Buckinghamshire Provincial Grand Stewards':
                        loffice_counter = 9
                    else:
                        loffice_counter = 8
                    # print(glref, pdf_section, lodge_id, lodge_name, loffice_counter)
                else:
                    loffice_counter = -1
            if loffice_counter == 4:
                lodge_office = line.strip()
            if loffice_counter == 2:
                start_date = line.strip()
            if loffice_counter == 0:
                end_date = line.strip()
            if pdf_section == 'Lodge Offices' and loffice_counter == 0:
                # Constraint 'Glref, Provincial, UnitID, Rank, Start'
                #stmt = 'SELECT "Gl ref" from Ranks where "Gl ref" = \'' + glref + "' AND "Provincial" = '' AND "UnitID"  = '" + lodge_id + ' AND \"Rank\" = ' + lodge_office + "' AND \"Start\" = '" + start_date  + "'"
                stmt = 'SELECT "Glref" from Ranks where "Glref" = \'' + glref + "' AND \"Provincial\" = '' AND \"UnitID\" = '" + lodge_id + "' AND \"Rank\" = '" + lodge_office +"' AND \"Start\" = '" + start_date +"'"
                cur.execute(stmt)
                con.commit()
                result = str(cur.fetchone())
                if result.upper() == 'NONE':
                    dasql.dataAdder('Lodge Offices','contacts.db', 'Ranks', Glref = glref , Type=office_type, Provincial='', UnitID=lodge_id, UnitName=lodge_name, Rank=lodge_office, Start=start_date, End=end_date)
                loffice_counter = -1

dasql.output('contacts.db', 'Ranks', 'ranks.csv')

con.close()
