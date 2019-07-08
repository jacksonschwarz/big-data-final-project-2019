import json

def createData(year):
    with open("data/{}.json".format(year), "r") as file:
        data = file.read()
    data = dict(json.loads(data))["PatentBulkData"]
    return data

def findCompany(row):
    try:
        assignee = row["assignmentDataBag"]["assignmentData"][0]["assigneeBag"]["assignee"]
        contact = assignee[0]["contactOrPublicationContact"][0]
        name = ", ".join([x["value"] for x in contact["name"]["personNameOrOrganizationNameOrEntityName"]])
        return name
    except (KeyError, IndexError):
        return None

def findName(row):
        try:

            applicantOrInventorBag = row["patentCaseMetadata"]["partyBag"]["applicantBagOrInventorBagOrOwnerBag"][1]
            inventorContact = applicantOrInventorBag["inventorOrDeceasedInventor"][0]["contactOrPublicationContact"][0]
            inventorName = inventorContact["name"]["personNameOrOrganizationNameOrEntityName"][0]["personStructuredName"]
            fullName = " ".join(inventorName.values())
            return fullName
        except (KeyError, IndexError):
            return None
        

def findLocation(row):
        try:
            applicantOrInventorBag = row["patentCaseMetadata"]["partyBag"]["applicantBagOrInventorBagOrOwnerBag"][1]
            inventorContact = applicantOrInventorBag["inventorOrDeceasedInventor"][0]["contactOrPublicationContact"][0]
            cityName = inventorContact["cityName"]
            stateName = inventorContact["geographicRegionName"]["value"]
            countryCode = inventorContact["countryCode"]
            fullLocation = ((cityName),(stateName), (countryCode))    
            return fullLocation
        except (KeyError, IndexError):
            return None

def findDate(row):
    try:
        return row["patentCaseMetadata"]["filingDate"]
    except (KeyError, IndexError):
        return None

def writeYear(year):
    f = open("./parsed-data/{}-parsed.txt".format(year), "w")
    for row in createData(year):
        rowTup = (findCompany(row), findName(row), findLocation(row), findDate(row))
        f.write(str(rowTup))
    f.close()

for y in range(2017, 2020):
    print("Parsing {}".format(y))
    writeYear(y)