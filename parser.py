import json
import pickle

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

def findClass(row):
    try:
        nationalClass = row["patentCaseMetadata"]["patentClassificationBag"] \
            ["cpcClassificationBagOrIPCClassificationOrECLAClassificationBag"][0] \
            ["mainNationalClassification"]["nationalClass"]
        return nationalClass
    except (KeyError, IndexError):
        return None

def findDate(row):
    try:
        return row["patentCaseMetadata"]["filingDate"]
    except (KeyError, IndexError):
        return None

def writeYear(year):
    try:
        f = open("./parsed-data/{}-parsed.pkl".format(year), "wb")
        totalData = []
        for row in createData(year):
            rowTup = (findCompany(row), findClass(row), findName(row), findLocation(row), findDate(row))
            totalData.append(rowTup)
        pickle.dump((totalData), f)
        f.close()
    except(IOError):
        print("Data not found for year {}".format(y))
        

for y in range(1900, 2020):
    print("Parsing {}".format(y))
    writeYear(y)