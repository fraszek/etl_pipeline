import json
from pickle import INT
import psycopg2

file_path = 'C:/Users/frane/Desktop/Minor/DIA/Assignment/DBahn-berlin/station_data.json'
with open(file_path, 'r') as file:
    data = json.load(file)

connection = psycopg2.connect(
    dbname='DIA_Assignment',
    user='postgres',
    password='postgres',
    host='localhost',
    port='5432')

cursor = connection.cursor()
total = data['total']

#cursor.execute("")
#cursor.execute("""SELECT * FROM stations;""")
#result = cursor.fetchall()
#print(result)


records = []

for station in data['result']:
    number = station['number']
    #print("Number: ", number)
    ifopt = station['ifopt']
    name = station['name']
    mailingAddress_city = station['mailingAddress']['city']
    mailingAddress_zipcode = station['mailingAddress']['zipcode']
    mailingAddress_street = station['mailingAddress']['street']
    category = station['category']
    priceCategory = station['priceCategory']
    hasParking = station['hasParking']
    hasBicycleParking = station['hasBicycleParking']
    hasLocalPublicTransport = station['hasLocalPublicTransport']
    hasPublicFacilities = station['hasPublicFacilities']
    hasLockerSystem = station['hasLockerSystem']
    hasTaxiRank = station['hasTaxiRank']
    hasTravelNecessities = station['hasTravelNecessities']
    hasSteplessAccess = station['hasSteplessAccess']
    hasMobilityService = station['hasMobilityService']
    hasWiFi = station['hasWiFi']
    hasTravelCenter = station['hasTravelCenter']
    hasRailwayMission = station['hasRailwayMission']
    hasDBLounge = station['hasDBLounge']
    hasLostAndFound = station['hasLostAndFound']
    hasCarRental = station['hasCarRental']
    federalState = station['federalState']
    regionalbereich_number = station['regionalbereich']['number']
    regionalbereich_name = station['regionalbereich']['name']
    regionalbereich_shortName = station['regionalbereich']['shortName']
    aufgabentraeger_shortName = station['aufgabentraeger']['shortName']
    aufgabentraeger_name = station['aufgabentraeger']['name']
    timeTableOffice_email = station['timeTableOffice']['email']
    timeTableOffice_name =  station['timeTableOffice']['name']
    szentrale_number = station['szentrale']['number']
    szentrale_publicPhoneNumber = station['szentrale']['publicPhoneNumber']
    szentrale_name = station['szentrale']['name']
    stationManagement_number = station['stationManagement']['number']
    stationManagement_name = station['stationManagement']['name']
    productLine_productLine = station['productLine']['productLine']
    productLine_segment = station['productLine']['segment']
	
    evaNumbers = station['evaNumbers']
    eva_result = ()
    eva_geographicCoordinates = []
    for eva in station['evaNumbers']:
        eva_evaNumber = eva['number']
        #print("eva Number: ", eva_evaNumber)
        eva_geographicCoordinates_type = eva['geographicCoordinates']['type']
        eva_geographicCoordinates_longitude = eva['geographicCoordinates']['coordinates'][0]
        eva_geographicCoordinates_latitude = eva['geographicCoordinates']['coordinates'][1]
        eva_isMain = eva['isMain']
        eva_geographicCoordinates.append((eva_geographicCoordinates_longitude, eva_geographicCoordinates_latitude, 
                                          eva_evaNumber, eva_isMain, eva_geographicCoordinates_type))

    ril_result = ()
    ril_geographicCoordinates = []
    for ril in station['ril100Identifiers']:
        #print(("Ril Identifier: "), ril['rilIdentifier'])
        ril_id = ril['rilIdentifier']
        ril_isMain = ril['isMain']
        ril_hasSteamPermission = ril['hasSteamPermission']
        ril_steamPermission = ril['steamPermission']
        if(ril.get('geographicCoordinates') is not None):
            ril_geographicCoordinates_type = ril['geographicCoordinates']['type']
            ril_geographicCoordinates_longitude = ril['geographicCoordinates']['coordinates'][0]
            ril_geographicCoordinates_latitude = ril['geographicCoordinates']['coordinates'][1]
        ril_primaryLocationCode = ril['primaryLocationCode']
        #print(ril_isMain)
        ril_geographicCoordinates.append((ril_id, ril_geographicCoordinates_longitude, ril_geographicCoordinates_latitude))
        
        for(eva_long, eva_lat, eva_number, eva_isMain, eva_type) in eva_geographicCoordinates:
            if(eva_isMain == ril_isMain == True):
                a = 0
                
                records.append((number, 
                                ifopt,
                                name,
                                mailingAddress_city, 
                                mailingAddress_zipcode, 
                                mailingAddress_street, 
                                category, 
                                priceCategory, 
                                hasParking, 
                                hasBicycleParking, 
                                hasLocalPublicTransport, 
                                hasPublicFacilities, 
                                hasLockerSystem, 
                                hasTaxiRank,
                                hasTravelNecessities,
                                hasSteplessAccess, 
                                hasMobilityService,
                                hasWiFi, 
                                hasTravelCenter,
                                hasRailwayMission, 
                                hasDBLounge, 
                                hasLostAndFound, 
                                hasCarRental, 
                                federalState,
                                regionalbereich_number, 
                                regionalbereich_name, 
                                regionalbereich_shortName,
                                aufgabentraeger_shortName, 
                                aufgabentraeger_name, 
                                timeTableOffice_email, 
                                timeTableOffice_name,
                                szentrale_number, 
                                szentrale_publicPhoneNumber, 
                                szentrale_name, 
                                stationManagement_number,
                                stationManagement_name,
                                eva_number,
                                eva_type,
                                eva_long,
                                eva_lat,
                                eva_isMain,
                                ril_id,
                                ril_isMain,
                                ril_hasSteamPermission, 
                                ril_steamPermission, 
                                ril_geographicCoordinates_type,
                                ril_geographicCoordinates_longitude, 
                                ril_geographicCoordinates_latitude,
                                ril_primaryLocationCode, 
                                productLine_productLine, 
                                productLine_segment, ))
                
print(records[0])            
print(records.__len__())

cursor.execute("DELETE FROM stations;")
cursor.executemany("""INSERT INTO stations (
                        "number", 
                        "ifopt",
                        "name",
                        "mailingaddress_city",
                        "mailingaddress_zipcode",
                        "mailingaddress_street",
                        "category",
                        "pricecategory",
                        "hasparking",
                        "hasbicycleparking",
                        "haslocalpublictransport",
                        "haspublicfacilities",
                        "haslockersystem",
                        "hastaxirank",
                        "hastravelnecessities",
                        "hassteplessaccess",
                        "hasmobilityservice",
                        "haswifi",
                        "hastravelcenter",
                        "hasrailwaymission",
                        "hasdblounge",
                        "haslostandfound",
                        "hascarrental",
                        "federalstate",
                        "regionalbereich_number",
                        "regionalbereich_name",
                        "regionalbereich_shortname",
                        "aufgabentraeger_shortname",
                        "aufgabentraeger_name",
                        "timetableoffice_email",
                        "timetableoffice_name",
                        "szentrale_number",
                        "szentrale_publicphonenumber",
                        "szentrale_name",
                        "stationmanagement_number",
                        "stationmanagement_name",
                        "evanumber",
                        "evanumber_geographiccoordinates_type",
                        "evanumber_geographiccoordinates_longitude",
                        "evanumber_geographiccoordinates_latitude",
                        "evanumber_ismain",
                        "rilidentifier",
                        "rilidentifier_ismain",
                        "rilidentifier_hassteampermission",
                        "rilidentifier_steampermission",
                        "rilidentifier_geographiccoordinates_type",
                        "rilidentifier_geographiccoordinates_longitude",
                        "rilidentifier_geographiccoordinates_latitude",
                        "rilidentifier_primarylocationcode",
                        "productline_productline",
                        "productline_segment")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
               %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
               %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
               %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
               %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       """, records)
connection.commit()




    #print("Eva Geographic Coordinates: ", eva_geographicCoordinates)
    #print("Ril Geographic Coordinates: ", ril_geographicCoordinates)
    
    


        #add DB_Information, localServiceStaff

        #Making it so that each eva number matches with each ril number would be the best, but sometimes multiple ril numbers
        #per eva number

        #2.2 Should the query be done for only isMain stations? And given latitute and longitude, answer only for main or all?

        #Should we even load the data with different locations, not main eva numbers

    
#SELECT * FROM stations WHERE 