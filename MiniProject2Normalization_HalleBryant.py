""" Mini Project 2 - Halle Bryant """

## Using the citywide tree inventory from Buffalo's OpenData project, I will
##create a normalized database containing information about each documented tree
##in the city. The dataset contains 133k entries with 28 columns each, with each
##entry corresponding to a different tree site. I have reduced the dataset to the
##first 10,000 rows for the sake of project runtime, but will reflect on its potential
##implications for the full inventory of trees in Buffalo. The dataset it available at https:/
##/data.buffalony.gov/Quality-of-Life/Tree-Inventory/n4ni-uuec/about_data.
##
## The goal of this project is to create a normalized database that city residents and
##officials can use to keep track of the species and quantities of trees throughout the city,
##as well as their distribution across Buffalo's different residential areas, impact on air
##quality and public health, and climate effect. For this project, I will write a proposal recommending
##to the city's parks department where to focus their tree-planting efforts in the upcoming year.
##
## Based on the criteria of the database Third Normal Form, I will create a database containing
##tables such that all entries are atomized and each row is unique (1NF), there are no tables with
##columns that are partially dependent on the primary key (2NF), and there are no tables with columns
##dependent on a non-key column (3NF).
##
##Based on the 28 columns included in the dataset, it should be broken into the following tables to
##meet the 3NF standard:
##
## Sites 
##[SiteID] Integer not null Primary Key
##[SiteNumber] Integer not null
##[SiteStatus] Text not null (VACANT/INHABITEDD)
##[Latitude] Float not null
##[Longitude] Float not null
##[AddressID] Integer not null
##Foreign Key (AddressID) References Addresses (AddressID)
##
##Addresses 
##[AddressID] Integer not null Primary Key
##[BuildingNumber] Integer not null
##[Street] Text not null
##[DistrictID] Integer not null
##Foreign Key (DistrictID) References Districts (DistrictID)
##
##Districts Table
##[DistrictID] Integer not null Primary Key
##[DistrictName] Text not null
##
##SiteSpecies Table
##[SiteID] Integer not null Primary Key
##[SpeciesID] Integer not null
##Foreign Key (SiteID) References Sites (SiteID)
##Foreign Key (SpeciesID) References Species (SpeciesID)
##
##Species Table
##[SpeciesID] Integer not null Primary Key
##[BotanicalName] Text not null
##[CommonName] Text not null
##
##Measurements Table
##[SiteID] Integer not null Primary Key
##[DiameterBreastHeight] Float not null
##[LeafSurfaceArea] Float not null
##Foreign Key (SiteID) References Sites (SiteID)
##
##EnvironmentalBenefits
##[SiteID] Integer not null Primary Key
##[PollutantsSaved] Float not null
##[C02Avoided] Float not null
##[C02Sequestered] Float not null
##[StormwaterGallonsSaved] Float not null
##[KilowattHoursSaved] Float not null
##[ThermsSaved] Float not null
##Foreign Key (SiteID) References Sites (SiteID)
##
##EconomicBenefits Table
##[SiteID] Integer not null Primary Key
##[C02Benefits] Float not null
##[StormwaterBenefits] Float not null
##[AirQualityBenefits] Float not null
##[PropertyBenefits] Float not null
##Foreign Key (SiteID) References Sites (SiteID)


import pandas as pd
import sqlite3
from sqlite3 import Error

df = pd.read_csv(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\Tree_Inventory_reduced_notclean.csv")
# Dropping rows with any empty cells
df_cleaned = df.dropna()

# Resaving for db creation 
df_cleaned.to_csv('Tree_Inventory_reduced.csv', index=False)


# I will use the following helper functions provided by Professor Narayanan in EAS503 SQL tutorials:
def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = ON")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):

    if drop_table_name: # You can optionally pass drop_table_name to drop the table.
        try:
            c = conn.cursor()
            c.execute(f"DROP TABLE IF EXISTS \"{drop_table_name}\"")
        except Error as e:
            print(e)

    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

##Since Tables for SiteID, Locations, and Addresses each
##reference a foreign table, it makes sense to start with CouncilDistricts so the right dictionaries
##will be in place at later steps.

## Step 1: Create CouncilDistricts Table.

def step1_create_councildistricts_table(datafile, database):
    districts = []
    with open(datafile) as file:
        header = None
        for line in file:
            if not line.strip():
                continue
            if not header:
                header = line.strip().split(",")
                continue

            data = line.strip().split(",")
            district = (data[21].strip(), )

            if district not in districts:
                districts.append(district)

    districts = sorted(districts, key = lambda ele: ele[0])

    create_district_table_sql = """
    CREATE TABLE [Districts](
    [DistrictID] Integer not null primary key,
    [District] Text not null)
    """

    def insert_district(conn, values):
        sql = "INSERT INTO Districts (District) VALUES (?)"
        cur = conn.cursor()
        cur.executemany(sql, values)
        return cur.lastrowid

    
    conn = create_connection(database, True) # deleting the database at first step for repeated runs 
    with conn:
        create_table(conn, create_district_table_sql)
        insert_district(conn, districts)

    conn.close()
        
# Checking for correct Step 1 output
step1_create_councildistricts_table(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\Tree_Inventory_reduced.csv", r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")
conn = sqlite3.connect(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")
df = pd.read_sql_query("select * from Districts", conn)
print(df)
conn.close()



# Step 2: Create dictionary of CouncilDistrict IDs

def step2_create_district_to_districtid_dict(database):
    conn = create_connection(database)
    sql_statement = "select * from Districts"
    df = pd.read_sql_query(sql_statement, conn)

    district_to_districtid = {}

    # Iterate through our df to store region:regionID mappings
    for rowid, value in df.iterrows():
        districtid, district = value
        district_to_districtid[district] = districtid
    conn.close()
    
    return district_to_districtid

# Checking for correct step 2 output
district_to_districtid = step2_create_district_to_districtid_dict(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")
print(district_to_districtid)



### Step 3: Create Addresses Table (References DistrictID)

def step3_create_addresses_table(datafile, database):
    district_to_districtid = step2_create_district_to_districtid_dict(database)
    addresses = []
    
    with open(datafile) as file:
        header = None
        for line in file:
            
            if not line.strip():
                continue
            if not header:
                header = line.strip().split(",")
                continue

            data = line.strip().split(",")
            address = (data[17], data[18], district_to_districtid[data[21]])

            if address not in addresses:
                addresses.append(address)

    addresses = sorted(addresses, key = lambda ele:'{} {} {}'.format(ele[2], ele[1], ele[0])) # sorts by district, streetname and then number

    create_addresses_table_sql = """
    CREATE TABLE [Addresses](
    [AddressID] Integer not null primary key,
    [BuildingNo] Integer not null,
    [Street] Text not null,
    [DistrictID] Integer not null,
    Foreign Key (DistrictID) References Districts (DistrictID)
    )
    """

    def insert_address(conn, values):
        sql = "INSERT INTO Addresses (BuildingNo, Street, DistrictID) VALUES (?,?,?)"
        cur = conn.cursor()
        cur.executemany(sql, values)
        return cur.lastrowid

    conn = create_connection(database)

    with conn:
        create_table(conn, create_addresses_table_sql)
        insert_address(conn, addresses)

    conn.close()

# Checking for correct Step 3 output
step3_create_addresses_table(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\Tree_Inventory_reduced.csv", r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")
conn = sqlite3.connect(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")
df = pd.read_sql_query("select * from Addresses", conn)
print(df)
conn.close()



# Step 4: Address to AddressID dict

def step4_create_address_to_addressid_dict(database):
    conn = create_connection(database)
    sql_statement = "select * from Addresses"
    df = pd.read_sql_query(sql_statement, conn)

    address_to_addressid = {}

    # Iterate through our df to store region:regionID mappings
    for rowid, value in df.iterrows():
        value = value.tolist()
        addressid = value[0]
        address = (*value[1:4],)
        address_to_addressid[address] = addressid
    conn.close()
    
    return address_to_addressid

# Checking for correct step 4 output
address_to_addressid = step4_create_address_to_addressid_dict(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")
print(address_to_addressid)


# Step 5: Create Sites table (references locationid, addressid)

def step5_create_sites_table(datafile, database):
    address_to_addressid = step4_create_address_to_addressid_dict(database)
    district_to_districtid = step2_create_district_to_districtid_dict(database)

    sites = []
    
    with open(datafile) as file:
        header = None
        for line in file:            
            if not line.strip():
                continue
            if not header:
                header = line.strip().split(",")
                continue

            data = line.strip().split(",")

            if (data[1] == "VACANT") or (data[1] == 0):
                status = "Vacant"
            else:
                status = "Inhabited"

            lat, long = float(data[23]), float(data[24])
            addressid = address_to_addressid[(int(data[17].split(".")[0]), data[18], district_to_districtid[data[21]])]
                
            site = (data[25], data[20], status, lat, long, addressid)

            if site not in sites:
                sites.append(site)

    sites = sorted(sites, key = lambda ele:ele[0]) # sorts by site id (which is preassigned in raw data)

    create_sites_table_sql = """
    CREATE TABLE [Sites](
    [SiteID] Integer not null Primary Key,
    [SiteNumber] Integer not null,
    [SiteStatus] Text not null,
    [Latitude] Float not null,
    [Longitude] Float not null,
    [AddressID] Integer not null,
    Foreign Key (AddressID) References Addresses (AddressID)
    )
    """

    def insert_sites(conn, values):
        sql = "INSERT INTO Sites (SiteID, SiteNumber, SiteStatus, Latitude, Longitude, AddressID) VALUES (?,?,?,?,?,?)"
        cur = conn.cursor()
        cur.executemany(sql, values)
        return cur.lastrowid

    conn = create_connection(database)

    with conn:
        create_table(conn, create_sites_table_sql)
        insert_sites(conn, sites)

    conn.close()

# Checking for correct Step 5 output
step5_create_sites_table(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\Tree_Inventory_reduced.csv", r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")

conn = sqlite3.connect(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")
df = pd.read_sql_query("select * from Sites", conn)
print(df)
conn.close()

##### Now that Sites, Locations, Addresses, and Council Districts have been broken down into more usable tables, it's time to separate the
####remaining data by topic/area of relevance. Namely SiteSpecies (type of tree at each individual site, Species (table of species and names),
####Measurements (sizes of trees), EnvironmentalBenefits (metrics for each tree related to their environmental impact), and EconomicBenefits
####(the recorded dollar amount associated with each tree's environmental impacts):  



#Step 6: Create Species table

def step6_create_species_table(datafile, database):
    all_species = []
    
    with open(datafile) as file:
        header = None
        for line in file:            
            if not line.strip():
                continue
            if not header:
                header = line.strip().split(",")
                continue

            data = line.strip().split(",")

            if (data[1] != "VACANT") and (data[1] != "STUMP") and (data[1] != "STUMP OBSTRUCTED") and (data[1] != "0") and (data[1] != "0") and (data[1] != "unsuitable vacant"):
                species = (data[1].split("'")[0].strip(), data[2].split("'")[0].strip())
                
            else:
                species = ("None", "None")

            if species not in all_species:
                all_species.append(species)

    all_species = sorted(all_species, key = lambda ele:ele[0])

    create_species_table_sql = """
    CREATE TABLE [Species](
    [SpeciesID] Integer not null Primary Key,
    [BotanicalName] Text not null,
    [CommonName] Text not null
    )
    """

    def insert_species(conn, values):
        sql = "INSERT INTO Species (BotanicalName, CommonName) VALUES (?,?)"
        cur = conn.cursor()
        cur.executemany(sql, values)
        return cur.lastrowid

    conn = create_connection(database)

    with conn:
        create_table(conn, create_species_table_sql)
        insert_species(conn, all_species)

    conn.close()

# Checking for correct Step 8 output
step6_create_species_table(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\Tree_Inventory_reduced.csv", r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")

conn = sqlite3.connect(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")
df = pd.read_sql_query("select * from Species", conn)
print(df)
conn.close()



# Step 7: Create Species to speciesID dict (relies only on botanical name key)

def step7_create_species_to_speciesid_dict(database):
    conn = create_connection(database)
    sql_statement = "select * from Species"
    df = pd.read_sql_query(sql_statement, conn)

    species_to_speciesid = {}

    for rowid, value in df.iterrows():
        speciesid = value[0]
        species = value[1]
        species_to_speciesid[species] = speciesid
    conn.close()
    
    return species_to_speciesid

# Checking for correct step 7 output
species_to_speciesid = step7_create_species_to_speciesid_dict(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")
print(species_to_speciesid)



# Step 8: Create SiteSpecies table (references SpeciesID)
def step8_create_sitespecies_table(datafile, database):
    species_to_speciesid = step7_create_species_to_speciesid_dict(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")
    sitespecies = []
    
    with open(datafile) as file:
        header = None
        for line in file:            
            if not line.strip():
                continue
            if not header:
                header = line.strip().split(",")
                continue

            data = line.strip().split(",")
            siteid = data[25]
            
            invalid_species = ["VACANT", "STUMP", "STUMP OBSTRUCTED", "0", "unsuitable vacant", "UNSUITABLE VACANT"]

            if data[1] in invalid_species:
                speciesid = species_to_speciesid["None"]
                
            else:
                speciesid = species_to_speciesid[data[1].split("'")[0].strip()]

            entry = (siteid, speciesid)
                

            if entry not in sitespecies:
                sitespecies.append(entry)
            
    sitespecies = sorted(sitespecies, key = lambda ele: float(ele[0]))

    create_sitespecies_table_sql = """
    CREATE TABLE [SiteSpecies](
    [SiteID] Integer not null Primary Key,
    [SpeciesID] Integer not null,
    Foreign Key (SiteID) References Sites (SiteID),
    Foreign Key (SpeciesID) References Species (SpeciesID)
    )
    """

    def insert_sitespecies(conn, values):
        sql = "INSERT INTO SiteSpecies (SiteID, SpeciesID) VALUES (?,?)"
        cur = conn.cursor()
        cur.executemany(sql, values)
        return cur.lastrowid

    conn = create_connection(database)

    with conn:
        create_table(conn, create_sitespecies_table_sql)
        insert_sitespecies(conn, sitespecies)

    conn.close()
    
## Checking for correct Step 8 output
step8_create_sitespecies_table(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\Tree_Inventory_reduced.csv", r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")

conn = sqlite3.connect(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")
df = pd.read_sql_query("select * from SiteSpecies", conn)
print(df)
conn.close()

# No dictionary is needed for the previous table. Now, it's time to make tables with each tree site's features:



# Step 9: Create Measurements table (references site id)

def step9_create_measurements_table(datafile, database):
    measurements = []
    
    with open(datafile) as file:
        header = None
        for line in file:            
            if not line.strip():
                continue
            if not header:
                header = line.strip().split(",")
                continue

            data = line.strip().split(",")


            siteid = data[25]
            dbh = data[3]
            lsa = data[16]
            
            measurement = (siteid, dbh, lsa)
            
            if measurement not in measurements:
                measurements.append(measurement)
            
    measurements = sorted(measurements, key = lambda ele: float(ele[0]))

    create_measurements_table_sql = """
    CREATE TABLE [Measurements](
    [SiteID] Integer not null,
    [DiameterBreastHeight] Float not null,
    [LeafSurfaceArea] Float not null,
    Foreign Key (SiteID) References Sites (SiteID)
    )
    """

    def insert_measurements(conn, values):
        sql = "INSERT INTO Measurements (SiteID, DiameterBreastHeight, LeafSurfaceArea) VALUES (?,?,?)"
        cur = conn.cursor()
        cur.executemany(sql, values)
        return cur.lastrowid

    conn = create_connection(database)

    with conn:
        create_table(conn, create_measurements_table_sql)
        insert_measurements(conn, measurements)

    conn.close()

#Checking output of Step 9
step9_create_measurements_table(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\Tree_Inventory_reduced.csv", r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")

conn = sqlite3.connect(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")
df = pd.read_sql_query("select * from Measurements", conn)
print(df)
conn.close()


# Step 10: Create EnvironmentalBenefits table

def step10_create_environmentalbenefits_table(datafile, database):
    env_benefits = []
    
    with open(datafile) as file:
        header = None
        for line in file:            
            if not line.strip():
                continue
            if not header:
                header = line.strip().split(",")
                continue

            data = line.strip().split(",")

            siteid = data[25]
            
            pollutants = data[14]
            co2avoid = data[8]
            co2seq = data[9]
            stormwater = data[6]
            kwh = data[11]
            therms = data[12]
            
            benefit = (siteid, pollutants, co2avoid, co2seq, stormwater, kwh, therms)
            
            if benefit not in env_benefits:
                env_benefits.append(benefit)
            
    env_benefits = sorted(env_benefits, key = lambda ele: float(ele[0]))

    create_env_benefits_table_sql = """
    CREATE TABLE [EnvironmentalBenefits](
    [SiteID] Integer not null Primary Key,
    [PollutantsSaved] Float not null,
    [CO2Avoided] Float not null,
    [CO2Sequestered] Float not null,
    [StormwaterGallonsSaved] Float not null,
    [KilowattHoursSaved] Float not null,
    [ThermsSaved] Float not null,
    Foreign Key (SiteID) References Sites (SiteID)
    )
    """

    def insert_env_benefits(conn, values):
        sql = "INSERT INTO EnvironmentalBenefits (SiteID, PollutantsSaved, CO2Avoided, CO2Sequestered, StormwaterGallonsSaved, KilowattHoursSaved, ThermsSaved) VALUES (?,?,?,?,?,?,?)"
        cur = conn.cursor()
        cur.executemany(sql, values)
        return cur.lastrowid

    conn = create_connection(database)

    with conn:
        create_table(conn, create_env_benefits_table_sql)
        insert_env_benefits(conn, env_benefits)

    conn.close()

#Checking output of Step 10
step10_create_environmentalbenefits_table(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\Tree_Inventory_reduced.csv", r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")

conn = sqlite3.connect(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")
df = pd.read_sql_query("select * from EnvironmentalBenefits", conn)
print(df)
conn.close()

# Step 11: Create EconomicBenefits table

def step11_create_economicbenefits_table(datafile, database):
    econ_benefits = []
    
    with open(datafile) as file:
        header = None
        for line in file:            
            if not line.strip():
                continue
            if not header:
                header = line.strip().split(",")
                continue

            data = line.strip().split(",")

            siteid = data[25]
            
            co2_savings = data[7]
            energy_savings = data[10]
            stormwater_savings = data[5]
            aq_savings = data[13]
            prop = data[15]
            total = data[4]
            
            benefit = (siteid, co2_savings,energy_savings, stormwater_savings, aq_savings, prop, total)
            
            if benefit not in econ_benefits:
                econ_benefits.append(benefit)
            
    econ_benefits = sorted(econ_benefits, key = lambda ele:float(ele[0]))

    create_econ_benefits_table_sql = """
    CREATE TABLE [EconomicBenefits](
    [SiteID] Integer not null Primary Key,
    [CO2Benefits] Float not null,
    [EnergyBenefits] Float not null,
    [StormwaterBenefits] Float not null,
    [AirQualityBenefits] Float not null,
    [PropertyBenefits] Float not null,
    [TotalYearlyBenefits] Float not null,
    Foreign Key (SiteID) References Sites (SiteID)
    )
    """

    def insert_econ_benefits(conn, values):
        sql = "INSERT INTO EconomicBenefits (SiteID, CO2Benefits, EnergyBenefits, StormwaterBenefits, AirQualityBenefits, PropertyBenefits, TotalYearlyBenefits) VALUES (?, ?, ?, ?, ?, ?, ?)"
        cur = conn.cursor()
        cur.executemany(sql, values)
        return cur.lastrowid

    conn = create_connection(database)

    with conn:
        create_table(conn, create_econ_benefits_table_sql)
        insert_econ_benefits(conn, econ_benefits)

    conn.close()

#Checking output of Step 11
step11_create_economicbenefits_table(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\Tree_Inventory_reduced.csv", r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")

conn = sqlite3.connect(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")
df = pd.read_sql_query("select * from EconomicBenefits", conn)
print(df)
conn.close()

# This completes the normalization portion of the project!
