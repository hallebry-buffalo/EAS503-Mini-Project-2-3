import pandas as pd
import sqlite3


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

database =r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db"
pd.set_option("display.max_columns", None)

## Query 1: Average Environmental Benefits 
##To begin gathering metrics for the report, I want to return the average values of all inhabited tree sites
##for environmental impact, meaning CO2Avoided, CO2Sequestered, Pollutants absorbed, kWh saved, and therms saved.
##To do so, I"ll have to reference the EnvironmentalBenefits table joined with Sites where there are trees:


sql_query1 ="""Select AVG(CO2Avoided), AVG(CO2Sequestered), AVG(PollutantsSaved),
            AVG(StormwaterGallonsSaved), AVG(KilowattHoursSaved), AVG(ThermsSaved)
            From EnvironmentalBenefits
            Join Sites
            on Sites.SiteID = EnvironmentalBenefits.SiteID
            Where SiteStatus == "Inhabited"
            """

conn = create_connection(database)

with conn:
    query1_df = pd.read_sql_query(sql_query1, conn)
    print(query1_df)
    

# Query 2: Total Yearly Benefits 
##Next, I want to find the net economic benefit of all the city"s tree sites. To do so, I"ll have to find the sum
##of TotalYearlyBenefits in the EconomicBenefits table:

sql_query2 =    """Select SUM(TotalYearlyBenefits)
                From EconomicBenefits
                """

with conn:
    query2_df = pd.read_sql_query(sql_query2, conn)
    print(query2_df)


# Query 3: Number of Vacant Sites
## Next, I want to find the total number of vacant sites in the inventory. I"ll do so by referencing the Sites table and
## counting the number of entries *not* "Inhabited":

sql_query3 ="""Select COUNT(*)
            From Sites
            Where SiteStatus Not Like "Inhabited"
            """

with conn:
    query3_df = pd.read_sql_query(sql_query3, conn)
    print(query3_df)

# Query 4: Tree vacancies and districts
## Next, I want to find the number of trees out of my sample of 10,000 that are in the Ellicott District compared
##to the proportion residing in the wealthier Delaware district.

sql_query4a ="""Select COUNT(*)
            From Sites
            Join Addresses
            on Sites.AddressID = Addresses.AddressID
            Join Districts
            on Districts.DistrictID = Addresses.DistrictID
            Where (SiteStatus = "Inhabited") and (District = "Ellicott")
            """

with conn:
    query4a_df = pd.read_sql_query(sql_query4a, conn)
    print(query4a_df)

sql_query4b ="""Select COUNT(*)
            From Sites
            Join Addresses
            on Sites.AddressID = Addresses.AddressID
            Join Districts
            on Districts.DistrictID = Addresses.DistrictID
            Where (SiteStatus = "Inhabited") and (District = "Delaware")
            """

with conn:
    query4a_df = pd.read_sql_query(sql_query4a, conn)
    query4b_df = pd.read_sql_query(sql_query4b, conn)
    print(query4a_df, query4b_df)

# Query 5: Most common tree species in Buffalo
##Next, I"d like to determine which species is most common in the city:

sql_query5 ="""Select BotanicalName, CommonName, Count(SiteSpecies.SiteID)
            From Species
            Join SiteSpecies
            on Species.SpeciesID = SiteSpecies.SpeciesID
            Join Sites
            on Sites.SiteID = SiteSpecies.SiteID
            Where (SiteStatus = "Inhabited")
            GROUP BY Species.SpeciesID
            ORDER BY Count(SiteSpecies.SiteID) DESC
            """

with conn:
    query5_df = pd.read_sql_query(sql_query5, conn)
    print(query5_df)

# Query 6: Tree species with highest ability to absorb pollution based on the inventory subset
## This query revealed the majority of the top 5 most effective air purifying trees were Elm.

sql_query6 ="""Select CommonName, PollutantsSaved
            From EnvironmentalBenefits
            Join SiteSpecies
            on SiteSpecies.SiteID = EnvironmentalBenefits.SiteID
            Join Species
            on SiteSpecies.SpeciesID = Species.SpeciesID
            ORDER BY PollutantsSaved DESC
            """

with conn:
    query6_df = pd.read_sql_query(sql_query6, conn)
    print(query6_df)

conn.close()


# Using pandas to corroborate results

# Step 1: Get all tables from normalized database

conn = create_connection(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")
with conn: 
    # get Addresses
    addresses = pd.read_sql_query("select * from Addresses", conn)
    print(addresses)

    # get Districts
    districts = pd.read_sql_query("select * from Districts", conn)
    print(districts)
    
    # get EconomicBenefits
    economic_benefits = pd.read_sql_query("select * from EconomicBenefits", conn)
    print(economic_benefits)
    
    # get EnvironmentalBenefits
    environmental_benefits = pd.read_sql_query("select * from EnvironmentalBenefits", conn)
    print(environmental_benefits)
    
    # get Measurements
    measurements = pd.read_sql_query("select * from Measurements", conn)
    print(measurements)

    # get SiteSpecies
    site_species = pd.read_sql_query("select * from SiteSpecies", conn)
    print(site_species)

    # get Sites
    sites = pd.read_sql_query("select * from Sites", conn)
    print(sites)

    # get Species
    species = pd.read_sql_query("select * from Species", conn)
    print(species)

conn.close()

# Query 1 Pandas implementation: getting average environmental impact at active sites 
query1_join = pd.merge(
    sites[sites["SiteStatus"] == "Inhabited"],  
    environmental_benefits,
    on="SiteID",
    how="inner"
)

avg_environmental_benefits = query1_join[[
    "CO2Avoided", 
    "CO2Sequestered", 
    "PollutantsSaved",
    "StormwaterGallonsSaved", 
    "KilowattHoursSaved", 
    "ThermsSaved"
]].mean()

print(avg_environmental_benefits)


# Query 2 Pandas implementation: getting total yearly economic benefits across db
total_yearly_benefits = economic_benefits[["TotalYearlyBenefits"]].sum()
print(total_yearly_benefits)


# Query 3 Pandas implementation: getting number of vacant sites
number_of_vacant_sites = len(sites[sites.iloc[:,2] != "Inhabited"])
print(number_of_vacant_sites)


# Query 4 Pandas implementation: getting number of active sites in Ellicott dist. and Delaware dist.
query4a_join = sites[sites["SiteStatus"] == "Inhabited"].merge(addresses,on="AddressID", how="inner").merge(districts[districts["District"]=="Ellicott"], on="DistrictID",how="inner")
print(len(query4a_join))

query4b_join = sites[sites["SiteStatus"] == "Inhabited"].merge(addresses,on="AddressID", how="inner").merge(districts[districts["District"]=="Delaware"], on="DistrictID",how="inner")
print(len(query4b_join))


# Query 5 Pandas implementation: getting most common tree species

query5_join = species.merge(site_species, on="SpeciesID", how="inner").merge(sites[sites["SiteStatus"] == "Inhabited"], on="SiteID",how="inner")

query5_reorder= query5_join.groupby(["SpeciesID", "BotanicalName", "CommonName"]).size().reset_index(name="SiteCount").sort_values(by="SiteCount", ascending=False)[["BotanicalName", "CommonName", "SiteCount"]]

print(query5_reorder)


# Query 6 Pandas implementation: getting most pollution-reducing tree types

query6_join = environmental_benefits.merge(site_species,on="SiteID",how="inner").merge(species,on="SpeciesID",how="inner").sort_values(by="PollutantsSaved", ascending = False)[["CommonName","PollutantsSaved"]]

print(query6_join)








