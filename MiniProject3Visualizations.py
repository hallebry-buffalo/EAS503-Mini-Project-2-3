import pandas as pd
import sqlite3
import seaborn as sns
import matplotlib.pyplot as plt
import geopandas as gpd
from geopandas import GeoDataFrame
import geodatasets
import contextily as ctx
import plotly.express as px


"""This script contains the code for Project 3 visualizations, using Project 2's
normalized database to communicate for a public audience. My normalization project
focused on a dataset about trees/tree sites across Buffalo, their characteristics,
the distribution across different neighborhoods, and and the environmental and economic
impact they have on the city.

For my Project 3, I will use the database tables I created (converted to pandas dataframes)
to create visuals for a public-facing story about the benefits of a citywide reforestation
initiative, specifically focused on neighborhoods with little greenery and high risk for health
issues in the face of pollution due to hazardous infrastructure like the Humboldt Parkway."""


# Loading tables from normalized database 
conn = sqlite3.connect(r"C:\Users\hb102\Documents\Python\EAS 503 Mini Project 2-3\BuffaloTrees.db")

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

# Recreating certain query-based data frames for plots
# Query 1 Pandas implementation: getting average environmental impact at active sites 
query1_join = pd.merge(sites[sites["SiteStatus"] == "Inhabited"],environmental_benefits, on="SiteID",how="inner")

avg_environmental_benefits = query1_join[["CO2Avoided", "CO2Sequestered", "PollutantsSaved","StormwaterGallonsSaved", "KilowattHoursSaved", "ThermsSaved"]].mean()



# Query 2 Pandas implementation: getting total yearly economic benefits across db
total_yearly_benefits = economic_benefits[["TotalYearlyBenefits"]].sum()

# Query 3 Pandas implementation: getting number of vacant sites
number_of_vacant_sites = len(sites[sites.iloc[:,2] != "Inhabited"])


# Query 4 Pandas implementation: getting number of active sites in Ellicott dist. and Delaware dist.
query4_df = sites[sites["SiteStatus"] == "Inhabited"].merge(addresses,on="AddressID", how="inner").merge(districts, on="DistrictID",how="inner")
query4a = query4_df[query4_df["District"] == "Ellicott"]
query4b = query4_df[query4_df["District"] == "Delaware"]

# Query 5 Pandas implementation: getting most common tree species
query5_join = species.merge(site_species, on="SpeciesID", how="inner").merge(sites[sites["SiteStatus"] == "Inhabited"], on="SiteID",how="inner")

query5_reorder= query5_join.groupby(["SpeciesID", "BotanicalName", "CommonName"]).size().reset_index(name="SiteCount").sort_values(by="SiteCount", ascending=False)[["BotanicalName", "CommonName", "SiteCount"]]


# Query 6 Pandas implementation: getting most pollution-reducing tree types
query6_join = environmental_benefits.merge(site_species,on="SiteID",how="inner").merge(species,on="SpeciesID",how="inner").sort_values(by="PollutantsSaved", ascending = False)[["CommonName","PollutantsSaved"]]


# Setting seaborn style conventions
sns.set_style("whitegrid")


# Query 4 plots:
# Active sites by district 
ellicott_count = len(query4a)
delaware_count = len(query4b)
masten_count = len(query4_df[query4_df["District"] == "Masten"])
niagara_count = len(query4_df[query4_df["District"] == "Niagara"])
north_count = len(query4_df[query4_df["District"] == "North"])
south_count = len(query4_df[query4_df["District"] == "South"])
university_count = len(query4_df[query4_df["District"] == "University"])
lovejoy_count = len(query4_df[query4_df["District"] == "Lovejoy"])
fillmore_count = len(query4_df[query4_df["District"] == "Fillmore"])

query4_out = pd.DataFrame({'District': ['Ellicott', 'Delaware', "Masten", "Niagara","North", "South","University","Lovejoy","Fillmore"],'Active Sites': [ellicott_count, delaware_count, masten_count, niagara_count, north_count, south_count, university_count, lovejoy_count, fillmore_count]})

# District comparison pie chart
plt.figure(figsize=(8, 8))

plt.pie(
    query4_out['Active Sites'], 
    labels=None,
    explode = [0, 0.1, 0, 0, 0, 0, 0, 0, 0],
    autopct=None,
    startangle=90,
    pctdistance=0.85,
    shadow=True,
    colors=sns.cubehelix_palette(start=2, rot=0, dark=0, light=.95, reverse=True),
    textprops={'fontsize': 12, 'wrap': False},  
    wedgeprops={'edgecolor': 'w', 'linewidth': 1},
    labeldistance = 1
)
plt.legend(query4_out['District'], loc='upper right')
plt.title('Living Tree Sites Distribution by District in Sample of 10,000', fontsize=14, pad=20)
plt.tight_layout(pad=3)
plt.show()


# Query 5 plot: Most common tree species 
query5_top = query5_reorder[query5_reorder["CommonName"] != 'None'].head(15)

plt.figure(figsize=(12, 8))
sns.barplot(data=query5_top, x='SiteCount', y='CommonName', palette=sns.light_palette("seagreen"))
plt.title('Most Common Buffalo Tree Species')
plt.xlabel('Number of Sites in 10,000')
plt.ylabel('Tree Species')
plt.tight_layout()
plt.show()


# Query 6 plot: Top pollution-fighting tree species  
query6_top = query6_join.head(15)

plt.figure(figsize=(12, 8))
sns.barplot(data=query6_top, x='PollutantsSaved', y='CommonName', palette=sns.light_palette("seagreen"))
plt.title('Pollution-Mitigating Tree Species')
plt.xlabel('Pollutants absorbed (lbs/year)')
plt.ylabel('Tree Species')
plt.tight_layout()
plt.show()

# Combined environmental benefits visualization
plt.figure(figsize=(14, 8))
sns.heatmap(query1_join[["CO2Avoided", "CO2Sequestered", "PollutantsSaved", "StormwaterGallonsSaved", "KilowattHoursSaved", "ThermsSaved"]].corr(),annot=True, cmap= sns.light_palette("seagreen", as_cmap=True), center=0)
plt.title('Environmental Benefits Correlation Matrix')
plt.tick_params(axis='x', labelrotation=35)
plt.tight_layout(pad=3)
plt.show()

# Economic benefits distribution
plt.figure(figsize=(12, 6))
merged_econ = economic_benefits.merge(sites[sites["SiteStatus"]!= "Vacant"], on="SiteID",how="inner")
sns.histplot(merged_econ['TotalYearlyBenefits'], bins=30, kde=True, color = "darkgreen")
plt.title('Distribution of Total Yearly Economic Benefit per Inhabited Tree Site')
plt.xlabel('Dollar Value')
plt.ylabel('Count')
plt.tight_layout()
plt.show()








