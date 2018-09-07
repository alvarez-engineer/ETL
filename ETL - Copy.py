import pandas as pd
import numpy as np
from datetime import datetime as dt

# Creating multiple function takes more time upfront, but it allows for a
# more efficient time with scaling. For example the Inspecting funciton below
# will work on any database, and the Normal BP can be extended to include
# age groups and/or vitals.

demo = pd.read_csv('C:\pythonprograms\ETL\Demographics.csv')
bp = pd.read_csv('C:\pythonprograms\ETL\BloodPressure.csv')
demo_copy = pd.read_csv('C:\pythonprograms\ETL\Demographics - Copy.csv')
bp_copy = pd.read_csv('C:\pythonprograms\ETL\BloodPressure - Copy.csv')

# Inspecting the data for demographics
def Inspecting(db):
    # Shows top 5 rows
    top = db.head(5)
    print('Top: \n')
    print(top)
    # Shows bottom 5 rows
    bot = db.tail(5)
    print('\nBottom: \n')
    print(bot)
    # Gives extra info
    print('\nInfo: \n')
    info = db.info()
    # Gives stats
    stats = db.describe()
    print('\nStats: \n')
    print(stats)



# Data cleaning
def Cleaning(db):
    # Could also pass the database name so it can tie them together

    # Check cells with null
    null_sum = db.isnull().sum()
    # If there are cells with null then we want to deal with the rows,
    # in this case I will simple create a text file with that info
    # and eliminate the rows
    if null_sum.all() > 0:
        null_string = 'There are nulls.'
        # Drop Rows
        db_clean_row = db.dropna()
        return db_clean_row
    else:
        Null_string = 'There are no nulls'
        return db

# Call the inspecting function
# Inspecting(demo_copy)

# Call the cleaning funciton
demo_copy_clean = Cleaning(demo_copy)
bp_copy_clean = Cleaning(bp_copy)

# Now that everything has been loocked at and cleaned we can get
# to the fun stuff
# Function to take in the age in months and give back
def NormalBP(age):
    if age >=44:
        low_bp = 55
    else:
        low_bp = 46
    return low_bp

# Function take in id and gives me bp thresdhold
def PatientToBP(db, patient_id, date):
    # Make sure db is a pandas database
    # Make sure datetime is a datetime then extract the date
    # find the age
    criteria_1 = db['PERSON_ID'] == patient_id
    criteria_2 = db['SERVICE_DATE'] == date
    criteria_all = criteria_1 & criteria_2
    print(db[criteria_all].iloc[0,2])

    # db[(db['PERSON_ID'] == patient_id) & (db['SERVICE_DATE'] == date)]


    # age = db.loc[(db['PERSON_ID'] == patient_id) & db['SERVICE_DATE'] == date]
    # print(age)

    # Use other function to get low_bp
    # low_bp = NormalBP(age)
    # return low_bp

# Use a string split to grab the date, which always shows up before the ' '
date = bp_copy.iloc[1,1].split(' ')[0]

db = PatientToBP(demo_copy, 123, date)

# Build sub groups based on id first and then date second

# for index, row in demo_copy.iterrows():
#     print(demo_copy.iloc[index,0])
