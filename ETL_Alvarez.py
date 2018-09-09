import pandas as pd
import numpy as np

# Creating multiple function takes more time upfront, but it allows for a more 
# scalable program. For example the low bp function can be modified to include 
# a high bp threshold and the code can be rerun with little modifications 

demo = pd.read_csv('C:/pythonPrograms/ETL/Demographics.csv')
bp = pd.read_csv('C:/pythonPrograms/ETL/BloodPressure.csv')

# Function used to inspect any database, may be a bit overkill but it helps 
def Inspect(db):
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

# Function used to check database for missing entries
def Clean(db):
    # Check cells with null
    null_sum = db.isnull().sum()
    # If there are cells with null then we want to deal with the rows,
    # in this case I will simply eliminate the rows, you can also have code
    # input the mean of the column
    if null_sum.all() > 0:
        null_string = 'There are nulls.'
        # Drop Rows
        db = db.dropna()
    else:
        null_string = 'There are no nulls'
    return db

# Function used to sort databases
def SortAndClean(db):
    # Makes sure the database is sorted based on PERSON_ID and SERVICE_DATE
    # or TIME
    if 'TIME' in db.columns:
        db_sorted = db.sort_values(by=['PERSON_ID', 'TIME'])
    else:
        db_sorted = db.sort_values(by=['PERSON_ID', 'SERVICE_DATE'])
    # Invoke cleaning function
    db_cleaned_sorted = Clean(db_sorted)
    return db_cleaned_sorted

# Function to take in the age in months and give back bp thresdhold, can be
# expanded to include other thresholds
def BPFromAge(age):
    # add a catch for unreasonable bp?
    if age >= 44:
        low_bp = 55
        # high_bp = 80
    else:
        low_bp = 46
        # high_bp = 75
    return low_bp
    # return low_bp, high_bp

# Function take in id and gives me bp thresdhold, includes BPFromAge 
def PatientToBP(db, patient_id, date):
    # Set the ceriteria using both the patient_id and service date to parse
    # demographics and get age
    criteria_id = db['PERSON_ID'] == patient_id
    criteria_date = db['SERVICE_DATE'] == date
    criteria_all = criteria_id & criteria_date
    # The iloc attribute is another way to parse through databases with
    # pandas, downside is it's static
    age = db[criteria_all].iloc[0,2]
    # Pass age to BPFromAge to get low bp for that age
    low_bp = BPFromAge(age)
    return low_bp
    
# This is my apply function, panda passes this function for each group created
# basically I check if the bp is below the low_bp and start two counters, if 
# it's not then the counter gets reset and the temp counter checks if we made
# it to the magical 15 minutes. Then I print out the duration when the counter
# hits 0 (this indicated the max value for that series).
def Under15(data):
    count = 0
    temp_count = 0
    # grab patient_id from a static location in each groups database
    patient_id = data.iloc[0,0]
    # grab date from a static location in each groups database
    date = data.iloc[0,1]
    # call the PatientToBP function to get the low bp value based on the
    # patient id and date of service
    low_bp = PatientToBP(demo, patient_id, date)
    for index, row in data.iterrows():
        if row['SYSTOLIC_BLOOD_PRESSURE'] < low_bp:
            count = count + 1
            temp_count = temp_count + 1
        else:
            count = 0
            # This counts duration, can be changed to any number
            if temp_count >= 15:
                # index -1 to place duration at the end of the condition
                data.loc[(index-1),'DURATION_LOW'] = temp_count
            temp_count = 0  
    return data

# decided to make a seperate database for testing
test = bp

# Sort and Clean the database
test = SortAndClean(test)

# Split datetime into date and time, easier to work with
test[['DATE', 'TIME']] = test['TIME'].str.split(' ', 1, expand=True)

# Organizing column headers
test = test[['PERSON_ID', 'DATE', 'TIME', 'SYSTOLIC_BLOOD_PRESSURE']]

# Creating another column to show subjects duration who go below target for at 
# least 15 min
test['DURATION_LOW'] = 0

# Build sub groups based on id first and then date second, this is akin to 
# using this kind of sql code
# SELECT column_name(s)
# FROM table_name
# GROUP BY column_name(s)
# ORDER BY column_name(s);
test_grouped = test.groupby(['PERSON_ID', 'DATE'])
# run this to see first line of all groups
# print(test_grouped.first())

# Apply my custom function explained above
test_apply = test_grouped.apply(Under15)

# Create final database to export
test_final = test_apply[['PERSON_ID','DATE','DURATION_LOW']]
test_final = test_final.loc[test_apply['DURATION_LOW'] > 0]

test_final.to_csv('FinalReport.csv',index=False)

# Let's make some pretty graphs, uncomment code below to see them
# test_grouped.plot(x='TIME', y='SYSTOLIC_BLOOD_PRESSURE')