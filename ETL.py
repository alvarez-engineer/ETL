import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Import/Create two databases
demo = pd.read_csv('Demographics.csv')
bp = pd.read_csv('BloodPressure.csv')

print(demo.dtypes)
print(demo)
print(demo.describe())

#  Check if there are any duplicates


# Clean the data
demo = demo.sort_index(axis=1, ascending=False)
