# -*- coding: utf-8 -*-
"""
Created on Tue Sep  8 13:23:20 2020

@author: skyso
"""
# import matplotlib.pyplot as plt
import pandas as pd


df = pd.read_csv("cba_data.csv")
dfn = pd.DataFrame({'Fieldwork sites':['PNG (March 2018)', 'PNG,BAY(July 2016)'], 'Fieldwork Budget (php)': [df.budget1.sum(), df.budget2.sum()]})
#dfn = pd.DataFrame({'Fieldwork sites':['PAR, HIN, JOR, IME (Feb 2019)', 'LAY, LTE, HIN (Dec 2019)', 'AGB, INA, MAR, BLC (Dec 2019)'], 'Fieldwork Budget (php)': [df.budget1.sum(), df.budget2.sum(), df.budget3.sum()]})
# ax = dfn.plot.bar(x='Fieldwork sites', y = 'Fieldwork Budget (php)', rot = 0, color = "orange", title = "Comparison of Maintenance Budget for Different Clustered Sites ")
# yticks([0 50 100])
# yticklabels({'y = 0','y = 50','y = 100'})  

from matplotlib.ticker import FuncFormatter
import matplotlib.pyplot as plt
import numpy as np

x = np.arange(2)
money = dfn["Fieldwork Budget (php)"].values


def thousands(x, pos):
    'The two args are the value and tick position'
    return 'P%1.1fK' % (x * 1e-3)


formatter = FuncFormatter(thousands)

fig, ax = plt.subplots()
ax.yaxis.set_major_formatter(formatter)
plt.bar(dfn["Fieldwork sites"], dfn["Fieldwork Budget (php)"], color = "orange")
plt.xticks(x, dfn["Fieldwork sites"].values)
plt.title("Comparison of Maintenance Cost for Different Clustered Sites", fontsize = 20)
plt.xlabel("Fieldwork sites")
plt.ylabel("Fieldwork Budget (php)")
plt.show()



