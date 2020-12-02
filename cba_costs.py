# -*- coding: utf-8 -*-
"""
Created on Tue Sep 29 15:29:27 2020

@author: skyso
"""
import pandas as pd
from matplotlib.ticker import FuncFormatter
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_csv("CBA_cost.csv")

x = np.arange(19)
C2018 = df['2018'].values
C2020 = df['2020'].values

def thousands(x, pos):
    'The two args are the value and tick position'
    return 'P%1.1fK' % (x * 1e-3)

formatter = FuncFormatter(thousands)

fig, ax = plt.subplots()
#ax.yaxis.set_major_formatter(formatter)
ax.bar(x + 0.0, df['2018'].values, color = 'b', width = 0.25)
ax.bar(x + 0.25, df['2020'].values, color = 'g', width = 0.25)
plt.xticks(x, df.Site.values)
plt.title("EWS-L Hardware Components Cost from 2018 to 2020")
plt.xlabel("Sites")
plt.ylabel("Cost (php)")
plt.show()
