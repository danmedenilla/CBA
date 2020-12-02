import MySQLdb
import pandas as pd
from datetime import datetime, timedelta
import itertools
import MySQLdb
import pandas as pd
from datetime import datetime as dt
from datetime import timedelta as td
import itertools
import numpy as np
import matplotlib.pyplot as plt


#Database connection
db = MySQLdb.connect(host = '192.168.150.112', user = 'pysys_local', passwd = 'NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg', db = 'analysis_db')
#dbmia = MySQLdb.connect(host = '192.168.150.112', user = 'pysys_local', passwd = 'NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg', db = 'analysis_db')

#Initialization
ls = list()

def budget_breakdown():
    query= "SELECT date, total_travel FROM analysis_db.fieldwork_bd_logs where date between '2016-01-01' and '2020-01-01'"
    budgetdf = pd.read_sql(query,db)
    return budgetdf

monthly_ave = pd.DataFrame()
budget_breakdown = budget_breakdown()
budget_breakdown['date'] = pd.to_datetime(budget_breakdown['date'])
budget_breakdown = budget_breakdown.set_index('date').resample('7D').sum()
budget_breakdown['pandas_SMA_3'] = budget_breakdown.rolling(window=3).mean()
budget_breakdown.index= budget_breakdown.index -  pd.offsets.Week(weekday=4)


init_date = dt.strptime("2018-01-01", "%Y-%m-%d")
for date in range (0,212):
    try:
        start_ts = (init_date + pd.offsets.Week(date)).strftime("%Y-%m-%d")
        end_ts = (init_date + pd.offsets.Week(date+1)).strftime("%Y-%m-%d")
        print(end_ts)
    
    
    
        def get_tsm_lists(db):
            query = "select tsm_name from analysis_db.tsm_sensors where tsm_name not in ('bayta', 'baysb', 'baytc', 'testa', 'testb', 'testc') and date_deactivated is null"
            
            localdf = pd.read_sql(query,db)
            ls= localdf.values
            merged = list((itertools.chain(*ls)))
            return merged
        
        def get_rain_list(db):
            query = "select gauge_name from analysis_db.rainfall_gauges where data_source = 'senslope' and gauge_name not in ('bayta', 'baysb', 'baytc', 'bayg', 'testa', 'testb', 'testc')  and date_deactivated is null"
            ls= list()
            localdf = pd.read_sql(query,db)
            ls= localdf.values
            merged = list((itertools.chain(*ls)))
            return merged
        
        def get_points(db,sensor,lgrname,startTs, endTs): #(db, tilt/rain, logger name, start date, end date)
            query= "SELECT DISTINCT ts FROM "+sensor +"_"+ lgrname + " WHERE ts >= "+ "'" + startTs+"'" +" and ts <= "+"'"+ endTs +"'" + " order by ts desc"
            localdf = pd.read_sql(query, db)
            if (localdf.empty == False): 
                localdf = localdf.reset_index().set_index('ts').resample('30T').first().reset_index().dropna()
            else:
                localdf = pd.DataFrame()
            return localdf
        def get_downtime(db,lgrname,startTs, endTs):
            query= "SELECT DISTINCT ts,battery1, csq FROM rain_"+ lgrname + " WHERE ts >= "+ "'" + startTs+"'" +" and ts <= "+"'"+ endTs +"'" + " order by ts desc"
            try:
                localdf = pd.read_sql(query, db)
                
                if (localdf.empty == False): 
                    localdf = localdf.reset_index().set_index('ts').resample('30T').first().reset_index().dropna()
                else:
                    localdf = pd.DataFrame()
                return localdf
            except:
                pass
            
        
        
        
        #MAIN CODE
        
        tilt_list = get_tsm_lists(db)
        rain_list = get_rain_list(db)
        tilt_list = sorted(tilt_list)
        rain_list = sorted(rain_list)
        
        #Date Format and Computation
        date_format = "%Y-%m-%d"
        a = dt.strptime(end_ts, date_format)
        b = dt.strptime(start_ts, date_format)
        delta = a-b
        total_uptime = (delta.days + 1) * 48
        
        
        #Projection
        projection_time = delta.days
        if(delta.days <366):
            proj = projection_time-total_uptime
        else:
            proj = 0
            
        tilt_result = pd.DataFrame(columns=('site', 'Percentage', 'Above 70'))
        rain_result = pd.DataFrame(columns=('site', 'Percentage', 'Above 70'))
        arq_result = pd.DataFrame(columns=('site', 'batt_ave', 'csq_ave'))
        
        #For arq batt and csq process
        z = 0
        while z < len(tilt_list):
            arq_points = get_downtime(db, tilt_list[z], start_ts, end_ts)
            #print(arq_points)
            if((arq_points is not None)==True):
                if(arq_points.empty == False):          
                    arq_points = arq_points.set_index('ts')
                else:
                    arq_points = pd.DataFrame({'battery1': [0], 'csq' :[0]})
            else:
                arq_points = pd.DataFrame({'battery1': [0], 'csq' :[0]})
        
            if (tilt_list[z][:3] == tilt_list[z+1][:3]):
                nxt_arq = get_downtime(db, tilt_list[z+1], start_ts, end_ts)
                if((nxt_arq is not None)==True):
                    if(nxt_arq.empty == False):
                        nxt_arq = nxt_arq.set_index('ts')
                        arq_points = pd.concat([arq_points,nxt_arq], axis=1)
                        arq_points = arq_points.reset_index()
                        z+=1
                    else:
                        arq_points = pd.DataFrame({'battery1': [0], 'csq' :[0]})
                        z+=1
                else:
                    arq_points = pd.DataFrame({'battery1': [0], 'csq' :[0]})
                    z+=1
            
            z+=1
            arq_d = {'site': [tilt_list[z-1][:3]], 'batt_ave': [arq_points.battery1.mean()], 'csq_ave': [arq_points.csq[0].mean()]}
            arq_df = pd.DataFrame(data = arq_d)
            arq_result = arq_result.append(arq_df, ignore_index = True)
            
            
            
        
        #For tilt sensor process
        x=0
        while x < len(tilt_list):
            tilt_points = get_points(db, 'tilt', tilt_list[x], start_ts, end_ts)
            
    
            if(tilt_points.empty == False):       
                tilt_points = tilt_points.set_index('ts')
            else:
                pass
            
            if (tilt_list[x][:3] == tilt_list[x+1][:3]):
                nxt_tilt = get_points(db, 'tilt', tilt_list[x+1], start_ts, end_ts)
                if(nxt_tilt.empty == False):
                    nxt_tilt = nxt_tilt.set_index('ts')
                    tilt_points = pd.concat([tilt_points,nxt_tilt], axis=1)
                    tilt_points = tilt_points.reset_index()
                    x+=1
                else:
                    tilt_points = tilt_points
                    x+=1
                
            
            x+=1
            tilt_uptime = len(tilt_points)#+proj
            tilt_percentage = (float(tilt_uptime/total_uptime))*100
                
            if (tilt_percentage >= 70): #sites with tilt percentage
                tilt_stat = 1
            else:
                tilt_stat = 0
            
            d = {'site': [tilt_list[x-1][:3]], 'Percentage': [tilt_percentage], 'Above 70': [tilt_stat]}
            df = pd.DataFrame(data = d)
            tilt_result = tilt_result.append(df, ignore_index=True)
            
    
            
        #tilt_result["site"] = tilt_result.Logger.str[:3]
        tilt_result = tilt_result.set_index("site")
        tilt_result = tilt_result.groupby(level=0).max()
        
        #For rain gauge sensor process
        #for y in range(0, len(rain_list)-1):
        y=0
        while y < len(rain_list):
        
            rain_points = get_points(db, 'rain', rain_list[y], start_ts, end_ts)
            if(rain_points.empty == False):       
                rain_points = rain_points.set_index('ts')
            else:
                pass
                if (y != len(rain_list)-1):
                    if (rain_list[y][:3] == rain_list[y+1][:3]):
                        nxt_rain = get_points(db, 'rain', rain_list[y+1], start_ts, end_ts)
                        if(nxt_rain.empty == False):
                            nxt_rain = nxt_rain.set_index('ts')
                            rain_points = pd.concat([rain_points,nxt_rain], axis=1)
                            rain_points = rain_points.reset_index()
                            y+=1;
                        else:
                            rain_points = rain_points
                            y+=1
        
                
            y+=1
            
            rain_uptime = len(rain_points)#+proj
            rain_percentage = (float(rain_uptime/total_uptime))*100
            if (rain_percentage >= 70):
                rain_stat = 1
            else:
                rain_stat = 0
            
            d = {'Logger': [rain_list[y-1]], 'Percentage': [rain_percentage], 'Above 70': [rain_stat]}
            df = pd.DataFrame(data = d)
            rain_result = rain_result.append(df, ignore_index=True)
        
        
        rain_result["site"] = rain_result.Logger.str[:3]
        rain_result = rain_result.set_index("site")
        rain_result = rain_result.groupby(level=0).max()
        
        #print(tilt_result)
        #print(rain_result)
        
        result = pd.concat([tilt_result, rain_result], axis=1, sort = True)
        result_ave_tilt = result.Percentage.mean()[[0]][0]
        result_ave_rain = result.Percentage.mean()[[1]][0]
        d = {'Month':[start_ts],'Uptime Subsurface': [result_ave_tilt], 'Uptime Rain Gauge': [result_ave_rain]}
        final_df = pd.DataFrame(data=d)
        
        monthly_ave = monthly_ave.append(final_df, ignore_index = True)
    except:
        pass
    continue

monthly_ave = monthly_ave.set_index("Month")
Total_Uptime_Budget = monthly_ave.join(budget_breakdown)

fig, (ax, ax1) = plt.subplots(2, sharex=True)

ax.plot(Total_Uptime_Budget.index, Total_Uptime_Budget['Uptime Subsurface'], 'o-', label='Subsurface Uptime')
ax.plot(Total_Uptime_Budget.index, Total_Uptime_Budget['Uptime Rain Gauge'], 'o-', label='Rain Gauge Uptime')
ax1.plot(Total_Uptime_Budget.index, Total_Uptime_Budget['total_travel'].fillna(0), 'o-', label='Fieldwork Budget')
ax1.plot(Total_Uptime_Budget.index, Total_Uptime_Budget['pandas_SMA_3'].fillna(0), 'o-', label='Rolling Mean (3mos)')
ax.legend(loc='best')
ax1.legend(loc='best')
fig.suptitle('Sensor Uptime vs Fieldwork Budget 2016-2019')
ax.set_ylabel("Sensor Uptime")
ax1.set_ylabel("Actual Fieldwork Budget")

plt.show()



