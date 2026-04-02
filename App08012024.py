import glob
import pyodbc
import datetime
import pandas as pd
import numpy as np
import time
import sys




yy = str(datetime.datetime.now().year)
mm = str("0"+str(datetime.date.today().month) if datetime.date.today().month<=9  else datetime.date.today().month)
dd = str("0"+str(datetime.date.today().day) if datetime.date.today().day<=9  else datetime.date.today().day)
hh = str("0"+str(datetime.datetime.now().hour) if datetime.datetime.now().hour<=9  else datetime.datetime.now().hour)

file_format = yy+mm+dd+hh

dt_ = datetime.datetime.now().date()
dt = str(datetime.datetime.now().date())
pre_dt = str(datetime.datetime.now().date() - datetime.timedelta(days=1))

#print(file_format)

path = f"{chr(92)}\\172.21.34.100\c$\production\monitoring\{file_format+'*'}.csv"
file = glob.glob(path)[-1] if len(glob.glob(path))>0 else sys.exit()
print(file)

now = datetime.datetime.now()
now = str(now.strftime("%H:%M:%S"))
if (now > "00:00:00" and now < "07:59:00"):
    Shift = "A"
    pre_shift = "C"
elif (now > "08:00:00" and now < "15:59:00"):
    Shift = "B"
    pre_shift = "A"
elif (now > "16:00:00" and now < "23:59:00"):
    Shift = "C"
    pre_shift = "B"



with open(file,'r') as temp_f:
    col_count = [ len(l.split(",")) for l in temp_f.readlines() ]

column_names = [i for i in range(0,max(col_count))]

f = pd.read_csv(file,header=None,delimiter=",",names=column_names, low_memory=False)

for i in range(len(f)):
    if (f.iloc[i][0]=="Order length"):
        index = i
        break

#print(index)

df2 = f.loc[(index+2):len(f),0:8]
df2.index = np.arange(0,len(df2))
df2.columns = ['Machine','Serial','Article','Actual_Cloth_Length','Set_Cloth_Length','Stop_Mode','Actual_Order_Length','Set_Order_Length','Units']
#print(df2)

conn = pyodbc.connect('Driver={SQL Server};'
        'Server=172.21.40.29;'
        'Database=DyingSheeting;'
        'uid=super;'
        'pwd=Trident@1')
cursor = conn.cursor()

if (now > "08:00:00" and now < "08:10:00") or (now > "16:00:00" and now < "16:10:00"):
    conn.execute(f" Update [DyingSheeting].[dbo].[TBS_Weaving] set Shift = '{Shift}' where Date = '{dt}' and Shift = '{pre_shift}' and (Status = '0' or Status = '-1') ")
    conn.commit()
elif (now > "00:00:00" and now < "00:10:00" ):
    conn.execute(f" Update [DyingSheeting].[dbo].[TBS_Weaving] set Shift = '{Shift}',Date = '{dt}' where Date = '{pre_dt}' and Shift = '{pre_shift}' and (Status = '0' or Status = '-1') ")
    conn.commit()

for i in range(len(df2)):
    pre_length = conn.execute(f"Select Actual_Length from [DyingSheeting].[dbo].[TBS_Weaving] where Date='{dt}' and Shift='{Shift}' and Machine_Number='{str(df2.iloc[i]['Machine'])}' and (Status = '0' or Status = '-1') ")
    p_length = pre_length.fetchall()
    if len(p_length)<=0:
        p_length = '-1'
    else:
        p_length = p_length[0][0]
    M_N = str(df2.iloc[i]['Machine'])
    N_L = str(df2.iloc[i]['Actual_Cloth_Length'])

    conn.execute(f"exec [DyingSheeting].[dbo].[UpdateTbsWeaving] @date='{dt}', @shift='{Shift}', @machine_no='{str(df2.iloc[i]['Machine'])}', @new_length='{str(df2.iloc[i]['Actual_Cloth_Length'])}', @article_no='{str(df2.iloc[i]['Article'])}', @pre_length='{p_length}',@filename = '{file}' ")
    conn.commit()



