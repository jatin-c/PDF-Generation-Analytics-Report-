import pandas as pd
from pandas.core.indexes.datetimes import date_range
import seaborn as sns
import matplotlib.pyplot as plt
import calendar
import numpy as np
from datetime import datetime, timedelta
import uuid
import time
import psycopg2
import copy
from influxdb import InfluxDBClient, DataFrameClient

def influx_init(client,payload):
    try:
        print("before")
        dbs=client.get_list_database()
        print("after")
    except Exception as e:
        return {"error":str(e)}
    print("line 19")
    db_x=[]
    x2=[]
    date_a =datetime.strptime(payload[0], "%d-%m-%Y")
    date_b = datetime.strptime(payload[1], "%d-%m-%Y")
    x2.append(date_a)
    x2.append(date_b)
    #print(x2[0].month==x2[1].month)
    if x2[0].month==x2[1].month:
        for i in dbs:
            if str(x2[0].month) in i['name'].split("_") and str(x2[0].year) in i['name'].split("_"):
                db_x.append(i['name'])
    else:
        for j in x2:
            for i in dbs:
                if str(j.month) in i['name'].split("_") and str(j.year) in i['name'].split("_"):
                    db_x.append(i['name'])
    return db_x


def master(engine,payload):
    json={}
    query="select * from modicare where `{fcolumns}`={fvalue};".format(fcolumns=payload[2],fvalue=repr(payload[3]))
    print("query 41 render ",query)
    df_master=pd.read_sql(query,con=engine)
    if df_master.empty:
        return json
    else:
        json['namelist']=df_master['Agent Name'].to_list()
        json['company']=df_master['Company'].to_list()
        json['w_location']=df_master['Working Location'].to_list()
        json['b_location']=df_master['Base Location'].to_list()
        json['department']=df_master['Department'].to_list()
        json['serial']=df_master['S.No.'].to_list()
        json['mac']=df_master['MAC User'].to_list()
        json['remark']=df_master['Remarks'].to_list()
        json['weekend']=df_master['Weekend'].to_list()
        json['name']=df_master['Name'].to_list()
        return json


def df_slicing(client, dbs,payload,idd):
    try:
        df_x=pd.DataFrame()
        df_y=pd.DataFrame()
        if len(dbs)>=2:
            for i in idd:
                query='select * from "agent_productivity_stats" where "agentid"={idx}'.format(idx=repr(str(i)))
                df1=client.query(query, database=dbs[0])
                df2=client.query(query, database=dbs[1])
                if bool(df1) and bool(df2):
                    df_x=df1['agent_productivity_stats'].copy()
                    df_y=df2['agent_productivity_stats'].copy()
                    df_x.columns = df_x.columns.str.replace(' ', '')
                    df_y.columns = df_y.columns.str.replace(' ', '')
                    df_x = df_x.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
                    df_x['date']=df_x.apply(lambda x: datetime.fromtimestamp(int(x['date'][:10])).strftime('%d-%m-%Y'),axis=1)
                    df_x.set_index(['date'],inplace=True)
                    df_y = df_y.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
                    df_y['date']=df_y.apply(lambda x: datetime.fromtimestamp(int(x['date'][:10])).strftime('%d-%m-%Y'),axis=1)
                    df_y.set_index(['date'],inplace=True)
                else:
                    pass
            if df_x.empty and df_y.empty:
                return False
            else:
                df_w=df_x[payload[0]:]
                df_z=df_y[:payload[1]]
                frames = [df_w,df_z]
                result = pd.concat(frames)
                result.reset_index(inplace=True)
                return result

        else:
            df_s=pd.DataFrame()
            for i in idd:
                query='select * from "agent_productivity_stats" where "agentid"={idd}'.format(idd=repr(str(i)))
                df3=client.query(query, database=dbs[0])
                if bool(df3):
                    df_s=df3['agent_productivity_stats'].copy()
                    df_s.columns = df_s.columns.str.replace(' ', '')
                    df_s = df_s.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
                    df_s['date']=df_s.apply(lambda x: datetime.fromtimestamp(int(x['date'][:10])).strftime('%d-%m-%Y'),axis=1)
                else:
                    pass
            if df_s.empty:
                return False
            else:
                df_s.set_index(['date'],inplace=True)
                df_t=df_s[payload[0]:payload[1]]
                df_t.reset_index(inplace=True)
                return df_t
    except Exception as e:
        return {"error":str(e)+"Inside function df_slicing"}



def pre_process(df_sliced,flag):
    try:
        if flag=='e':
            df_sliced = df_sliced.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
            df_sliced['Weekday']=df_sliced.apply(lambda x: calendar.day_name[datetime.strptime(x['date'],'%d-%m-%Y').weekday()],axis=1)
            df_sliced['UnproductiveTime(Readable)']=df_sliced.apply(lambda x: "{:%Hh %Mm}".format(timedelta(hours=float(x['unProductiveTime']))+datetime.min),axis=1)
            df_sliced['productiveTime(Readable)']=df_sliced.apply(lambda x: "{:%Hh %Mm}".format(timedelta(hours=float(x['productiveTime']))+datetime.min),axis=1)
            df_sliced['IdleTime(Readable)']=df_sliced.apply(lambda x: "{:%Hh %Mm}".format(timedelta(hours=float(x['screenLockTime']))+datetime.min),axis=1)
            df_sliced['TotalTime(Readable)']=df_sliced.apply(lambda x: "{:%Hh %Mm}".format(timedelta(hours=float(x['systemUpTime']))+datetime.min),axis=1)
            df_sliced['TotalActiveTime(Readable)']=df_sliced.apply(lambda x: "{:%Hh %Mm}".format(timedelta(hours=float(x['onScreenTime']))+datetime.min),axis=1)
            df_sliced['OfflineTime']=df_sliced.apply(lambda x: x['totalInactiveTime']+x['sessionBreakTime'],axis=1)
            df_sliced['OfflineTime(Readable)']=df_sliced.apply(lambda x: "{:%Hh %Mm}".format(timedelta(hours=float(x['OfflineTime']))+datetime.min),axis=1)
            df_sliced['Firstlogin']=df_sliced.apply(lambda x: "NaT" if x['loginTime1']=='NA'else datetime.fromtimestamp(int(x['loginTime1'][:10])).strftime("%I:%M %p"),axis=1)
            df_sliced['lastlogin']=df_sliced.apply(lambda x: "NaT" if x['loginTime2']=='NA'else datetime.fromtimestamp(int(x['loginTime2'][:10])).strftime("%I:%M %p"),axis=1)
            df_sliced['Firstlogout']=df_sliced.apply(lambda x: "NaT" if x['logoutTime1']=='NA'else datetime.fromtimestamp(int(x['logoutTime1'][:10])).strftime("%I:%M %p"),axis=1)
            df_sliced['lastlogout']=df_sliced.apply(lambda x: "NaT" if x['logoutTime2']=='NA'else datetime.fromtimestamp(int(x['logoutTime2'][:10])).strftime("%I:%M %p"),axis=1)
            filterd_e=df_sliced.filter(['date','Weekday','UnproductiveTime(Readable)','productiveTime(Readable)','IdleTime(Readable)','TotalTime(Readable)','TotalActiveTime(Readable)','OfflineTime','OfflineTime(Readable)','Firstlogin','lastlogin','Firstlogout','lastlogout','unProductiveTime','productiveTime','screenLockTime','systemUpTime','onScreenTime'],axis=1)
            return filterd_e
        elif flag=='s':
            df_sliced = df_sliced.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
            df_sliced['Weekday']=df_sliced.apply(lambda x: calendar.day_name[datetime.strptime(x['date'],'%d-%m-%Y').weekday()],axis=1)
            df_sliced['Month']=df_sliced.apply(lambda x: datetime.strptime(x['date'],'%d-%m-%Y').strftime('%b'),axis=1)
            filterd_s=df_sliced.filter(['date','Weekday','unProductiveTime','productiveTime','screenLockTime'],axis=1)
            return filterd_s
        elif flag=='p':
            filterd_p=df_sliced.filter(['unProductiveTime','productiveTime','screenLockTime'],axis=1)
            return filterd_p
    except Exception as e:
        return [str(e)+"Inside function pre-process"]


def evaluation_json(Weekend,df_sliced):
    json={}
    if type(df_sliced)==bool:
        return False
    else:
        flag='e'
        df_norm=pre_process(df_sliced,flag)
        if type(df_norm)==list:
            return df_norm
        json['ActiveScreenTime_sum']=round(df_norm['onScreenTime'].sum(),2)
        json['ActiveScreenTime_mean']=round(df_norm['onScreenTime'].mean(),2)
        json['IdleTime_sum']=round(df_norm['screenLockTime'].sum(),2)
        json['IdleTime_mean']=round(df_norm['screenLockTime'].mean(),2)
        json['ProductiveScreentime_sum']=round(df_norm['productiveTime'].sum(),2)
        json['ProductiveScreentime_mean']=round(df_norm['productiveTime'].mean(),2)
        json['UnProductiveScreentime_sum']=round(df_norm['unProductiveTime'].sum(),2)
        json['UnProductiveScreentime_mean']=round(df_norm['unProductiveTime'].mean(),2)
        json['productivetime_list']=df_norm['productiveTime(Readable)'].to_list()
        json['unproductivetime_list']=df_norm['UnproductiveTime(Readable)'].to_list()
        json['idletime_list']=df_norm['IdleTime(Readable)'].to_list()


        lst_flgn=df_norm['Firstlogin'].to_list()
        lst_flout=df_norm['Firstlogout'].to_list()
        lst_llgn=df_norm['lastlogin'].to_list()
        lst_llout=df_norm['lastlogout'].to_list()

        json['TotalTime(Hrs)_sum']=round(df_norm['systemUpTime'].sum(), 2)
        json['TotalTime(Hrs)_mean']=round(df_norm['systemUpTime'].mean(), 2)
        json['TotalActiveTime(Hrs)_sum']=round(df_norm['onScreenTime'].sum(), 2)
        json['offlinetime_sum']=round(df_norm['OfflineTime'].sum(),2)
        json['logintime_list']=[j if i=='NaT' else i for i, j in zip(lst_flgn, lst_llgn)]
        json['logouttime_list']=[k if l=='NaT' else l for k, l in zip(lst_flout, lst_llout)]
        json['date_list']=df_norm['date'].to_list()
        json['weekday_list']=df_norm['Weekday'].to_list()
        json['totaltime_list']=df_norm['TotalTime(Readable)'].to_list()
        json['totalactivetime_list']=df_norm['TotalActiveTime(Readable)'].to_list()
        json['offlinetime_list']=df_norm['OfflineTime(Readable)'].to_list()


        if len(Weekend.split("/"))>1:
            df_sat=df_norm[df_norm["Weekday"]=="Saturday"]
            df_sun=df_norm[df_norm["Weekday"]=="Sunday"]

            json['IdleTime_sat']=round(df_sat['screenLockTime'].sum(), 2)
            json['IdleTime_sun']=round(df_sun['screenLockTime'].sum(), 2)
            json['ProductiveScreentime_sat']=round(df_sat['productiveTime'].sum(), 2)
            json['ProductiveScreentime_sun']=round(df_sun['productiveTime'].sum(), 2)
            json['UnProductiveScreentime_sat']=round(df_sat['unProductiveTime'].sum(), 2)
            json['UnProductiveScreentime_sun']=round(df_sun['unProductiveTime'].sum(), 2)
            json['ActiveScreenTime_sat']=round(df_sat['onScreenTime'].sum(), 2)
            json['ActiveScreenTime_sun']=round(df_sun['onScreenTime'].sum(), 2)
            json['TotalTime(Hrs)_sat']=round(df_sat['systemUpTime'].sum(), 2)
            json['TotalTime(Hrs)_sun']=round(df_sun['systemUpTime'].sum(), 2)
        else:
            if Weekend=='Sunday':
                Wkend='Sunday'
            else:
                Wkend='Saturday'
            df_wk=df_norm[df_norm["Weekday"]==Wkend]
            json['IdleTime_wk']=round(df_wk['screenLockTime'].sum(), 2)
            json['ProductiveScreentime_wk']=round(df_wk['productiveTime'].sum(), 2)
            json['UnProductiveScreentime_wk']=round(df_wk['unProductiveTime'].sum(), 2)
            json['ActiveScreenTime_wk']=round(df_wk['onScreenTime'].sum(), 2)
            json['TotalTime(Hrs)_wk']=round(df_wk['systemUpTime'].sum(), 2)
        return json



def stacked_chart(df_sliced):
    if type(df_sliced)==bool:
        return False
    else:
        flag='s'
        df_norm=pre_process(df_sliced,flag)
        if type(df_norm)==list:
            return df_norm
        date_cl=df_norm['date'].to_list()
        date_range_str="From "+str(date_cl[0])+" To "+str(date_cl[-1])
        filepath_stacked="/home/centos/iassist/bot/pdfreport/stacked/{path}".format(path=str(uuid.uuid1())+".png")
        temp=df_norm.groupby(by=['Weekday'])
        res1=temp[['screenLockTime', 'productiveTime', 'unProductiveTime']].sum()
        cats = [ 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        res1=res1.reindex(cats)
        fig, ax = plt.subplots(1, figsize=(16, 8))
        # numerical x
        x = np.arange(0, len(res1.index))
        # plot bars
        plt.bar(x - 0.3, res1['screenLockTime'], width = 0.4, color = '#BC8F8F')
        plt.bar(x - 0.1, res1['productiveTime'], width = 0.4, color = '#E6E6FA')
        plt.bar(x + 0.1, res1['unProductiveTime'], width = 0.4, color = '#708090')
        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)
        # x y details
        plt.ylabel('Usage Duration')
        plt.xticks(x, res1.index)
        plt.xlim(-0.5, 6.5)
        # grid lines
        ax.set_axisbelow(True)
        ax.yaxis.grid(color='gray', linestyle='dashed', alpha=0.2)
        # title and legend
        plt.title('Cummulative sum of WeekDays over a Month, '+date_range_str, loc ='left')
        plt.legend(['IdleTime', 'ProductiveScreentime', 'UnProductiveScreentime'], loc='upper left', ncol = 3)
        fig.savefig(filepath_stacked,bbox_inches='tight')
        plt.close(fig)
        return filepath_stacked

def pie_chart(df_sliced):
    if type(df_sliced)==bool:
        return False
    else:
        flag='p'
        df_norm=pre_process(df_sliced,flag)
        if type(df_norm)==list:
            return df_norm
        sumIt=round(df_norm['screenLockTime'].sum(),2)
        sumprod=round(df_norm['productiveTime'].sum(),2)
        sumunprod=round(df_norm['unProductiveTime'].sum(),2)
        labels = ['IdleTime', 'ProductiveScreentime', 'UnProductiveScreentime']
        sizes = [sumIt, sumprod, sumunprod]
        colors = ['#ff9999','#66b3ff','#99ff99']
        filepath_analyse="/home/centos/iassist/bot/pdfreport/analyse/{path}".format(path=str(uuid.uuid1())+".png")
        plt.figure(figsize=(8, 8))
        explode = (0.05,0.05,0.05)
        fig, ax1 = plt.subplots()
        ax1.pie(sizes, colors = colors, labels=labels, autopct='%1.1f%%', startangle=90,pctdistance=0.85, explode = explode)
        #draw circle
        centre_circle = plt.Circle((0,0),0.70,fc='white')
        fig = plt.gcf()
        fig.gca().add_artist(centre_circle)
        ax1.axis('equal')
        plt.tight_layout()
        plt.legend(loc='lower left')
        plt.savefig(filepath_analyse,bbox_inches='tight')
        plt.close(fig)
        return filepath_analyse

def is_productive_true(to_be_splied):
    is_true=copy.deepcopy(to_be_splied[to_be_splied['isProductive']==True])
    return is_true

def is_productive_false(to_be_splied):
    is_false=copy.deepcopy(to_be_splied[to_be_splied['isProductive']==False])
    return is_false


def df_slice_app(client,idd,dbs,payload):
    try:
        df_x=pd.DataFrame()
        df_y=pd.DataFrame()
        if len(dbs)>=2:
            for i in idd:
                query='select * from "agent_app_usage_stats" where "agentid"={idx}'.format(idx=repr(str(i)))
                df1=client.query(query, database=dbs[0])
                df2=client.query(query, database=dbs[1])
                if bool(df1) and bool(df2):
                    df_x=df1['agent_app_usage_stats'].copy()
                    df_y=df2['agent_app_usage_stats'].copy()
                    df_x.columns = df_x.columns.str.replace(' ', '')
                    df_y.columns = df_y.columns.str.replace(' ', '')
                    df_x = df_x.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
                    df_x['date']=df_x.apply(lambda x: datetime.fromtimestamp(int(x['date'][:10])).strftime('%d-%m-%Y'),axis=1)
                    df_x.set_index(['date'],inplace=True)
                    df_y = df_y.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
                    df_y['date']=df_y.apply(lambda x: datetime.fromtimestamp(int(x['date'][:10])).strftime('%d-%m-%Y'),axis=1)
                    df_y.set_index(['date'],inplace=True)
                else:
                    pass
            if df_x.empty or df_y.empty:
                return False
            else:
                df_w=df_x[payload[0]:]
                df_z=df_y[:payload[1]]
                frames = [df_w,df_z]
                result = pd.concat(frames)
                result.reset_index(inplace=True)
                return result
        else:
            df_s=pd.DataFrame()
            for i in idd:
                query='select * from "agent_app_usage_stats" where "agentid"={idd}'.format(idd=repr(str(i)))
                df3=client.query(query, database=dbs[0])
                if bool(df3):
                    df_s=df3['agent_app_usage_stats'].copy()
                    df_s.columns = df_s.columns.str.replace(' ', '')
                    df_s = df_s.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
                    df_s['date']=df_s.apply(lambda x: datetime.fromtimestamp(int(x['date'][:10])).strftime('%d-%m-%Y'),axis=1)
                else:
                    pass
            if df_s.empty:
                return False
            else:
                df_s.set_index(['date'],inplace=True)
                df_t=df_s[payload[0]:payload[1]]
                df_t.reset_index(inplace=True)
                return df_t
    except Exception as e:
        return {"error":str(e)+"Inside function df_slicing_app"}


def productive_app(df_app):
    if type(df_app)==bool:
        return False
    else:
        df_application=is_productive_true(df_app)
        if df_application.empty:
            return False
        else:
            df_application['applicationName']=df_application['applicationName'].fillna('none')
            df_application['length']=df_application[['applicationName']].apply(lambda x: len(x['applicationName']),axis=1)
            df_application.drop(df_application[df_application['length'] > 30].index, inplace = True)
            df_ap=df_application[['applicationName','duration']]
            temp=df_ap.groupby(by=['applicationName'],as_index=False)['duration'].sum()
            temp.sort_values(['duration'], ascending=False, inplace=True)
            filepath="/home/centos/iassist/bot/pdfreport/applications_pro/{path}".format(path=str(uuid.uuid1())+".png")
            plt.figure(figsize=(8, 8))  
            plt.rc('ytick', labelsize=13)
            plot=sns.barplot(data=temp.head(8),x='duration',y='applicationName',palette='mako',edgecolor='lightgrey',lw=1.5);
            fig=plot.get_figure()
            fig.savefig(filepath,bbox_inches='tight')
            plt.close(fig)
            return filepath

def unproductive_app(df_app):
    if type(df_app)==bool:
        return False
    else:
        df_application=is_productive_false(df_app)
        if df_application.empty:
            return False
        else:
            df_application['applicationName']=df_application['applicationName'].fillna('none')
            df_application['length']=df_application[['applicationName']].apply(lambda x: len(x['applicationName']),axis=1)
            df_application.drop(df_application[df_application['length'] > 30].index, inplace = True)
            df_ap=df_application[['applicationName','duration']]
            temp=df_ap.groupby(by=['applicationName'],as_index=False)['duration'].sum()
            temp.sort_values(['duration'], ascending=False, inplace=True)
            filepath="/home/centos/iassist/bot/pdfreport/applications_unpro/{path}".format(path=str(uuid.uuid1())+".png")
            plt.figure(figsize=(8, 8))   
            plt.rc('ytick', labelsize=13)
            plot=sns.barplot(data=temp.head(8),x='duration',y='applicationName',palette='rocket_r',edgecolor='lightgrey',lw=1.5);
            fig=plot.get_figure()
            fig.savefig(filepath,bbox_inches='tight')
            plt.close(fig)
            return filepath

def df_slice_web(client,idd,dbs,payload):
    try:
        df_x=pd.DataFrame()
        df_y=pd.DataFrame()
        if len(dbs)>=2:
            for i in idd:
                query='select * from "agent_browser_usage_stats" where "agentid"={idx}'.format(idx=repr(str(i)))
                df1=client.query(query, database=dbs[0])
                df2=client.query(query, database=dbs[1])
                if bool(df1) and bool(df2):
                    df_x=df1['agent_browser_usage_stats'].copy()
                    df_y=df2['agent_browser_usage_stats'].copy()
                    df_x.columns = df_x.columns.str.replace(' ', '')
                    df_y.columns = df_y.columns.str.replace(' ', '')
                    df_x = df_x.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
                    df_x['date']=df_x.apply(lambda x: datetime.fromtimestamp(int(x['date'][:10])).strftime('%d-%m-%Y'),axis=1)
                    df_x.set_index(['date'],inplace=True)
                    df_y = df_y.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
                    df_y['date']=df_y.apply(lambda x: datetime.fromtimestamp(int(x['date'][:10])).strftime('%d-%m-%Y'),axis=1)
                    df_y.set_index(['date'],inplace=True)
                else:
                    pass
            if df_x.empty or df_y.empty:
                return False
            else:
                df_w=df_x[payload[0]:]
                df_z=df_y[:payload[1]]
                frames = [df_w,df_z]
                result = pd.concat(frames)
                result.reset_index(inplace=True)
                return result

        else:
            df_s=pd.DataFrame()
            for i in idd:
                query='select * from "agent_browser_usage_stats" where "agentid"={idd}'.format(idd=repr(str(i)))
                df3=client.query(query, database=dbs[0])
                if bool(df3):
                    df_s=df3['agent_browser_usage_stats'].copy()
                    df_s.columns = df_s.columns.str.replace(' ', '')
                    df_s = df_s.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
                    df_s['date']=df_s.apply(lambda x: datetime.fromtimestamp(int(x['date'][:10])).strftime('%d-%m-%Y'),axis=1)
                else:
                    pass
            if df_s.empty:
                return False
            else:
                df_s.set_index(['date'],inplace=True)
                df_t=df_s[payload[0]:payload[1]]
                df_t.reset_index(inplace=True)
                return df_t
    except Exception as e:
        return {"error":str(e)+"Inside function df_slicing_web"}
    




def productive_web(df_web):
    if type(df_web)==bool:
        return False
    else:
        df_webusage=is_productive_true(df_web)
        if df_webusage.empty:
            return False
        else:
            df_webusage.drop_duplicates(inplace=True)
            df_webusage['url']=df_webusage['url'].fillna('none')
            df_webusage['length']=df_webusage[['url']].apply(lambda x: len(x['url']),axis=1)
            df_webusage.drop(df_webusage[df_webusage['length'] > 30].index, inplace = True)
            df_wb=df_webusage[['url','duration']]
            temp=df_wb.groupby(by=['url'],as_index=False)['duration'].sum()
            temp.sort_values(['duration'], ascending=False, inplace=True)
            filepath="/home/centos/iassist/bot/pdfreport/webusage_pro/{path}".format(path=str(uuid.uuid1())+".png")
            plt.figure(figsize=(8, 8))
            plt.rc('ytick', labelsize=13)
            plot=sns.barplot(data=temp.head(8),x='duration',y='url',palette='mako',edgecolor='lightgrey',lw=1.5);
            fig=plot.get_figure()
            fig.savefig(filepath,bbox_inches='tight')
            plt.close(fig)
            return filepath

def unproductive_web(df_web):
    if type(df_web)==bool:
        return False
    else:
        df_webusage=is_productive_false(df_web)
        if df_webusage.empty:
            return False
        else:
            df_webusage.drop_duplicates(inplace=True)
            df_webusage['url']=df_webusage['url'].fillna('none')
            df_webusage['length']=df_webusage[['url']].apply(lambda x: len(x['url']),axis=1)
            df_webusage.drop(df_webusage[df_webusage['length'] > 30].index, inplace = True)
            df_wb=df_webusage[['url','duration']]
            temp=df_wb.groupby(by=['url'],as_index=False)['duration'].sum()
            temp.sort_values(['duration'], ascending=False, inplace=True)
            filepath="/home/centos/iassist/bot/pdfreport/webusage_unpro/{path}".format(path=str(uuid.uuid1())+".png")
            plt.figure(figsize=(8, 8))
            plt.rc('ytick', labelsize=13)
            plot=sns.barplot(data=temp.head(8),x='duration',y='url',palette='rocket_r',edgecolor='lightgrey',lw=1.5);
            fig=plot.get_figure()
            fig.savefig(filepath,bbox_inches='tight')
            plt.close(fig)
            return filepath