from fpdf import FPDF
from datetime import datetime
import pandas as pd
import uuid
from pdfreport.render import pie_chart, stacked_chart, evaluation_json, master, influx_init, df_slicing, df_slice_app, df_slice_web,productive_app,unproductive_app,productive_web,unproductive_web
from sqlalchemy import create_engine
from influxdb import InfluxDBClient, DataFrameClient
pdf=FPDF()


def peepify(client,conn,payload):
    mon=",".join(list(set([datetime.strptime(payload[0],'%d-%m-%Y').strftime('%b'),datetime.strptime(payload[1],'%d-%m-%Y').strftime('%b')])))
    ser=1
    username = "root"
    password = "123456789"
    host = "localhost"
    database = "dlp"
    connection = "mysql+pymysql://" +username + ":"+password + "@"+ host + "/" + database
    engine = create_engine(connection,echo=True)
    master_json= master(engine,payload)
    try:
        print("chlja bhai please")
        query="select agentid, agentname from agentmaster;"
        df_mstr=pd.read_sql(query,con=conn)
    except Exception as e:
        return str(e)+"during postgres"
    if bool(master_json):
        print("before influx connection")
        print(client.get_list_database())
        dbs = influx_init(client,payload)
        print("connected to influx")
        if type(dbs)==dict:
            return dbs['error']
        else:
            for name,name2,w_location,b_location,department,group,remk,mac,Weekend in zip(master_json['name'],master_json['namelist'],master_json['w_location'],master_json['b_location'],master_json['department'],master_json['company'],master_json['remark'],master_json['mac'],master_json['weekend']):
                try:
                    df_idd=df_mstr[df_mstr['agentname']==str(name2)].copy()
                except Exception as e:
                    return str(e)
                if df_idd.empty:
                    name_header=str(name)+", "+str(department)
                    location=w_location+", "+b_location
                    WIDTH = 210
                    HEIGHT = 297
                    pdf.add_page()
                    if remk !='none' and mac !='none':
                        remk=remk
                        mac=mac
                    elif mac!='none':
                        remk=" "
                    elif remk!='none':
                        mac=" "
                    else:
                        remk=" "
                        mac=" "
                    pdf.set_font('Arial', 'B', 10)
                    pdf.text(10, 20, name_header)
                    pdf.text(10, 15, str(group))
                    pdf.text(160, 15, str(mac))
                    pdf.set_font('Arial', 'B', 10)
                    pdf.text(86, 6, str(remk))
                    pdf.text(86, 70, "AgentId is not available in the database")
                    pdf.set_font('Arial', 'U', 6)
                    pdf.text(11,291,"S.No :- "+str(ser))
                    pdf.set_font('Arial', 'B', 8)
                    pdf.text(10, 25, str(location))
                    pdf.line(10, 27, 200, 27)
                else:
                    idd=df_idd['agentid'].to_list()
                    df_sliced=df_slicing(client, dbs, payload, idd)
                    if type(df_sliced)==dict:
                        return df_sliced['error']
                    eval_json=evaluation_json(Weekend, df_sliced)
                    if type(eval_json)==list:
                        return eval_json[0]
                    name_header=str(name)+", "+str(department)
                    location=w_location+", "+b_location
                    if eval_json==False:
                        WIDTH = 210
                        HEIGHT = 297
                        pdf.add_page()
                        if remk !='none' and mac !='none':
                            remk=remk
                            mac=mac
                        elif mac!='none':
                            remk=" "
                        elif remk!='none':
                            mac=" "
                        else:
                            remk=" "
                            mac=" "
                        pdf.set_font('Arial', 'B', 10)
                        pdf.text(10, 20, name_header)
                        pdf.text(10, 15, str(group))
                        pdf.text(160, 15, str(mac))
                        pdf.set_font('Arial', 'B', 10)
                        pdf.text(86, 6, str(remk))

                        pdf.set_font('Arial', 'U', 6)
                        pdf.text(11,291,"S.No :- "+str(ser))
                        pdf.set_font('Arial', 'B', 8)
                        pdf.text(10, 25, str(location))
                        pdf.line(10, 27, 200, 27)
                    else:
                        WIDTH = 210
                        HEIGHT = 297
                        pdf.add_page()
                        if remk !='none' and mac !='none':
                            remk=remk
                            mac=mac
                        elif mac!='none':
                            remk=" "
                        elif remk!='none':
                            mac=" "
                        else:
                            remk=" "
                            mac=" "

                        pdf.set_font('Arial', 'B', 10)
                        pdf.text(10, 20, name_header)
                        pdf.text(10, 15, str(group))
                        pdf.text(160, 15, str(mac))
                        pdf.set_font('Arial', 'B', 10)
                        pdf.text(86, 6, str(remk))

                        pdf.set_font('Arial', 'U', 6)
                        pdf.text(11,291,"S.No :- "+str(ser))

                        pdf.set_font('Arial', 'B', 8)
                        pdf.text(10, 25, str(location))

                        pdf.line(10, 27, 200, 27)
                        #file_company=company(name,engine)
                        fr_om=eval_json['date_list'][0]
                        till=eval_json['date_list'][-1]

                        list1=eval_json['date_list']
                        list2=eval_json['weekday_list']
                        list3=eval_json['logintime_list']
                        list4=eval_json['logouttime_list']
                        list5=eval_json['totaltime_list']
                        list6=eval_json['totalactivetime_list']
                        list7=eval_json['productivetime_list']
                        list8=eval_json['unproductivetime_list']
                        list9=eval_json['idletime_list']
                        list10=eval_json['offlinetime_list']


                        pdf.set_font('Arial', 'B', 10)
                        pdf.text(160,20, 'Monthly Worktime Profile')
                        date_range=str(fr_om)+' - '+str(till)+" ("+str(mon)+")"
                        pdf.set_font('Arial','B', 6)
                        pdf.text(161,24,date_range)

                        text0='Date'
                        text00='Day'
                        text1='Login time'
                        text2='Logout time'
                        text3='Total time'
                        text4='Total Active Time'
                        text5='Productive time'
                        text6='UnProductive time'
                        text7='Idle Time'
                        text8='Offline'

                        pdf.set_font('Arial', 'B', 8)
                        pdf.text(8, 33, text0)
                        pdf.text(22, 33, text00)
                        pdf.text(38, 33, text1)
                        pdf.text(58, 33, text2)
                        pdf.text(78, 33, text3)
                        pdf.text(96, 33, text4)
                        pdf.text(124, 33, text5)
                        pdf.text(148, 33, text6)
                        pdf.text(177, 33, text7)
                        pdf.text(195, 33, text8)


                        z=38
                        for i,j,k,l,m,n,o,p,q,r in zip(list1,list2,list3,list4,list5,list6,list7,list8,list9,list10):
                            if j=='Saturday' or j=='Sunday':
                                pdf.set_text_color(255,0,0)
                                pdf.set_font('Arial', '', 6)
                                pdf.text(7, z, i)
                                pdf.text(22, z, j)
                                pdf.text(40, z, k)
                                pdf.text(60, z, l)
                                pdf.text(79, z, str(m))
                                pdf.text(100, z, str(n))
                                pdf.text(126, z, str(o))
                                pdf.text(152, z, str(p))
                                pdf.text(178, z, str(q))
                                pdf.text(197, z, str(r))
                                z=z+4
                                pdf.set_text_color(0,0,0)
                            else:
                                pdf.set_text_color(0,0,0)
                                pdf.set_font('Arial', '', 6)
                                pdf.text(7, z, i)
                                pdf.text(22, z, j)
                                pdf.text(40, z, k)
                                pdf.text(60, z, l)
                                pdf.text(79, z, str(m))
                                pdf.text(100, z, str(n))
                                pdf.text(126, z, str(o))
                                pdf.text(152, z, str(p))
                                pdf.text(178, z, str(q))
                                pdf.text(197, z, str(r))
                                z=z+4
                        h=0
                        h=z+2
                        text_ttl="Total: "
                        pdf.set_font('Arial', 'B', 6)
                        pdf.text(7, h, text_ttl)
                        pdf.text(79, h, str(eval_json['TotalTime(Hrs)_sum'])+'Hrs')
                        pdf.text(100, h, str(eval_json['TotalActiveTime(Hrs)_sum'])+'Hrs')
                        pdf.text(126, h, str(eval_json['ProductiveScreentime_sum'])+'Hrs')
                        pdf.text(152, h, str(eval_json['UnProductiveScreentime_sum'])+'Hrs')
                        pdf.text(178, h, str(eval_json['IdleTime_sum'])+'Hrs')
                        pdf.text(197, h, str(eval_json['offlinetime_sum'])+'Hrs')

                        pdf.add_page()
                        if remk !='none' and mac !='none':
                            remk=remk
                            mac=mac
                        elif mac!='none':
                            remk=" "
                        elif remk!='none':
                            mac=" "
                        else:
                            remk=" "
                            mac=" "

                        pdf.set_font('Arial', 'B', 10)
                        pdf.text(10, 20, name_header)
                        pdf.text(10, 15, str(group))
                        pdf.text(160, 15, str(mac))
                        pdf.set_font('Arial', 'B', 10)
                        pdf.text(86, 6, str(remk))

                        pdf.set_font('Arial', 'U', 6)
                        pdf.text(11,291,"S.No :- "+str(ser))

                        pdf.set_font('Arial', 'B', 8)
                        pdf.text(10, 25, str(location))


                        pdf.set_font('Arial', 'B', 10)
                        pdf.text(160,20, 'Monthly Worktime Profile')
                        date_range=str(fr_om)+' - '+str(till)+" ("+str(mon)+")"
                        pdf.set_font('Arial','B', 6)
                        pdf.text(161,24,date_range)

                        pdf.line(10, 27, 200, 27)

                        totaltime_str='Total Time:'
                        idle_str='Idle Time:'
                        active_str='Active Screen Time:'
                        productive_str='Productive Time:'
                        unproductive_str='UnProductive Time:'



                        pie_plot=pie_chart(df_sliced)
                        if type(pie_plot)==list:
                            return pie_plot[0]

                        if pie_plot==False:
                            pdf.set_font('Arial', 'B', 6)
                            pdf.text(50, 40,"Data not available")
                        else:
                            pdf.image(pie_plot, 55, 38, WIDTH/2)

                        stacked_plot=stacked_chart(df_sliced)
                        if type(stacked_plot)==list:
                            return stacked_plot[0]

                        if stacked_plot==False:
                            pdf.set_font('Arial', 'B', 6)
                            pdf.text(120, 168,"Data not available")
                        else:
                            pdf.image(stacked_plot,10, 178, WIDTH/1.1)


                        #pdf.image(file_company,22,103, WIDTH/2.5)
                        #pdf.set_font('Arial', '', 6)

                        if len(Weekend.split("/"))>1:
                            pdf.set_font('Arial','U', 10)
                            pdf.text(22,135, 'Total WorkTime')

                            pdf.set_font('Arial','B', 6)
                            pdf.text(24,140,totaltime_str)
                            pdf.text(38,140,str(eval_json['TotalTime(Hrs)_sum'])+'Hrs')
                            pdf.text(15,145, active_str)
                            pdf.text(38,145,str(eval_json['ActiveScreenTime_sum'])+'Hrs')
                            pdf.text(25,150, idle_str)
                            pdf.text(38,150,str(eval_json['IdleTime_sum'])+'Hrs')
                            pdf.text(18,155, productive_str)
                            pdf.text(38,155,str(eval_json['ProductiveScreentime_sum'])+'Hrs')
                            pdf.text(15,160, unproductive_str)
                            pdf.text(38,160,str(eval_json['UnProductiveScreentime_sum'])+'Hrs')

                            pdf.set_font('Arial','U', 10)
                            pdf.text(111,135, 'WorkTime on Weekend(Sat)')

                            pdf.set_font('Arial','B', 6)
                            pdf.text(122,140,totaltime_str)
                            pdf.text(136,140,str(eval_json['TotalTime(Hrs)_sat'])+'Hrs')
                            pdf.text(113,145, active_str)
                            pdf.text(136,145,str(eval_json['ActiveScreenTime_sat'])+'Hrs')
                            pdf.text(123,150, idle_str)
                            pdf.text(136,150,str(eval_json['IdleTime_sat'])+'Hrs')
                            pdf.text(116,155, productive_str)
                            pdf.text(136,155,str(eval_json['ProductiveScreentime_sat'])+'Hrs')
                            pdf.text(113,160, unproductive_str)
                            pdf.text(136,160,str(eval_json['UnProductiveScreentime_sat'])+'Hrs')

                            pdf.set_font('Arial','U', 10)
                            pdf.text(60,135, 'Daily Average WorkTime')

                            pdf.set_font('Arial','B', 6)
                            pdf.text(71,140,totaltime_str)
                            pdf.text(85,140,str(eval_json['TotalTime(Hrs)_mean'])+'Hrs')
                            pdf.text(62,145, active_str)
                            pdf.text(85,145,str(eval_json['ActiveScreenTime_mean'])+'Hrs')
                            pdf.text(72,150, idle_str)
                            pdf.text(85,150,str(eval_json['IdleTime_mean'])+'Hrs')
                            pdf.text(65,155, productive_str)
                            pdf.text(85,155,str(eval_json['ProductiveScreentime_mean'])+'Hrs')
                            pdf.text(62,160, unproductive_str)
                            pdf.text(85,160,str(eval_json['UnProductiveScreentime_mean'])+'Hrs')

                            pdf.set_font('Arial','U', 10)
                            pdf.text(159,135, 'WorkTime on Weekend(Sun)')

                            pdf.set_font('Arial','B', 6)
                            pdf.text(170,140,totaltime_str)
                            pdf.text(184,140,str(eval_json['TotalTime(Hrs)_sun'])+'Hrs')
                            pdf.text(161,145, active_str)
                            pdf.text(184,145,str(eval_json['ActiveScreenTime_sun'])+'Hrs')
                            pdf.text(171,150, idle_str)
                            pdf.text(184,150,str(eval_json['IdleTime_sun'])+'Hrs')
                            pdf.text(164,155, productive_str)
                            pdf.text(184,155,str(eval_json['ProductiveScreentime_sun'])+'Hrs')
                            pdf.text(161,160, unproductive_str)
                            pdf.text(184,160,str(eval_json['UnProductiveScreentime_sun'])+'Hrs')
                        else:
                            pdf.set_font('Arial','U', 10)
                            pdf.text(42,135, 'Total WorkTime')

                            pdf.set_font('Arial','B', 6)
                            pdf.text(44,140,totaltime_str)
                            pdf.text(58,140,str(eval_json['TotalTime(Hrs)_sum'])+'Hrs')
                            pdf.text(35,145, active_str)
                            pdf.text(58,145,str(eval_json['ActiveScreenTime_sum'])+'Hrs')
                            pdf.text(45,150, idle_str)
                            pdf.text(58,150,str(eval_json['IdleTime_sum'])+'Hrs')
                            pdf.text(38,155, productive_str)
                            pdf.text(58,155,str(eval_json['ProductiveScreentime_sum'])+'Hrs')
                            pdf.text(35,160, unproductive_str)
                            pdf.text(58,160,str(eval_json['UnProductiveScreentime_sum'])+'Hrs')

                            pdf.set_font('Arial','U', 10)
                            pdf.text(80,135, 'Daily Average WorkTime')

                            pdf.set_font('Arial','B', 6)
                            pdf.text(91,140,totaltime_str)
                            pdf.text(105,140,str(eval_json['TotalTime(Hrs)_mean'])+'Hrs')
                            pdf.text(82,145, active_str)
                            pdf.text(105,145,str(eval_json['ActiveScreenTime_mean'])+'Hrs')
                            pdf.text(92,150, idle_str)
                            pdf.text(105,150,str(eval_json['IdleTime_mean'])+'Hrs')
                            pdf.text(85,155, productive_str)
                            pdf.text(105,155,str(eval_json['ProductiveScreentime_mean'])+'Hrs')
                            pdf.text(82,160, unproductive_str)
                            pdf.text(105,160,str(eval_json['UnProductiveScreentime_mean'])+'Hrs')

                            if Weekend=="Sunday":
                                Weekend='(Sun)'
                            else:
                                Weekend='(Sat)'

                            pdf.set_font('Arial','U', 10)
                            pdf.text(131,135, 'WorkTime on Weekend'+Weekend)

                            pdf.set_font('Arial','B', 6)
                            pdf.text(142,140,totaltime_str)
                            pdf.text(156,140,str(eval_json['TotalTime(Hrs)_wk'])+'Hrs')
                            pdf.text(133,145, active_str)
                            pdf.text(156,145,str(eval_json['ActiveScreenTime_wk'])+'Hrs')
                            pdf.text(143,150, idle_str)
                            pdf.text(156,150,str(eval_json['IdleTime_wk'])+'Hrs')
                            pdf.text(136,155, productive_str)
                            pdf.text(156,155,str(eval_json['ProductiveScreentime_wk'])+'Hrs')
                            pdf.text(133,160, unproductive_str)
                            pdf.text(156,160,str(eval_json['UnProductiveScreentime_wk'])+'Hrs')

                        pdf.add_page()
                        df_slice_apps=df_slice_app(client,idd,dbs,payload)
                        if type(df_slice_apps)==dict:
                            return df_slice_apps['error']
                        df_slice_webs=df_slice_web(client,idd,dbs,payload)
                        if type(df_slice_webs)==dict:
                            return df_slice_webs['error']

                        file_pro_app=productive_app(df_slice_apps)
                        file_unpro_app=unproductive_app(df_slice_apps)
                        file_pro_web=productive_web(df_slice_webs)
                        file_unpro_web=unproductive_web(df_slice_webs)
                        if remk !='none' and mac !='none':
                            remk=remk
                            mac=mac
                        elif mac!='none':
                            remk=" "
                        elif remk!='none':
                            mac=" "
                        else:
                            remk=" "
                            mac=" "

                        pdf.set_font('Arial', 'B', 10)
                        pdf.text(10, 20, name_header)
                        pdf.text(10, 15, str(group))
                        pdf.text(160, 15, str(mac))
                        pdf.set_font('Arial', 'B', 10)
                        pdf.text(86, 6, str(remk))

                        pdf.set_font('Arial', 'U', 6)
                        pdf.text(11,291,"S.No :- "+str(ser))

                        pdf.set_font('Arial', 'B', 8)
                        pdf.text(10, 25, str(location))

                        pdf.set_font('Arial', 'B', 10)
                        pdf.text(160,20, 'Monthly Worktime Profile')
                        date_range=str(fr_om)+' - '+str(till)+" ("+str(mon)+")"
                        pdf.set_font('Arial','B', 6)
                        pdf.text(161,24,date_range)

                        pdf.line(10, 27, 200, 27)
                        if file_pro_app==False:
                            pdf.set_font('Arial', 'B', 6)
                            pdf.text( 40, 55, "Data not available")
                        else:
                            pdf.set_font('Arial', 'B', 6)
                            pdf.text(28,50,'Productive Applications')
                            pdf.image(file_pro_app, 3, 55, WIDTH/2.2)

                        if file_unpro_app==False:
                            pdf.set_font('Arial', 'B', 6)
                            pdf.text( 140, 55, "Data not available")
                        else:
                            pdf.set_font('Arial', 'B', 6)
                            pdf.text(125,50,'UnProductive Applications')
                            pdf.image(file_unpro_app, 100, 55, WIDTH/2.2)

                        if file_pro_web==False:
                            pdf.set_font('Arial', 'B', 6)
                            pdf.text( 40, 150, "Data not available")
                        else:
                            pdf.set_font('Arial', 'B', 6)
                            pdf.text(25,145,'Productive Web Usage')
                            pdf.image(file_pro_web, 3, 150, WIDTH/2.2)

                        if file_unpro_web==False:
                            pdf.set_font('Arial', 'B', 6)
                            pdf.text( 140, 150, "Data not available")
                        else:
                            pdf.set_font('Arial', 'B', 6)
                            pdf.text(123,50,'UnProductive Web Usage')
                            pdf.image(file_unpro_web,100, 150, WIDTH/2)
                    ser+=1
        #mon=", ".join(eval_json['month'])
        now_date=datetime.now().date()
        date=now_date.strftime('%Y-%m-%d')
        now_time=datetime.now().time()
        time=now_time.strftime('%I-%M')
        #uuid_x=str(uuid.UUID())
        #filename=str(mon)+"_"+"Report"+"_"+str(date)+"_"+str(time)+".pdf"
        filename=str(mon)+"Report"+"_"+str(date)+"_"+str(time) +".pdf"
        file_path='/home/centos/iassist/bot/dlppdf/'+filename
        pdf.output(file_path, 'F')
        path={}
        path['filepath']=file_path
        return path
    else:
        err='Data not available where '+str(payload[2])+' is '+str(payload[3])
        return str(err)