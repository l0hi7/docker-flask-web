from crudforgres import Crudforgres
import psycopg2.extras
import pandas as pd
import uuid
import numpy as np
import regex as re
import json
import datetime



class NDMSDBMSUpdate:
    def __init__(self):
        psycopg2.extras.register_uuid()
        self.table_transactions = Crudforgres(user = 'admin',password = 'secret',host = '10.237.172.42',port = '5432',dbname = 'postgres',schema='ndms',table = "transactions",primarykey = 'designid')
        self.table_transactions.connect()
        self.table_customer = Crudforgres(user = 'admin',password = 'secret',host = '10.237.172.42',port = '5432',dbname = 'postgres',schema='ndms', table = 'customer',primarykey = 'customerid')
        self.table_customer.connect()
        self.table_region = Crudforgres(user = 'admin',password = 'secret',host = '10.237.172.42',port = '5432',dbname = 'postgres',schema='ndms', table = 'region',primarykey = 'regionid')
        self.table_region.connect()
        self.table_design = Crudforgres(user = 'admin',password = 'secret',host = '10.237.172.42',port = '5432',dbname = 'postgres',schema='ndms', table = 'design',primarykey = 'designid')
        self.table_design.connect()
        self.table_resources= Crudforgres(user = 'admin',password = 'secret',host = '10.237.172.42',port = '5432',dbname = 'postgres',schema='ndms', table = 'resources',primarykey = 'resourceid')
        self.table_resources.connect()
        self.table_l3 = Crudforgres(user = 'admin',password = 'secret',host = '10.237.172.42',port = '5432',dbname = 'postgres',schema='ndms', table = 'l3',primarykey = 'l3old')
        self.table_l3.connect()
        self.table_l2_5 = Crudforgres(user = 'admin',password = 'secret',host = '10.237.172.42',port = '5432',dbname = 'postgres',schema='ndms', table = 'l2_5',primarykey = 'l2_5new')
        self.table_l2_5.connect()
        self.multi_table = Crudforgres(user = 'admin',password = 'secret',host = '10.237.172.42',port = '5432',dbname = 'postgres',schema='ndms', table = 'mustangall_view',primarykey='resource_name')
        self.multi_table.connect()
        self.user_table = Crudforgres(user = 'admin',password = 'secret',host = '10.237.172.42',port = '5432',dbname = 'postgres',schema='auth', table = 'usertable',primarykey='email')
        self.user_table.connect()
    def start(self):
        # print("DOME")
                # self.table_transactions.delete_all()
        elem=pd.read_excel("C:\\Users\\ls1142\\Desktop\\VM Fibre Upgrade Program Internal CS Tracker v4 (4).xlsx",sheet_name="Region Progress", header=1)
        elem = elem.replace({np.nan: None})
        # print(elem.shape[0])
        # self.table_customer.insert(customerid=5892,customer="VMO2")
        # self.table_customer.commit()
        for i in range(300):
            inside = True
            while inside==True:
                id = str(uuid.uuid4())
                # print(id)
                check=self.table_transactions.select(columns=["designid"],primaryKey_value=id)
                if check is None or len(check)==0:
                    inside=False
                    hpcount=elem["HP Count"]
                    if hpcount is not  None:
                        self.table_transactions.insert_many(columns = ('designid','hpcount', 'revisedhpcount','total_forcastdate','designstartdate','surveydesigner','surveypackready','surveystartdate','surveyresourcename1','surveyresourcename2','survey_completedate','civilpackstatus', 'l2_l3forcastdate','l2_l3completedate','l4requiredcabinets','no_l4completedcabinets','msdr_status','fiber_status','msdranelemiber_status','swims_status','lsbud_status','bt_status','digdat_status','l3_l4forcastdate','l3_l4completedate','qc_issueddate','qc_designername','qc_completeddate','fulcrum_po_raised','sent_to_customerdate','customer_approver','final_approveddate'),
                                                            rows = [[id,elem["HP Count"],elem["Revise HP Count"],elem["Forecast Release Date L2.5 - L4"],elem["L2.5, L3-L4 Design Start Date:"],elem["Survey Pack Designer1"],elem["Survey Pack Designer2"],elem["Survey Start Date "],elem["Survey Resource #1"],elem["Survey Resource #2"],elem["Survey Complete Date"],elem["L2.5/L3 Civils Pack Status"],elem["L2.5 - L3 Forecast Date"],elem["L2.5 - L3 Complete Date"],elem["Required Number of L4 Cabinets"],elem["Number of L4 Cabinets Design Complete"],elem["MSDR SLD"],elem["Fiber SLD"],elem["MSDR/Fiber Pack "],elem["Swims"],elem["LSBUD"],elem["BT"],elem["DIGDAT"],elem["L3 - L4 Forecast Date"],elem["L3 -L4 Complete Date"],elem["Issued for QC"],elem["UK QC Designer"],elem["Onshore QC Completed "],elem["Fulcrum PO Raised"],elem[" Released to VM02"],elem["Final Approval VM02 By"],elem["Final Approval (Acceptance  Date) "]]])
                        self.table_transactions.commit()
                        resource_check=self.table_resources.select(columns=["resourceid"],primaryKey_value=elem["L2.5 - L3 IND Designer"])
                        if resource_check is None or len(resource_check)==0:
                            self.table_resources.insert(resourceid=elem["L2.5 - L3 IND Designer"])
                            self.table_resources.commit()
                        l2_present=elem["VM data L3 Node Name"]
                        if l2_present is None:
                            self.table_l2_5.insert_many(columns = ('l2_5old','l2_5new','customerid'), rows=[[elem["L2.5 "],elem["New L2.5 Reference Name"],5892]])
                            self.table_l2_5.commit()
                            self.table_design.insert_many(columns = ('designid','l2_5new','l3old','resourceid'), rows=[[id,elem["New L2.5 Reference Name"],elem["VM data L3 Node Name"],elem["L2.5 - L3 IND Designer"]]])   
                        else:
                            newl2_5=elem["New L2.5 / L3"]
                            newl_5=re.findall("[A-Za-z]{4}-[A-Za-z]{1}-[A-Za-z0-9]{2}",newl2_5)
                            self.table_l3.insert_many(columns = ('l3old','l2_5new','l3new'), rows=[[elem["VM data L3 Node Name"],newl_5[0],elem["New L2.5 / L3"]]])
                            self.table_l3.commit()
                            self.table_design.insert_many(columns = ('designid','l2_5new','l3old','resourceid'), rows=[[id,newl_5[0],elem["VM data L3 Node Name"],elem["L2.5 - L3 IND Designer"]]])
        self.table_transactions.close(True)
        self.table_customer.close(True)
        self.table_design.close(True)
        self.table_l2_5.close(True)
        self.table_l3.close(True)
        self.table_region.close(True)
        self.table_resources.close(True)

    
    
    def createnew(self,content=None):
        data = json.load(content)
        for elem in data['data']:
            inside = True
            while inside==True:
                id = str(uuid.uuid4())
                print(id)
                check=self.table_transactions.select(columns=["designid"],primaryKey_value=id)
                if check is None or len(check)==0:
                    inside=False
                    hpcount=elem["hpcount"]
                    if hpcount is not  None:
                        self.table_transactions.insert_many(columns = ('designid','hpcount', 'revisedhpcount','total_forcastdate','designstartdate','surveydesigner1','surveydesigner2','surveystartdate','surveyresourcename1','surveyresourcename2','survey_completedate','civilpackstatus', 'l2_l3forcastdate','l2_l3completedate','l4requiredcabinets','no_l4completedcabinets','msdr_status','fiber_status','msdranelemiber_status','swims_status','lsbud_status','bt_status','digdat_status','l3_l4forcastdate','l3_l4completedate','qc_issueddate','qc_designername','qc_completeddate','fulcrum_po_raised','sent_to_customerdate','customer_approver','final_approveddate'),
                                                            rows = [[id,elem["hpcount"],elem["revisedhpcount"],elem["total_forcastdate"],elem["L2.5, L3-L4 Design Start Date:"],elem["Survey Pack Designer1"],elem["Survey Pack Designer2"],elem["Survey Start Date "],elem["Survey Resource #1"],elem["Survey Resource #2"],elem["Survey Complete Date"],elem["L2.5/L3 Civils Pack Status"],elem["L2.5 - L3 Forecast Date"],elem["L2.5 - L3 Complete Date"],elem["Required Number of L4 Cabinets"],elem["Number of L4 Cabinets Design Complete"],elem["MSDR SLD"],elem["Fiber SLD"],elem["MSDR/Fiber Pack "],elem["Swims"],elem["LSBUD"],elem["BT"],elem["DIGDAT"],elem["L3 - L4 Forecast Date"],elem["L3 -L4 Complete Date"],elem["Issued for QC"],elem["UK QC Designer"],elem["Onshore QC Completed "],elem["Fulcrum PO Raised"],elem[" Released to VM02"],elem["Final Approval VM02 By"],elem["Final Approval (Acceptance  Date) "]]])
                        self.table_transactions.commit()
                        resource_check=self.table_resources.select(columns=["resourceid"],primaryKey_value=elem["resourceid"])
                        if resource_check is None or len(resource_check)==0:
                            self.table_resources.insert(resourceid=elem["resourceid"])
                            self.table_resources.commit()
                        l2_present=elem["l3old"]
                        if l2_present is None:
                            self.table_l2_5.insert_many(columns = ('l2_5old','l2_5new','customerid'), rows=[[elem["l2_5old"],elem["l2_5new"],5892]])
                            self.table_l2_5.commit()
                            self.table_design.insert_many(columns = ('designid','l2_5new','l3old','resourceid'), rows=[[id,elem["l2_5new"],elem["l3old"],elem["resourceid"]]])   
                        else:
                            self.table_l3.insert_many(columns = ('l3old','l2_5new','l3new'), rows=[[elem["l3old"],elem["l2_5new"],elem["l3new"]]])
                            self.table_l3.commit()
                            self.table_design.insert_many(columns = ('designid','l2_5new','l3old','resourceid'), rows=[[id,elem["l2_5new"],elem["l3old"],elem["resourceid"]]])
        self.table_transactions.close(True)
        self.table_customer.close(True)
        self.table_design.close(True)
        self.table_l2_5.close(True)
        self.table_l3.close(True)
        self.table_region.close(True)
        self.table_resources.close(True)

    def updateexisting(self,content=None):
        data = json.load(content)
        for elem in data['data']:
            inside = True
            while inside==True:
                id = str(uuid.uuid4())
                print(id)
                check=self.table_transactions.select(columns=["designid"],primaryKey_value=id)
                if check is None or len(check)==0:
                    inside=False
                    hpcount=elem["hpcount"]
                    if hpcount is not  None:
                        self.table_transactions.insert_many(columns = ('designid','hpcount', 'revisedhpcount','total_forcastdate','designstartdate','surveydesigner1','surveydesigner2','surveystartdate','surveyresourcename1','surveyresourcename2','survey_completedate','civilpackstatus', 'l2_l3forcastdate','l2_l3completedate','l4requiredcabinets','no_l4completedcabinets','msdr_status','fiber_status','msdranelemiber_status','swims_status','lsbud_status','bt_status','digdat_status','l3_l4forcastdate','l3_l4completedate','qc_issueddate','qc_designername','qc_completeddate','fulcrum_po_raised','sent_to_customerdate','customer_approver','final_approveddate'),
                                                            rows = [[id,elem["hpcount"],elem["revisedhpcount"],elem["total_forcastdate"],elem["L2.5, L3-L4 Design Start Date:"],elem["Survey Pack Designer1"],elem["Survey Pack Designer2"],elem["Survey Start Date "],elem["Survey Resource #1"],elem["Survey Resource #2"],elem["Survey Complete Date"],elem["L2.5/L3 Civils Pack Status"],elem["L2.5 - L3 Forecast Date"],elem["L2.5 - L3 Complete Date"],elem["Required Number of L4 Cabinets"],elem["Number of L4 Cabinets Design Complete"],elem["MSDR SLD"],elem["Fiber SLD"],elem["MSDR/Fiber Pack "],elem["Swims"],elem["LSBUD"],elem["BT"],elem["DIGDAT"],elem["L3 - L4 Forecast Date"],elem["L3 -L4 Complete Date"],elem["Issued for QC"],elem["UK QC Designer"],elem["Onshore QC Completed "],elem["Fulcrum PO Raised"],elem[" Released to VM02"],elem["Final Approval VM02 By"],elem["Final Approval (Acceptance  Date) "]]])
                        self.table_transactions.commit()
                        resource_check=self.table_resources.select(columns=["resourceid"],primaryKey_value=elem["resourceid"])
                        if resource_check is None or len(resource_check)==0:
                            self.table_resources.insert(resourceid=elem["resourceid"])
                            self.table_resources.commit()
                        l2_present=elem["l3old"]
                        if l2_present is None:
                            self.table_l2_5.insert_many(columns = ('l2_5old','l2_5new','customerid'), rows=[[elem["l2_5old"],elem["l2_5new"],5892]])
                            self.table_l2_5.commit()
                            self.table_design.insert_many(columns = ('designid','l2_5new','l3old','resourceid'), rows=[[id,elem["l2_5new"],elem["l3old"],elem["resourceid"]]])   
                        else:
                            self.table_l3.insert_many(columns = ('l3old','l2_5new','l3new'), rows=[[elem["l3old"],elem["l2_5new"],elem["l3new"]]])
                            self.table_l3.commit()
                            self.table_design.insert_many(columns = ('designid','l2_5new','l3old','resourceid'), rows=[[id,elem["l2_5new"],elem["l3old"],elem["resourceid"]]])
        self.table_transactions.close(True)
        self.table_customer.close(True)
        self.table_design.close(True)
        self.table_l2_5.close(True)
        self.table_l3.close(True)
        self.table_region.close(True)
        self.table_resources.close(True)
    
    def getall(self):
        key_list=["DT_RowId","nl2-5","nl2-l3","l3new","status","l3l4completedate","region","area","hpcount","rhpcount","designstartdate","msdrsld","fibersld","msdr-fiberpack","swims","lsbud",
                  "bt","digdat","forecastreleasedate","l2l3civilspackstatus","l2l3forecastdate","l2l3completedate","reqnoofl4cabinates","noofl4cabinatesdesigncomplete","l3l4forecastdate","fulcrumporaised",
                  "releasedtovm02","finalapprovedvm02by","finalapproval","surveystartdate","surveyresource1","surveyresource2","surveycompletedate","ukqcdesigner","issuedforqc",
                  "onshoreqccompleted","surveypackdesigner","surveypackready","resourceid","l3-nodename","l2-5","l2l3inddesigner","x-coord","y-coord","l3l4inddesigner"]
        l3_list=[]
        l2_list=[]
        diction={}
        all_data=self.multi_table.select_all()
        for data in all_data:
            if data[0] not in l2_list:
                l2_list.append(data[0])
            # for indata in data:

        # print(l2_list)
        # print(len(l2_list))
        for node in l2_list:
            ramp=1
            for data in all_data:
                if data[0] == node:
                    l3_val_list=[]
                    count=0
                    for val in data:
                        if count==0:
                            Noneval= val
                            l3_val_list.append(ramp)
                            ramp+=1
                        elif count==1:
                            if val is None or val==None:
                                l3_val_list.append(str(Noneval))
                            else:
                                l3_val_list.append(str(val))
                        elif count ==2:
                            if isinstance(val, datetime.date):
                                valu="Completed"
                                l3_val_list.append(valu)
                            else:
                                valu="WIP"
                                l3_val_list.append(valu)
                        elif isinstance(val, datetime.date):
                                # print(val)
                                val="{}/{}/{}".format(val.month,val.day,val.year)
                                # print(val)
                        else:
                            pass
                        l3_val_list.append(str(val))
                        count+=1
                    l3_val_list.append("")
                    l3_val_list.append("")
                    l3_val_list.append("")
                    datas = {Key:Value for Key,Value in zip(key_list,l3_val_list)}
                    l3_list.append(datas)
            diction["data"]=l3_list
            save_file = open("savedata.json", "w")  
            json.dump(diction, save_file, indent = 4)  
            save_file.close()

        
        return diction
    
    
    def getusertable(self,userfirstname):
        key_list=["DT_RowId","nl2-5","nl2-l3","l3new","status","l3l4completedate","region","area","hpcount","rhpcount","designstartdate","msdrsld","fibersld","msdr-fiberpack","swims","lsbud",
                  "bt","digdat","forecastreleasedate","l2l3civilspackstatus","l2l3forecastdate","l2l3completedate","reqnoofl4cabinates","noofl4cabinatesdesigncomplete","l3l4forecastdate","fulcrumporaised",
                  "releasedtovm02","finalapprovedvm02by","finalapproval","surveystartdate","surveyresource1","surveyresource2","surveycompletedate","ukqcdesigner","issuedforqc",
                  "onshoreqccompleted","surveypackdesigner","surveypackready","resourceid","l3-nodename","l2-5","l2l3inddesigner","x-coord","y-coord","l3l4inddesigner"]
        l3_list=[]
        l2_list=[]
        diction={}
        all_data=self.multi_table.select_all("Akshay DV")
        for data in all_data:
            if data[0] not in l2_list:
                l2_list.append(data[0])
            # for indata in data:

        # print(l2_list)
        # print(len(l2_list))
        for node in l2_list:
            ramp=1
            for data in all_data:
                if data[0] == node:
                    l3_val_list=[]
                    count=0
                    for val in data:
                        if count==0:
                            l3_val_list.append(ramp)
                            ramp+=1
                        if count==1:
                            l3_val_list.append(str(val))
                        if count ==2:
                            if isinstance(val, datetime.date):
                                valu="Completed"
                                l3_val_list.append(valu)
                            else:
                                valu="WIP"
                                l3_val_list.append(valu)
                        
                        if isinstance(val, datetime.date):
                            # print(val)
                            val="{}/{}/{}".format(val.month,val.day,val.year)
                            # print(val)
                        l3_val_list.append(str(val))
                        count+=1
                    l3_val_list.append("")
                    l3_val_list.append("")
                    l3_val_list.append("")
                    datas = {Key:Value for Key,Value in zip(key_list,l3_val_list)}
                    l3_list.append(datas)
            diction["data"]=l3_list
            save_file = open("savedata.json", "w")  
            json.dump(diction, save_file, indent = 4)  
            save_file.close()

        
        return diction



    def userauthtable(self,email):
        key_list=["id","firstname","lastname","email","passwords","permissions","project type"]
        all_data=self.user_table.select(columns=["id","firstname","lastname","email","passwords","groupname","permissions"],primaryKey_value=email)
        # print(all_data)
        count=0
        userdict={}
        detaillist=[]
        projlist=[]
        for data in all_data:
            if count==0:
                detaillist.extend([data[0],data[1],data[2],data[3],data[4],data[6]])
                count=1
            projlist.append(data[5])
        print(projlist)
        print(detaillist)
        self.user_table.close()
        detaillist.append(projlist)
        datas = {Key:Value for Key,Value in zip(key_list,detaillist)}
        userdict["data"]=datas
        return userdict


# df=NDMSDBMSUpdate().getusertable("Akshay DV")
# print(df)