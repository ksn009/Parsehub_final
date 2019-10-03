import json
from flask import Flask, request, render_template
import requests
import csv
from datetime import datetime
from io import StringIO
from werkzeug.wrappers import Response
import mysql.connector
import time
mydb = mysql.connector.connect(
  host="localhost",
  user="admin",
  passwd="vlvdW0IWRkTDfkvpk3m6",
  database="flaskdb"
)
mycursor = mydb.cursor()
v=[]
now = datetime.now()
now1="project3_"+str(now)+".csv"
application = Flask(__name__)
@application.route('/')
def my_form():
    return render_template('index.html')
@application.route('/datareturn', methods=['POST'])
def datareturn():
    return render_template('my-form-api-lastrun.html')
@application.route('/rundata', methods=['POST'])
def rundata():
    return render_template('my-form-api.html')
@application.route('/run', methods=['POST'])
def run():
    api = request.form['api']
    api1=api.split()
    api_len=len(api1)
    pt = request.form['pt']
    pt1=pt.split()
    pt_len=len(pt1)
    link= request.form['link']
    link1=link.split()
    link_len=len(link1)
    processing_link=[]
    res=""
    link_temp=""
    j="\",\""
    un_processing_link_numb=0
    un_processing_link=[]
    links199=[]
    link_temp199=[]    
    temp_count = 0
    max_link = api_len*199                                                      # MAXIMUM LINKS 
    if api_len == pt_len:
        if link_len > max_link:
            ip_link=199
        else:
            ip_link = link_len/api_len
        temp_count_max = int(ip_link)    
        temp_count_max_1 = int(ip_link)
        if link_len > max_link:
            z=0
            for n in range(0,max_link):
                processing_link.append(link1[n])
                un_processing_link_numb=max_link-link_len
            for n in range(max_link-un_processing_link_numb,link_len):
                un_processing_link[z]=un_processing_link.append(link1[z])
                z=z+1
        else:
            for n in range(0,link_len):
                processing_link.append(link1[n])
        for ii in range(0,api_len):
            for i in range(temp_count,temp_count_max_1):
                link_temp199.append(processing_link[i])
            temp_count=i+1
            temp_count_max_1=temp_count_max_1+temp_count_max
            link_temp=j.join(link_temp199)
            links199.append("{\"url\":[\""+link_temp+"\"]}")
            params = {
                    "api_key": api1[ii],
                    "start_url": "https://www.amazon.in/","start_template": "main_template",
                    "start_value_override": links199[ii],
                    "send_email": "1"
                    }
            r = requests.post("https://www.parsehub.com/api/v2/projects/"+pt1[ii]+"/run", data=params)
            res = res +' <br> '+ r.text
            link_temp199=[]
            time.sleep(5)
        res="Successfully Completed 5 Projects"+'<br><br>'+ res
        for nn in range(0,un_processing_link_numb):
            res = res + '<br>' + un_processing_link[nn]
        res="Un Processed Links Due to Over Load <br>" + res
        return res
    
    else:
        er="Please provide same number of API_KEY and PROJECT_TOKEN"
        return er
@application.route('/dreturn', methods=['POST'])
def dreturn():
    api = request.form['api']
    api1=api.split()
    api_len=len(api1)

    pt = request.form['pt']
    pt1=pt.split()
    pt_len=len(pt1)

    if api_len == pt_len:
        for ii in range(0,api_len):
            params = {"api_key": api1[ii],"format": "json"}
            r = requests.get('https://www.parsehub.com/api/v2/runs/'+pt1[ii]+'/data', params=params)
        y = json.loads(r.text)
        total_product=len(y["list1"])
        for n in range(0,total_product):
            bulk = y["list1"][n]["bulk"]
            title = y["list1"][n]["title"]
            brand = y["list1"][n]["brand"]
            brand_url = y["list1"][n]["brand_url"]
            mp = (y["list1"][n]["price_note"][0]["price"])
            mp = mp.split()
            mp1 = mp[1].split(',')
            mp2 = "".join(mp1)
            mrp = float(mp2)
            
            pp = (y["list1"][n]["price_note"][1]["price"])
            pp = pp.split()
            pp1 = pp[1].split(',')
            pp2 = "".join(pp1)
            price = float(pp2)
            you_save= str(mrp-price)+' ('+ str(int(((mrp-price)/mrp)*100))+'%)'
            if len(y["list1"][n]) == 10:
                ss_brand = y["list1"][n]["selection2"][0]["product_information"]
                ss_colour = y["list1"][n]["selection2"][1]["product_information"]
                ss_imn = y["list1"][n]["selection2"][6]["product_information"]
                ss_ASIN = y["list1"][n]["selection2"][7]["product_information"]
                ss_cr = y["list1"][n]["selection2"][8]["product_information"]
                ss_bsr = y["list1"][n]["selection2"][9]["product_information"]
                ss_dfa = y["list1"][n]["selection2"][10]["product_information"]
            else:
                ss_brand = ""
                ss_colour = ""
                ss_imn = ""
                ss_ASIN = ""
                ss_cr = ""
                ss_bsr = ""
                ss_dfa = ""
            discription = y["list1"][n]["description"]
            rating_len=len(y["list1"][n]["rating_stars"])
            rating = ["","","","",""]
            for nn in range (0,rating_len):
                rating[nn] = str(y["list1"][n]["rating_stars"][nn]["Rating_ration"])
            img_count = str(len(y["list1"][n]["imagecount"]))
            img_url = y["list1"][n]["image"]
            v.append((bulk,title,brand,brand_url,mrp,price,you_save,ss_brand,ss_colour,ss_imn,ss_ASIN,ss_cr,ss_bsr,ss_dfa,discription,rating[0],rating[1],rating[2],rating[3],rating[4],img_count,img_url))
        def generate():
            data = StringIO()
            w = csv.writer(data)
            w.writerow(("Bulk","Title","Brand","Brand URL","MRP","Price","You Save","Sel Brand","Sel Colour",
           "Sel Item Model No","Sel ASIN","Sel Customer Rating","Sel BSR","Sel DFA","Description","5 Star",
           "4 Star","3 Star","2 Star","1 Star","No Img","Img URL"))
            yield data.getvalue()
            data.seek(0)
            data.truncate(0)
            for item in v:
                w.writerow((
                    item[0],item[1],item[2],item[3],item[4],item[5],item[6],item[7],item[8],
                    item[9],item[10],item[11],item[12],item[13],item[14],item[15],item[16],
                    item[17],item[18],item[19],item[20],item[21]
                    ))
                yield data.getvalue()
                data.seek(0)
                data.truncate(0)
        response = Response(generate(), mimetype='text/csv')
        sql = "INSERT INTO scraped_data (bulk,title,brand,brand_url,mrp,price,you_save,sel_brand,sel_colour,sel_imn,sel_ASIN,sel_cr,sel_bsr,sel_dfa,description,5_rr,4_rr,3_rr,2_rr,1_rr,imagecount,image) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        mycursor.executemany(sql, v)
        mydb.commit()
        response.headers.set("Content-Disposition", "attachment", filename=now1)
        return response
        
    else:
        er="Please provide same number of API_KEY and PROJECT_TOKEN"
        return er



if __name__ == '__main__':
    application.run()