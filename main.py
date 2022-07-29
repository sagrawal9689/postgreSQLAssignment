from ast import arg
from datetime import datetime
from telnetlib import TM
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, DateTime
from sqlalchemy.dialects.postgresql import UUID
import uuid
import psycopg2
from datetime import datetime
import random 

DB_NAME= "device"

engine = create_engine(f'postgresql://postgres:sahil@localhost/{DB_NAME}')

Session = sessionmaker(bind=engine)

session = Session()

Base = declarative_base()

class DeviceData(Base):
    __tablename__ = 'device_data'

    id = Column(Integer, primary_key=True, nullable=False)
    global_dpid = Column(Integer, nullable=False)
    ts = Column(DateTime, nullable=False)
    value = Column(Float, nullable=False)
    device_id = Column(UUID(as_uuid=True), default=uuid.uuid4 ,nullable=False)

    def __repr__(self):
        return "DeviceData(id='{self.id}', " \
                       "global_dpid='{self.global_dpid}', " \
                       "ts='{self.ts}', " \
                       "value={self.value}, " \
                       "device_id={self.device_id})".format(self=self)


Base.metadata.create_all(engine)

#connect to the db 
con = psycopg2.connect(
            host = "localhost",
            database="device",
            user = "postgres",
            password = "sahil")

#cursor 
cur = con.cursor()

device_data= []

uuidList= [uuid.uuid4() for i in range(100)]

def convertStringToTimeObject(givenTime):
    
    date_time_obj = datetime.strptime(givenTime,r'%m %d %Y')
    
    return date_time_obj

for i in range(1000):
    device_data.append((random.randint(1,1000),convertStringToTimeObject(f"{random.randint(1,12)} {random.randint(1,28)} {random.randint(2000,2023)}"),random.randint(1,100),uuidList[random.randint(0,99)]))

args_str = b','.join(cur.mogrify("(%s,%s,%s,%s)", x) for x in device_data)
args_str=args_str.decode("utf8")

cur.execute("INSERT INTO device_data (global_dpid,ts,value,device_id) VALUES " + args_str) 

# cur.executemany("INSERT INTO device_data (global_dpid,ts,value,device_id) VALUES(%s,%s,%s,%s)", tup)

#commit the transcation 
con.commit()

# querying data for a specific device_id using sqlalchemy

record = session.query(DeviceData).filter_by(device_id=uuidList[0]).all()

print("All the devices with the given id are : \n")

for r in record:
    print(r)    

# querying data within a specific time range for a specific device_id using psycopg2

tMin= datetime.strptime("2 17 2001",r'%m %d %Y')
tMax= datetime.strptime("2 17 2020",r'%m %d %Y')

result= cur.execute(f"SELECT global_dpid, ts , value FROM device_data WHERE device_id= '{uuidList[0]}' AND ts>= timestamp '{tMin}' AND ts<= timestamp '{tMax}'")

result= cur.fetchall()

# creating csv from the result data

import csv

with open(f'{uuidList[0]}.csv','w',newline='') as out:
    csv_out=csv.writer(out)
    csv_out.writerow(['global_dpid', 'ts' , 'value'])
    for row in result:
        csv_out.writerow(row)


#close the cursor
cur.close()

#close the connection
con.close()