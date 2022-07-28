from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, DateTime
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime

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


Base.metadata.create_all(engine)

cc_cookie = DeviceData(global_dpid=1,ts= datetime.now(),value=15.25)

session.add(cc_cookie)
session.commit()