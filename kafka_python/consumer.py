from confluent_kafka import Consumer, KafkaError
import json
from sqlalchemy import create_engine, Column, Integer, String, DateTime
# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()

class UserActivity(Base):
    tablename = 'user_activity_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_type = Column(String)
    username = Column(String)
    timestamp = Column(DateTime, default=datetime.now)

def store_user_activity(data, session):
    try:
        user_activity = UserActivity(event_type=data['event_type'], username=data['username'],timestamp=data["timestamp"])
        print(type(user_activity))
        session.add(user_activity)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error storing user activity: {e}")

def kafka_consumer():
    # Connect to MySQL database using SQLAlchemy
    #engine = create_engine('mysql+pymysql://root:root@localhost/user_activity')
    engine = create_engine('mysql+pymysql://root@localhost/user_activity')

    Session = sessionmaker(bind=engine)
    session = Session()

    conf = {
        'bootstrap.servers': "localhost:9092",
        'group.id': "user_activity_group",
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe(['user_activity'])  # Subscribe to the 'user_activity' topic

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition
                    continue
                else:
                    print(msg.error())
                    break
            
            # Process message
            data = json.loads(msg.value().decode('utf-8'))
            store_user_activity(data, session)

    except KeyboardInterrupt:
        pass

    finally:
        session.close()
        consumer.close()

if name == "main":
    kafka_consumer()