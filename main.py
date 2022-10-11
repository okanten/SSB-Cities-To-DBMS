import requests
from peewee import *
from dotenv import load_dotenv
from os import getenv

answer = input("Only an idiot would run this on a production database. Please type 'I am not an idiot' to continue:\n")

if answer != 'I am not an idiot':
  print("Idiot")
  exit(1)

load_dotenv()

SSB_REGION_URL = getenv('SSB_REGION_URL')
SSB_REGION_KEY = getenv('SSB_REGION_KEY')

SSB_CITY_URL = getenv('SSB_CITY_URL')
SSB_CITY_KEY = getenv('SSB_CITY_KEY')

DB_TYPE = getenv('DB_TYPE')
DB_NAME = getenv('DB_NAME')

db = None

if DB_TYPE == 'mysql':
  DB_HOST = getenv('DB_HOST')
  DB_PORT = int(getenv('DB_PORT'))
  DB_USERNAME = getenv('DB_USERNAME')
  DB_PASS = getenv('DB_PASS')
  db = MySQLDatabase(DB_NAME, user=DB_USERNAME, password=DB_PASS, host=DB_HOST, port=DB_PORT)
 
if DB_TYPE == 'sqlite3':
  db = SqliteDatabase(f"{DB_NAME}.db")

if db is None:
  print("Incorrect DB_TYPE in .env. Exiting...")
  exit(1)

class Region(Model):
  code = CharField(primary_key=True, null=False, unique=True)
  name = CharField(null=False)
  
  class Meta:
    database = db


class City(Model):
  code = CharField(primary_key=True, null=False, unique=True) 
  region = ForeignKeyField(Region, backref='cities')
  name = CharField(null=False)
  
  class Meta:
    database = db
   

db.connect()
db.drop_tables([Region, City])
db.create_tables([Region, City])


regions = requests.get(SSB_REGION_URL).json().get(SSB_REGION_KEY)
cities = requests.get(SSB_CITY_URL).json().get(SSB_CITY_KEY)

for region in regions:
  with db.atomic() as transaction:
    try:
      new_region = Region.create(code=region.get('code'), name=region.get('name'))
      transaction.commit()
    except DatabaseError as e:
      transaction.rollback()
      print(e)
      exit(1)
  
for city in cities:
  with db.atomic() as transaction:
    try:
      region = city.get('code')[:2]
      new_city = City.create(code=city.get('code'), region=region, name=city.get('name'))
      transaction.commit()
    except DatabaseError as e:
      transaction.rollback()
      print(e)
      exit(1)

      
print("Successfully created/updated tables.")