import requests
import csv
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

BRING_POST = getenv('BRING_POST')

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

class County(Model):
  code = CharField(primary_key=True, null=False, unique=True)
  name = CharField(null=False)
  
  class Meta:
    database = db


class Municipality(Model):
  code = CharField(primary_key=True, null=False, unique=True) 
  county = ForeignKeyField(County, backref='municipalities')
  name = CharField(null=False)
  
  class Meta:
    database = db
   
class PostalAddress(Model):
  zip = CharField(primary_key=True, null=False, unique=True)
  name = CharField(null=False)
  municipality = ForeignKeyField(Municipality, backref='postaladdresses')
  
  class Meta:
    database = db

db.connect()
db.drop_tables([County, Municipality, PostalAddress])
db.create_tables([County, Municipality, PostalAddress])


counties = requests.get(SSB_REGION_URL).json().get(SSB_REGION_KEY)
municipalities = requests.get(SSB_CITY_URL).json().get(SSB_CITY_KEY)
postaladdresses = requests.get(BRING_POST)

for county in counties:
  with db.atomic() as transaction:
    try:
      new_county = County.create(code=county.get('code'), name=county.get('name'))
      transaction.commit()
    except DatabaseError as e:
      transaction.rollback()
      print(e)
      exit(1)
  
for muni in municipalities:
  with db.atomic() as transaction:
    try:
      county = muni.get('code')[:2]
      new_muni = Municipality.create(code=muni.get('code'), county=county, name=muni.get('name'))
      transaction.commit()
    except DatabaseError as e:
      transaction.rollback()
      print(e)
      exit(1)

with open('./postal.tsv', 'wb') as f:
  f.write(postaladdresses.content)
 
with open("postal.tsv", "r", encoding="ISO-8859-1") as zipcodes:
  tsv_reader = csv.reader(zipcodes, delimiter="\t")

  for row in tsv_reader:
    (zip, name, muni_id, county, cat) = row
    with db.atomic() as transaction:
      try:
        postal_address = PostalAddress.create(zip=zip, name=name, municipality=muni_id)      
        transaction.commit()
      except DatabaseError as e:
        transaction.rollback()
        print(e)
        exit(1)

      
print("Successfully created/updated tables.")