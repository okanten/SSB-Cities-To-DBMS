create view zip_codes as
select 
	postaladdress.zip,
	postaladdress.name,
  municipality.name as Municipality,
  county.name as County
from postaladdress
join municipality
	on postaladdress.municipality_id = municipality.code
join county
	on municipality.county_id = county.code;