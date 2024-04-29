import requests
import json


payload = {
           "query":'select * from xxx', ## the select query
           "name":"test api new query",
           "data_source_id":1, ## can be determined from the /api/data_sources end point
           "schedule":None,
           "options":{"parameters":[]}
           }
res = requests.post('your redash domain' + '/api/queries', 
                    headers = {'Authorization':'your api'},
                    json=payload)

print(res)  
