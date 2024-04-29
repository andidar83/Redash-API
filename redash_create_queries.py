# -*- coding: utf-8 -*-
"""
Created on Mon Apr 29 15:14:13 2024

@author: NXP
"""

import requests
import json


payload = {
           "query":'select * from orders limit 5', ## the select query
           "name":"test api new query",
           "data_source_id":1, ## can be determined from the /api/data_sources end point
           "schedule":None,
           "options":{"parameters":[]}
           }
res = requests.post('https://redash-id.ninjavan.co/' + '/api/queries', 
                    headers = {'Authorization':'Poqwen8BjW4zjLHOzpmSlXDnm7TSrV5DzevDv6eD'},
                    json=payload)

print(res)  