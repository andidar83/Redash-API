# -*- coding: utf-8 -*-
"""
Created on Mon Apr 29 15:53:59 2024

@author: NXP
"""

import requests

def archive_query(redash_url, query_id, api_key):
    s = requests.Session()
    s.headers.update({'Authorization': 'Key {}'.format(api_key)})
    response = s.delete('{}/api/queries/{}'.format(redash_url, query_id))
    if response.status_code != 200:
        return 'Archive failed'
    else:
        return'Query ',i," archived..."

list_archive = [2319]
for i in list_archive:
    print(archive_query('your redash domain',i,'your api'))
