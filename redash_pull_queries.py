# -*- coding: utf-8 -*-
"""
Created on Sat Jul 15 08:59:49 2023

@author: NXP
"""

from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import ssl
import smtplib
import sys
import pandas as pd
import os
import requests
import time
from pprint import pprint
import json
import gspread as gs
import gspread_dataframe as gd
from pretty_html_table import build_table
from redash_toolbelt.client import Redash
credentials ={
  "type": "service_account",
  "project_id": "project-01-322003",
  "private_key_id": "231cd74240848f5b9bfc5b781c51ec109d9edbd5",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEuwIBADANBgkqhkiG9w0BAQEFAASCBKUwggShAgEAAoIBAQCQ7amh4vUO2J1h\ntTFefuhalmw2oQFfrMB+zHAEOS0ddV1/npq4kFHqrjw34aaRe61nQ+I7wUfuUEUu\ncouf+ll76UfIoJKyXD5UcI7lGYPJWAjGaPABg85kEGCpTrf8YS4afdNI13Rtwi3o\nsbIFt37S6AGY9AALcLaS5TA9WY30M6EDaUnGhCEHWPW+BLPkeRHspUUcUK2+U/zA\nUVT0klbOQ2NxqGbkyBjzqdz/GD4bDPmJiQ9Rs0m86OumI5EUtsj7m7509Qj6PZ4w\nZR/gd+xMiocpxYl2Q1D5W3e+6k3AZ/Uy4Av+zs0XW+2VfsZtwm+8A8NLb/La0c6C\nijdTGYplAgMBAAECgf8/udrDfu655UFwzsyIpu+VbhevqXGCQmtREbXrWLBCCAyU\nxFN3S7oOsuO2nZu6B31AfAfktf6vS3pTPuidq0ytnogcljpRVT41JycDYsXLukfd\nOr60Ujghtqt01/2XN6yOCpJ/KD3fFlhdvYCSaVjJuY/HD4M5yNX2WyX0oMoXSWp3\nUDRB4YBdLTuVFJctGBc6pLSZIrGk8f6zblkOE6OfYUQ3m0xX83bfjfhHRNUtE6ql\nYYmjUxB4UDr0MYi/W05U/y60BVH0dFsPYRqeVMWkKrl3VMyW0Q9lv19+I0YCitq5\nnetuNZAeW3WeGM1nmkPZVM3Y0wViD7AnI/tHTaECgYEAw5yFXToXl5bYhMCEc4FZ\naMV9qRdQvbc3I6UphEkVTbdeG+LCQokI0aap58CCmdBxqFQe1jzckCKnGlM0bhrr\n72Dewd+ZtRLm9cUzeT6+DeiFBU6w4pzH6wbIZyNW6QLPWyE6MzsmjWTlCCQrerih\nfnGFzaiiQoSSA8b+MYR28/ECgYEAvauUsp98UpraDXkcs/CdRbekFOmxHckDcHaY\nbdlygv0T7PmyQUc+PLVXy8dnX8+ztLDyowQr7T1eUgrwc9gnqu7gqKpFq3CuuK8c\n9iiBt6Pp/c0BZl4ISXznlLkxEq9O7D3om1QnxVt3x7UyMtbZ+vPtzkQOWV6g0ImZ\n0SddIbUCgYAgT1Uz/elxr4fZ/ZajIYVsKdrEuEYs3/tqlthRsmSjbptLzdu6c7oS\nLw43anPoBicP370sM+dWo3KohX/OhAHei0igC1fvMc0WYVlMOJHZ6EM4ijPramwX\nJQqrBopPeJhZfBaJ6cZHapfuDVNlNOPv575rJuSOSil4GEFaWn/vcQKBgAKGDexy\ngsz2koArZF5gjTcoQl6k3V5wFkOg0FZl1kg35dP5Xo3PzNwx7YEgnheAQ/vbft78\nZzk439BnpNtlvOFhXEviOBsX3LtIWTJNR6yR2cMuvx4FkkaoCszHygddncc266ll\nbUIK/YUCvDBA+5WDOV7NOQOnZqxfqFRIGc0hAoGBAIe44ZpXPPlyKhtsY/Yn2rV+\nawutAkct4gFUBV+CGcAsRvvbR4iCpoW83M9bLYepj/TvZyUAvDS2JpV1t+lDMjpn\nN5WZb48bZGphDmFiYcrYgURduqR8j3PhSUXGyCEruaTVpbFDkvxS0X2mysBNJ94q\n4xHuOkBDG9EP7V7UDjPN\n-----END PRIVATE KEY-----\n",
  "client_email": "g-sheets@project-01-322003.iam.gserviceaccount.com",
  "client_id": "111359721706864293131",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/g-sheets%40project-01-322003.iam.gserviceaccount.com"
}
gc = gs.service_account_from_dict(credentials)

  
def poll_job(s, redash_url, job):
    # TODO: add timeout
    while job['status'] not in (3,4):
        response = s.get('{}/api/jobs/{}'.format(redash_url, job['id']))
        job = response.json()['job']
        time.sleep(1)

    if job['status'] == 3:
        return job['query_result_id']
    
    return None


def get_fresh_query_result(redash_url, query_id, api_key, params):
    s = requests.Session()
    s.headers.update({'Authorization': 'Key {}'.format(api_key)})

    payload = dict(max_age=0, parameters=params)

    response = s.post('{}/api/queries/{}/results'.format(redash_url, query_id), data=json.dumps(payload))

    if response.status_code != 200:
        return 'Refresh failed'
        raise Exception('Refresh failed.')

    result_id = poll_job(s, redash_url, response.json()['job'])

    if result_id:
        response = s.get('{}/api/queries/{}/results/{}.json'.format(redash_url, query_id, result_id))
        if response.status_code != 200:
            raise Exception('Failed getting results.')
    else:
        raise Exception('Query execution failed.')

    return response.json()['query_result']['data']['rows']




redash = Redash("https://redash-id.ninjavan.co", "Poqwen8BjW4zjLHOzpmSlXDnm7TSrV5DzevDv6eD")
queries = redash.paginate(redash.queries)
queries = pd.DataFrame(queries)
print(queries)
queries.to_excel("REDASH_ALL_QUERIES_19_04_24.xlsx")