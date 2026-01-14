"""Skrypt do pobrania szczegółów rekordu Lead."""
import os
import json
import urllib.request
from refresh_zoho_access_token import refresh_access_token

client_id = os.environ.get('ZOHO_MEDIDESK_CLIENT_ID')
client_secret = os.environ.get('ZOHO_MEDIDESK_CLIENT_SECRET')
refresh_token = os.environ.get('ZOHO_MEDIDESK_REFRESH_TOKEN')

token_info = refresh_access_token(client_id, client_secret, refresh_token)
access_token = token_info['access_token']

lead_id = '751364000045329237'
url = f'https://www.zohoapis.eu/crm/v8/Leads/{lead_id}'
headers = {'Authorization': f'Zoho-oauthtoken {access_token}'}
req = urllib.request.Request(url, headers=headers)

with urllib.request.urlopen(req, timeout=30) as resp:
    data = json.loads(resp.read().decode('utf-8'))

lead = data['data'][0]
print('='*70)
print(f'LEAD ID: {lead_id}')
print('='*70)
for key, value in sorted(lead.items()):
    if value and value not in [None, '', [], {}]:
        print(f'{key}: {value}')
