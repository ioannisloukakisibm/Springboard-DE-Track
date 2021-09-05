import requests
import json
import docx


url = 'https://app.ticketmaster.com/discovery/v2/events.json?postalCode=27701&countryCode=US&apikey=Lqseo1SMwgdKqLEKOPTLlfJaLSCMtd0S'
response = requests.get(url)
print(response)
data=response.text

parsed = json.loads(data)

# print(json.dumps(parsed, indent=4))
check=json.dumps(parsed, indent=4) 


slice = check[0]

print(slice)

# mydoc = docx.Document()
# mydoc.add_paragraph(check)
# mydoc.save(r'C:\Users\IoannisLoukakis\desktop\check_html_ticketmaster_indented.docx')

# dpac venueId = KovZpa2X8e
# carolina theatre = KovZpZAFAl6A
# https://developer.ticketmaster.com/products-and-docs/apis/getting-started/