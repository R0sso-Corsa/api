# Google Search API

import requests
import json

# Replace with your actual API key
API_KEY = "AIzaSyD3ucqysaQsJY7MIdG1cJ8ygF6tP6LXRzM"

# Replace with your search query
SEARCH_QUERY = "bitcoin"

# Optional:  If using a custom search engine, replace with your CSE ID
CSE_ID = "704686a19399c49b5"

# Build the API URL
url = f"https://www.googleapis.com/customsearch/v1?key={API_KEY}&q={SEARCH_QUERY}"

# If using a CSE ID, add it to the URL
if CSE_ID:
    url += f"&cx={CSE_ID}"

# Make the request
try:
    response = requests.get(url)
    response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
    data = response.json()
    print(json.dumps(data, indent=4))  # Print the results in a readable format

except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")

#   https://trinket.io/embed/python3/a5bd54189b