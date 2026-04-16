import requests
from bs4 import BeautifulSoup

url= "https://realpython.github.io/fake-jobs/"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

response = requests.get(url,headers=headers)
soup= BeautifulSoup(response.content, "html.parser")
print(f"Soup title : {soup.title.text}")