from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

DATA_DIR = Path("data/text")
DATA_DIR.mkdir(parents=True, exist_ok=True)

SITEMAP_URL = "https://www.nhl.com/sitemap.xml"

r = requests.get(SITEMAP_URL, timeout=20)
soup = BeautifulSoup(r.text, "xml")

urls = []

for loc in soup.find_all("loc"):
    link = loc.text

    if "game-recap" in link:
        urls.append(link)

print("Found recap URLs:", len(urls))

df = pd.DataFrame({"url": urls})
df.to_csv(DATA_DIR / "recap_links.csv", index=False)

print("Done!")
