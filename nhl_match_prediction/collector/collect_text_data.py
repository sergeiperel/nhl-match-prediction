import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.nhl.com"
SEARCH_URL = "https://www.nhl.com/search/"

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data" / "text"
DATA_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0"}


def get_article_links(start_page=1, max_pages=10):
    links = set()

    for page in range(start_page, max_pages + 1):
        print(f"Parsing search page {page}...")
        params = {"query": "recap", "type": "type", "value": "story", "page": page}

        try:
            r = requests.get(SEARCH_URL, headers=HEADERS, params=params, timeout=20)
            r.raise_for_status()
        except requests.RequestException as e:
            print(f"Request error on page {page}: {e}")
            time.sleep(5)
            continue

        soup = BeautifulSoup(r.text, "html.parser")
        page_links = set()

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/news/" in href and "game-recap" in href:
                if href.startswith("/"):
                    href = BASE_URL + href
                page_links.add(href)

        if not page_links:
            print("No links found on this page. Stopping.")
            break

        new_links = page_links - links
        if not new_links:
            print("No new links found. Stopping.")
            break

        links.update(new_links)
        time.sleep(0.5)

    print(f"Total unique links found: {len(links)}")
    return list(links)


def parse_article(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except requests.RequestException as e:
        print("Request error:", e, url)
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    title_tag = soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else None

    date_tag = soup.find("time")
    date = date_tag.get_text(strip=True) if date_tag else None

    body = soup.find("div", {"data-module": "ArticleBody"})

    paragraphs = body.find_all("p") if body else soup.find_all("p")

    text = " ".join(p.get_text(strip=True) for p in paragraphs)

    # фильтр коротких статей
    if len(text) < 150:  # noqa: PLR2004
        return None

    return {"url": url, "title": title, "date": date, "text": text}


def main(start_page):
    # Существующие ссылки
    links_file = DATA_DIR / "recap_links.csv"

    existing_links = pd.read_csv(links_file)["0"].tolist() if links_file.exists() else []

    # Существующие статьи
    articles_file = DATA_DIR / "nhl_game_recaps.csv"

    existing_data = pd.read_csv(articles_file) if articles_file.exists() else pd.DataFrame()

    # 1. Собираем ссылки
    links = get_article_links(start_page=start_page, max_pages=1500)
    # убираем уже собранные
    links = [link for link in links if link not in existing_links]

    # сохраняем все ссылки вместе
    all_links = existing_links + links
    pd.Series(all_links).to_csv(DATA_DIR / "recap_links.csv", index=False)

    # 2. Парсим статьи
    data = []
    for _, link in enumerate(links, 1):
        article = parse_article(link)
        if article:
            data.append(article)

    new_data = pd.DataFrame(data)

    df = pd.concat([existing_data, new_data], ignore_index=True)
    df.to_csv(DATA_DIR / "nhl_game_recaps.csv", index=False)
    df.to_json(DATA_DIR / "nhl_game_recaps.json", orient="records", force_ascii=False)
    print(f"Done! Saved {len(df)} articles to {DATA_DIR}")


if __name__ == "__main__":
    main(start_page=0)
