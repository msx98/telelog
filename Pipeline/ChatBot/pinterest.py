import requests
import bs4
import json
from typing import Tuple, Optional
import os


def extract_image(url) -> Optional[Tuple[str, str]]:
    response = requests.get(url)
    soup = bs4.BeautifulSoup(response.text, "html.parser")
    script_tags = soup.find_all('script', attrs={'data-relay-response': 'true', 'type': 'application/json'})
    results = []
    for script_tag in script_tags:
        d = json.loads(script_tag.text)
        try:
            url = d["response"]["data"]["v3GetPinQuery"]["data"]["imageSpec_orig"]["url"]
            title = None#d["response"]["data"]["v3GetPinQuery"]["data"]["gridTitle"]
            results.append((url, title))
        except Exception:
            continue
    if results:
        return results[0]


def download_image(download_dir: str, url: str, title: str) -> str:
    response = requests.get(url)
    extension = url.split(".")[-1]
    if extension not in ["jpg", "jpeg", "png", "jpeg", "gif", "webp", "bmp", "tiff", "svg", "ico"]:
        extension = "jpg"
    filepath = f"{download_dir}/{title}.{extension}"
    os.makedirs(download_dir, exist_ok=True)
    with open(filepath, "wb") as f:
        f.write(response.content)
    return filepath
