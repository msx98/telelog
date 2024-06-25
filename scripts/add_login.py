#!/usr/bin/env python3

from typing import Optional, Dict
import os
import bs4
import re
import json
import requests
import pyrogram
import asyncio


TELEGRAM_ENV_JSON_PATH = ".telegram.env.json"
TELEGRAM_ENV_PATH = ".telegram.env"
assert os.path.exists(TELEGRAM_ENV_PATH), f"Telegram env file does not exist at {TELEGRAM_ENV_PATH}"


async def get_session_string(*, api_id: str, api_hash: str, phone_number: str, two_factor_pass: str) -> str:
    try:
        os.remove("TEMPORARY.session")
    except:
        pass
    async with pyrogram.Client(
        "TEMPORARY",
        phone_number=phone_number,
        api_id=api_id,
        api_hash=api_hash,
        password=two_factor_pass,
    ) as app:
        result = await app.export_session_string()
    os.remove("TEMPORARY.session")
    return result


def load_sessions() -> Dict:
    if os.path.exists(TELEGRAM_ENV_JSON_PATH):
        with open(TELEGRAM_ENV_JSON_PATH, "r") as f:
            return json.load(f)
    return {
        "defaults": dict(),
        "credentials": dict(),
        "session_strings": dict()
    }


def save_sessions(sessions: Dict):
    with open(TELEGRAM_ENV_JSON_PATH, "w") as f:
        json.dump(sessions, f, indent=4, sort_keys=True)
    print(f"Saved session to {TELEGRAM_ENV_JSON_PATH}")


def add_login_to_env(session_name: str, session_string: str):
    var_name = f"TELEGRAM_SESSION_STRING__{session_name}"
    with open(TELEGRAM_ENV_PATH, "a") as f:
        f.write(f"{var_name} = \"{session_string}\"\n")
    print(f"Added session to {TELEGRAM_ENV_PATH} as {var_name}")


def create_app(phone_number: str):
    sess = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://my.telegram.org",
        "DNT": "1",
        "Connection": "keep-alive",
        "Referer": "https://my.telegram.org/auth?to=apps",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }
    r = sess.post("https://my.telegram.org/auth/send_password", headers=headers, data=f"phone={phone_number}")
    if "Sorry, too many tries. Please try again later." in r.text:
        print("Sorry, too many tries. Please try again later.")
        return
    random_hash = json.loads(r.text)["random_hash"]
    headers["TE"] = "trailers"
    password = input("Enter the password you received: ")
    r = sess.post("https://my.telegram.org/auth/login", headers=headers, data=f"phone={phone_number}&random_hash={random_hash}&password={password}")
    r = sess.get("https://my.telegram.org/apps")
    soup = bs4.BeautifulSoup(r.text, "html.parser")
    hash_create = soup.find("input", {"name": "hash", "type": "hidden"}).get("value")
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://my.telegram.org",
        "DNT": "1",
        "Connection": "keep-alive",
        "Referer": "https://my.telegram.org/apps",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "TE": "trailers",
    }
    r = sess.post("https://my.telegram.org/apps/create", headers=headers, data=f"hash={hash_create}&app_title=MyGptBot&app_shortname=MyGptBot&app_url=&app_platform=android&app_desc=")
    soup = bs4.BeautifulSoup(r.text, "html.parser")
    api_id = soup.find_all("span", {"class": "form-control input-xlarge uneditable-input"})[0].find("strong").text
    api_hash = soup.find_all("span", {"class": "form-control input-xlarge uneditable-input"})[1].text
    return api_id, api_hash


def read(prompt: str, default: Optional[str]) -> str:
    if default is not None:
        return input(f"{prompt} [blank -> {default}]: ") or default
    return input(f"{prompt}: ")


async def main():
    sessions = load_sessions()
    existing_session_names = ", ".join(list(sessions["session_strings"].keys()) or ["NONE"])
    print(f"Existing session names: {existing_session_names}")
    session_name = input("Enter an UPPERCASE NAME for this session: ")
    if session_name in sessions["session_strings"]:
        print(f"Session name {session_name} already exists")
        return
    if not re.match(r"^[A-Z]+$", session_name):
        print(f"Session name {session_name} must match ^[A-Z]+$")
        return
    if sessions["credentials"]:
        s = "You have credentials for:\n"
        for i, phone_number in enumerate(sessions["credentials"]):
            s += f"\t[{i}] {phone_number}\n"
        print(s.strip())
        print()
        phone_number = input(f"Enter your phone number or [1-{len(sessions['credentials'])}]: ")
    else:
        phone_number = input("Enter your phone number: ")
    if phone_number in sessions["credentials"]:
        print(f"Using existing credentials for {phone_number}")
        api_id = sessions["credentials"][phone_number]["api_id"]
        api_hash = sessions["credentials"][phone_number]["api_hash"]
        two_factor_pass = sessions["credentials"][phone_number].get("two_factor_pass")
    else:
        if False:
            api_id, api_hash = create_app(phone_number)
        else:
            print(f"https://my.telegram.org/auth?to=apps")
            api_id = input("Enter your API ID: ")
            api_hash = input("Enter your API hash: ")
        two_factor_pass = sessions["default"]["two_factor_pass"]
        sessions["credentials"][phone_number] = {
            "api_id": api_id,
            "api_hash": api_hash,
            "two_factor_pass": two_factor_pass,
        }
    session_string = await get_session_string(
        api_id=api_id,
        api_hash=api_hash,
        phone_number=phone_number,
        two_factor_pass=two_factor_pass,
    )
    sessions["session_strings"][session_name] = session_string
    save_sessions(sessions)
    add_login_to_env(session_name, session_string)


if __name__ == "__main__":
    asyncio.run(main())
