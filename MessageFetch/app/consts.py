from dotenv import load_dotenv
import os
load_dotenv()

is_external = "DOCKER_HOST" in os.environ
TELEGRAM_API_ID = os.environ["TELEGRAM_API_ID"]
TELEGRAM_API_HASH = os.environ["TELEGRAM_API_HASH"]
TELEGRAM_PHONE = os.environ["TELEGRAM_PHONE"]
TELEGRAM_PASS = os.environ["TELEGRAM_PASS"]

if is_external:
    os.environ["MYSQL_HOST"] = "127.0.0.1"
    os.environ["MYSQL_PORT"] = "3308"
    os.environ["MONGO_HOST"] = "mongodb://localhost:27017/"
