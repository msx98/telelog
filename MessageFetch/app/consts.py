from dotenv import load_dotenv, dotenv_values
import os
config = os.environ | dotenv_values()

is_external = config.get("IS_DOCKER", None) in {None, False, 0, "False", "false", "0"}
TELEGRAM_API_ID = config["TELEGRAM_API_ID"]
TELEGRAM_API_HASH = config["TELEGRAM_API_HASH"]
TELEGRAM_PHONE = config["TELEGRAM_PHONE"]
TELEGRAM_PASS = config["TELEGRAM_PASS"]

MYSQL_HOST = config["MYSQL_HOST"]
MYSQL_PORT = config["MYSQL_PORT"]
MYSQL_USER = config["MYSQL_USER"]
MYSQL_PASSWORD = config["MYSQL_PASSWORD"]
MYSQL_DATABASE = config["MYSQL_DATABASE"]

DEBUG_CHAT_ID = int(config["DEBUG_CHAT_ID"])

MONGO_HOST = config["MONGO_HOST"]
MONGO_INITDB_ROOT_USERNAME = config["MONGO_INITDB_ROOT_USERNAME"]
MONGO_INITDB_ROOT_PASSWORD = config["MONGO_INITDB_ROOT_PASSWORD"]
MONGO_DATABASE = config["MONGO_DATABASE"]


if is_external:
    config["MYSQL_HOST"] = "127.0.0.1"
    config["MYSQL_PORT"] = "3308"
    config["MONGO_HOST"] = "mongodb://localhost:27017/"
