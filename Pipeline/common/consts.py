from dotenv import dotenv_values
import os

consts = os.environ | dotenv_values()

for key, value in consts.items():
    os.environ[key] = value
