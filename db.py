import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# FORCE load config.env
env_path = os.path.join(os.path.dirname(__file__), "config.env")
load_dotenv(env_path)

def get_engine():
    host = os.getenv("DB_HOST")
    db = os.getenv("DB_NAME")
    port = os.getenv("DB_PORT")
    user = os.getenv("DB_USER")
    pwd = os.getenv("DB_PASSWORD")

    # HARD FAIL with clear message
    if port is None:
        raise RuntimeError(
            "DB_PORT is None. config.env not loaded correctly."
        )

    url = f"postgresql://{user}:{pwd}@{host}:{int(port)}/{db}"
    return create_engine(url)
