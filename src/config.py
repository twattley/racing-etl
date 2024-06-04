from dotenv import load_dotenv
from pydantic_settings import BaseSettings


class Config(BaseSettings):
    tf_email: str
    tf_password: str
    bf_username: str
    bf_password: str
    bf_app_key: str
    bf_certs_path: str
    pg_db_host: str
    pg_db_user: str
    pg_db_name: str
    pg_db_password: str
    pg_db_port: int
    do_spaces_access_key_id: str
    do_spaces_secret_access_key: str
    chromedriver_path: str


def load_config(env_file: str = '.env'):
    load_dotenv(env_file, override=True)
    return Config()

config = load_config()


