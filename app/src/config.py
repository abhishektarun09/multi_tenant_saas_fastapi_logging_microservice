from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str
    better_stack_token: str

    aiven_kafka_bootstrap: str
    aiven_kafka_topic: str

    AIVEN_KAFKA_CA_PEM_B64: str
    AIVEN_KAFKA_SERVICE_CERT_B64: str
    AIVEN_KAFKA_SERVICE_KEY_B64: str

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


env = Settings()
