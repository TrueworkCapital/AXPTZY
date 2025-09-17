from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv


load_dotenv()

def _resolve_mysql_url() -> str:
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    # Default host/port if not provided
    host = os.getenv('DB_HOST', '127.0.0.1')
    database = os.getenv('DB_NAME')
    port = os.getenv('DB_PORT', '3306')

    if not (user and password and database):
        raise RuntimeError(
            "MySQL configuration missing. Provide DB_USER, DB_PASSWORD, DB_NAME (and optional DB_HOST, DB_PORT) via environment"
        )

    # Detect common misconfiguration where credentials are embedded in DB_HOST
    if '@' in host:
        raise RuntimeError(
            f"Invalid DB_HOST value '{host}'. Host must not contain '@'. Set DB_HOST to just hostname or IP, e.g. 127.0.0.1"
        )

    # URL-encode credentials to safely handle special characters like '@', ':', '/' etc.
    safe_user = quote_plus(user)
    safe_password = quote_plus(password)

    host_part = f"{host}:{port}" if port else host
    return f"mysql+mysqlconnector://{safe_user}:{safe_password}@{host_part}/{database}"


db_url = _resolve_mysql_url()

engine = create_engine(
    db_url, 
    pool_pre_ping=True,
    pool_size=5,  # Reduced pool size to avoid overwhelming MySQL
    max_overflow=10,  # Reduced overflow
    pool_timeout=60,  # Increased timeout
    pool_recycle=1800,  # Recycle connections every 30 minutes
    pool_reset_on_return='commit',  # Reset connections on return
    connect_args={
        "charset": "utf8mb4",
        "autocommit": False,
        "sql_mode": "TRADITIONAL",
        "connect_timeout": 60,  # Connection timeout
        "read_timeout": 60,     # Read timeout
        "write_timeout": 60,    # Write timeout
        "use_unicode": True,
        "get_warnings": True
    }
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
