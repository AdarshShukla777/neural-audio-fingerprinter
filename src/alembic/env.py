import sys
import os
from logging.config import fileConfig
from urllib.parse import urlparse

from sqlalchemy import engine_from_config
from sqlalchemy import pool
from sqlalchemy.engine import make_url 
from alembic import context
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder 

load_dotenv()
sys.path.insert(0, os.path.realpath(os.path.join(os.path.dirname(__file__), '..')))

from core.models import Base

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

# --- CONFIGURATION FROM .ENV ---
USE_SSH_TUNNEL = os.getenv("USE_SSH_TUNNEL", "false").lower() == "true"
DATABASE_URL = os.getenv("DATABASE_URL", "").strip().strip('"').strip("'")
REMOTE_DATABASE_URL = os.getenv("REMOTE_DATABASE_URL", "").strip().strip('"').strip("'")
SSH_TUNNEL_URL = os.getenv("SSH_TUNNEL_URL", "").strip().strip('"').strip("'")
LOCAL_PORT = int(os.getenv("LOCAL_PORT", "5433"))

def include_object(object, name, type_, reflected, compare_to):
    """
    Filter to ensure we only migrate tables in the 'wazzdat' schema.
    """
    if type_ == "table":
        if object.schema == "wazzdat":
            return True
        return False
    return True

def get_safe_url(url_string):
    """
    Escapes % characters for config parser compatibility.
    """
    return url_string.replace("%", "%%")

def force_pg8000_driver(url_string):
    """
    CRITICAL: Replaces the protocol with 'postgresql+pg8000' to avoid 
    'ModuleNotFoundError: No module named psycopg2' and 'invalid pointer' crashes.
    """
    if not url_string:
        return ""
    
    # If the URL has a protocol (e.g. postgres:// or postgresql://)
    if "://" in url_string:
        # Split credentials part: user:pass@host:port/db
        credentials_part = url_string.split("://", 1)[1]
        return f"postgresql+pg8000://{credentials_part}"
    
    return url_string

def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    # Logic: If SSH is OFF, use DATABASE_URL. If ON, use REMOTE_DATABASE_URL.
    raw_url = REMOTE_DATABASE_URL if USE_SSH_TUNNEL else DATABASE_URL
    
    # FORCE PG8000
    final_url = force_pg8000_driver(raw_url)

    if not final_url:
         raise ValueError("Database URL not set in .env")

    context.configure(
        url=get_safe_url(final_url),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        include_object=include_object 
    )

    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    server = None
    final_db_url = ""
    
    # 1. DETERMINE WHICH URL TO USE
    if USE_SSH_TUNNEL:
        # --- LOCAL DEV MODE (Needs Tunnel) ---
        if not REMOTE_DATABASE_URL or not SSH_TUNNEL_URL:
            raise ValueError("USE_SSH_TUNNEL is True, but REMOTE_DATABASE_URL or SSH_TUNNEL_URL is missing.")
            
        db_url = make_url(REMOTE_DATABASE_URL)
        ssh_url = make_url(SSH_TUNNEL_URL)

        print(f"Starting SSH Tunnel to {ssh_url.host}...")
        server = SSHTunnelForwarder(
            (ssh_url.host, ssh_url.port or 22),
            ssh_username=ssh_url.username,
            ssh_password=ssh_url.password,
            remote_bind_address=(db_url.host, db_url.port or 5432),
            local_bind_address=('localhost', LOCAL_PORT) 
        )
        server.start()
        print(f"SSH Tunnel active on localhost:{server.local_bind_port}")
        
        # Rewrite URL to point to Localhost Tunnel
        # We manually construct this so we can FORCE postgresql+pg8000 here too
        final_db_url = (
            f"postgresql+pg8000://{db_url.username}:{db_url.password}"
            f"@localhost:{server.local_bind_port}/{db_url.database}"
        )
        
    else:
        # --- SERVER MODE (Direct Connect) ---
        if not DATABASE_URL:
            raise ValueError("USE_SSH_TUNNEL is False, but DATABASE_URL is missing.")
            
        print("SSH Tunnel disabled. Connecting directly.")
        # FORCE PG8000 on the direct URL
        final_db_url = force_pg8000_driver(DATABASE_URL)

    # 2. CONFIGURE ALCHEMY
    # Inject the pg8000 URL into the config
    config.set_main_option("sqlalchemy.url", get_safe_url(final_db_url))

    try:
        connectable = engine_from_config(
            config.get_section(config.config_ini_section, {}),
            prefix="sqlalchemy.",
            poolclass=pool.NullPool,
        )

        with connectable.connect() as connection:
            context.configure(
                connection=connection, 
                target_metadata=target_metadata,
                include_schemas=True, 
                include_object=include_object 
            )

            with context.begin_transaction():
                context.run_migrations()
                
    finally:
        # 3. CLEANUP
        if server:
            server.stop()
            print("SSH Tunnel stopped.")

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()