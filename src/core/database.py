import os
import asyncio
import asyncpg
import logging
import uuid as uuid_lib
from urllib.parse import urlparse
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# SSH Toggle Configuration
USE_SSH_TUNNEL = os.getenv("USE_SSH_TUNNEL", "false").lower() == "true"

# Database URLs (strip quotes if present for compatibility)
DATABASE_URL = os.getenv("DATABASE_URL", "").strip().strip('"').strip("'")
REMOTE_DATABASE_URL = os.getenv("REMOTE_DATABASE_URL", "").strip().strip('"').strip("'")
SSH_TUNNEL_URL = os.getenv("SSH_TUNNEL_URL", "").strip().strip('"').strip("'")

# Local tunnel port
LOCAL_PORT = int(os.getenv("LOCAL_PORT", "5433"))

print(f"SSH Tunnel Enabled: {USE_SSH_TUNNEL}")

# Global resources
ssh_tunnel = None
pg_pool = None


def parse_ssh_url(ssh_url):
    """Parse SSH URL to extract credentials and host information."""
    if not ssh_url:
        raise ValueError("SSH_TUNNEL_URL not provided")
    
    parsed = urlparse(ssh_url)
    return {
        'host': parsed.hostname,
        'port': parsed.port or 22,
        'username': parsed.username,
        'password': parsed.password
    }


def parse_db_url(db_url):
    """Parse PostgreSQL URL to extract connection details."""
    if not db_url:
        raise ValueError("Database URL not provided")
    
    parsed = urlparse(db_url)
    return {
        'host': parsed.hostname,
        'port': parsed.port or 5432,
        'username': parsed.username,
        'password': parsed.password,
        'database': parsed.path.lstrip('/')
    }


async def init_ssh_tunnel():
    """Initialize SSH tunnel (runs in thread to avoid blocking)."""
    global ssh_tunnel
    
    # Skip if SSH tunnel is disabled
    if not USE_SSH_TUNNEL:
        logger.info("ℹ️ SSH Tunnel disabled - connecting directly to database")
        return None
    
    if ssh_tunnel is not None:
        return ssh_tunnel
    
    try:
        # Parse SSH and remote database URLs
        ssh_config = parse_ssh_url(SSH_TUNNEL_URL)
        remote_db_config = parse_db_url(REMOTE_DATABASE_URL)
        
        logger.info(f"🔐 Establishing SSH tunnel to {ssh_config['host']}:{ssh_config['port']}...")
        
        ssh_tunnel = SSHTunnelForwarder(
            (ssh_config['host'], ssh_config['port']),
            ssh_username=ssh_config['username'],
            ssh_password=ssh_config['password'],
            remote_bind_address=(remote_db_config['host'], remote_db_config['port']),
            local_bind_address=('localhost', LOCAL_PORT)
        )
        
        # Start tunnel in a thread-safe way
        ssh_tunnel.start()
        
        logger.info(f"✅ SSH Tunnel established on localhost:{LOCAL_PORT}")
        return ssh_tunnel
        
    except Exception as e:
        logger.exception(f"❌ Failed to establish SSH tunnel: {e}")
        ssh_tunnel = None
        raise


async def init_pg_pool():
    """Initialize PostgreSQL connection pool through SSH tunnel or direct connection."""
    global pg_pool
    
    if pg_pool is not None:
        return pg_pool
    
    try:
        if USE_SSH_TUNNEL:
            await init_ssh_tunnel()
            await asyncio.sleep(0.5)
            connection_url = DATABASE_URL
            logger.info("📊 Creating PostgreSQL connection pool through SSH tunnel...")
        else:
            
            connection_url = DATABASE_URL or REMOTE_DATABASE_URL
            logger.info("📊 Creating PostgreSQL connection pool with direct connection...")
        
        if not connection_url:
            raise RuntimeError("Database URL not configured")
        
        pg_pool = await asyncpg.create_pool(
            dsn=connection_url,
            min_size=2,
            max_size=10,
            timeout=30,
            command_timeout=300
        )
        
        logger.info("✅ PostgreSQL connection pool created")
        return pg_pool
        
    except Exception as e:
        logger.exception(f"❌ Failed to create PG pool: {e}")
        raise
    

async def close_pg_pool():
    """Close PostgreSQL connection pool and SSH tunnel."""
    global pg_pool, ssh_tunnel
    
    # Close database pool first
    if pg_pool:
        await pg_pool.close()
        logger.info("✅ PostgreSQL pool closed")
        pg_pool = None
    
    # Then close SSH tunnel if it was used
    if ssh_tunnel and USE_SSH_TUNNEL:
        ssh_tunnel.stop()
        logger.info("✅ SSH tunnel closed")
        ssh_tunnel = None


async def fetch_song_metadata(mbid: str):
    """
    Tiered metadata fetch with strong error handling:
    1. Try full metadata from mv_song_metadata_core
    2. Fallback to recording-based partial metadata
    3. Fill missing fields with defaults
    """

    DEFAULT_METADATA = {
        "title": "Unknown",
        "artist": "Unknown Artist",
        "album": "Unknown Album",
        "genre": [],
        "duration": 0
    }

    if not mbid:
        logger.warning("⚠️ fetch_song_metadata called with empty MBID")
        return DEFAULT_METADATA | {"song_id": None}

    if not pg_pool:
        logger.error("❌ PostgreSQL pool not initialized")
        return DEFAULT_METADATA | {"song_id": mbid}

    try:
        mbid_uuid = uuid_lib.UUID(mbid)
    except (ValueError, AttributeError) as e:
        logger.error(f"❌ Invalid MBID format: {mbid} - {e}")
        return DEFAULT_METADATA | {"song_id": mbid}

    full_query = """
        SELECT
            c.song_id,
            r.name AS title,
            a.artist,
            rel.name AS album,
            g.genre,
            c.duration
        FROM mv_song_metadata_core c
        JOIN public.recording r
            ON r.id = c.recording_id
        LEFT JOIN mv_artist_by_credit a
            ON a.artist_credit = c.artist_credit
        LEFT JOIN mv_genre_by_recording g
            ON g.recording = c.recording_id
        LEFT JOIN public.release rel
            ON rel.id = c.release_id
        WHERE c.song_id = $1;
    """
    try:
        async with pg_pool.acquire() as conn:
            row = await conn.fetchrow(full_query, mbid_uuid)
            if row:
                logger.info(f"✅ Full metadata found for MBID: {mbid}")
                return {
                    "song_id": str(row["song_id"]),
                    "title": row["title"] or DEFAULT_METADATA["title"],
                    "artist": row["artist"] or DEFAULT_METADATA["artist"],
                    "album": row["album"] or DEFAULT_METADATA["album"],
                    "genre": row["genre"] or [],
                    "duration": int(row["duration"]) if row["duration"] else 0
                }
            logger.warning(f"⚠️ Full metadata missing for MBID: {mbid}")

    except asyncpg.PostgresError as db_err:
        logger.exception(f"❌ DB error during full metadata fetch for {mbid}: {db_err}")
    except Exception as e:
        logger.exception(f"❌ Unexpected error in full metadata fetch for {mbid}: {e}")

    try:
        partial = await fetch_partial_metadata(mbid)

        if partial:
            logger.info(f"✅ Partial metadata found for MBID: {mbid}")
            return {
                "song_id": mbid,
                "title": partial.get("title") or DEFAULT_METADATA["title"],
                "artist": partial.get("artist") or DEFAULT_METADATA["artist"],
                "album": DEFAULT_METADATA["album"],
                "genre": [],
                "duration": 0
            }

        logger.warning(f"⚠️ Partial metadata missing for MBID: {mbid}")

    except asyncpg.PostgresError as db_err:
        logger.exception(f"❌ DB error during partial metadata fetch for {mbid}: {db_err}")
    except Exception as e:
        logger.exception(f"❌ Unexpected error in partial metadata fetch for {mbid}: {e}")

    logger.warning(f"⚠️ Using default metadata for MBID: {mbid}")

    return DEFAULT_METADATA | {"song_id": mbid}


async def test_connection():
    """Test SSH tunnel (if enabled) and database connection."""
    try:
        await init_pg_pool()
        async with pg_pool.acquire() as conn:
            version = await conn.fetchval('SELECT version();')
            logger.info(f"✅ Connection test successful. PostgreSQL version: {version[:50]}...")
            return True
    except Exception as e:
        logger.exception(f"❌ Connection test failed: {e}")
        return False

def get_sqlalchemy_url() -> str:
    """
    Returns the correct synchronous Database URL using the Pure-Python pg8000 driver.
    This prevents SSL binary conflicts (free(): invalid pointer crashes) in Docker.
    """
    # Use pg8000 driver instead of psycopg2
    driver = "postgresql+pg8000"

    if USE_SSH_TUNNEL:
        # --- LOCAL MODE (Using SSH Tunnel) ---
        remote_conf = parse_db_url(REMOTE_DATABASE_URL)
        
        # We construct the URL manually, forcing pg8000
        return (
            f"{driver}://{remote_conf['username']}:{remote_conf['password']}"
            f"@localhost:{LOCAL_PORT}/{remote_conf['database']}"
        )
    else:
        # --- SERVER MODE (Direct Connection) ---
        target_url = DATABASE_URL or REMOTE_DATABASE_URL
        
        if not target_url:
            raise ValueError("No Database URL configured for direct connection.")

        # Ensure we replace the protocol with our safe driver
        if "://" in target_url:
            # Strip the existing protocol (e.g., postgres:// or postgresql://)
            # and replace it with postgresql+pg8000://
            credentials = target_url.split("://", 1)[1]
            return f"{driver}://{credentials}"
            
        return target_url
    
async def fetch_partial_metadata(mbid: str):
    """
    Fetch minimal metadata (title, artist) from recording tables.
    Used when mv_song_metadata_core has no row.
    """
    if not mbid or not pg_pool:
        return None

    try:
        mbid_uuid = uuid_lib.UUID(mbid)
    except Exception:
        return None

    query = """
        SELECT
            r.name AS title,
            ac.name AS artist
        FROM recording r
        JOIN artist_credit ac
            ON r.artist_credit = ac.id
        WHERE r.gid = $1;
    """

    try:
        async with pg_pool.acquire() as conn:
            row = await conn.fetchrow(query, mbid_uuid)
            if row:
                return {
                    "title": row["title"],
                    "artist": row["artist"]
                }
            return None
    except Exception as e:
        logger.exception(f"❌ Partial metadata fetch failed for {mbid}: {e}")
        return None