# db_models.py
from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base

Base = declarative_base()

# 1. Defined as a simple Class with string constants (No Enum)
# This acts as your "Text Choices" source of truth
class DownloadStatus:
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"

# 2. The ORM Table Definition
class S3DownloadsLogORM(Base):
    __tablename__ = "s3_downloads_log"
    __table_args__ = {"schema": "wazzdat"} 

    id = Column(Integer, primary_key=True, index=True)
    mbid = Column(UUID(as_uuid=True), nullable=False, index=True)
    url = Column(String)
    title = Column(String, nullable=True)
    s3_key = Column(Text, nullable=True)
    download_status = Column(String(50), nullable=False, default=DownloadStatus.PENDING)
    
    created_at = Column(TIMESTAMP(timezone=True), server_default=text("now()"))