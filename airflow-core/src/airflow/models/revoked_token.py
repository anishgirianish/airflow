#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import structlog
from sqlalchemy import String, delete, exists, select
from sqlalchemy.orm import Mapped

from airflow.models.base import Base
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime, mapped_column

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

log = structlog.get_logger(__name__)

# Track last cleanup time to avoid running cleanup on every request
_last_cleanup_time: float = 0.0
# Run cleanup approximately every hour (3600 seconds)
_CLEANUP_INTERVAL_SECONDS: int = 3600


class RevokedToken(Base):
    """Stores revoked JWT token JTIs to support token invalidation on logout."""

    __tablename__ = "revoked_token"

    jti: Mapped[str] = mapped_column(String(32), primary_key=True)
    exp: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False, index=True)

    @classmethod
    @provide_session
    def revoke(cls, jti: str, exp: datetime, session: Session = NEW_SESSION) -> None:
        """Add a token JTI to the revoked tokens."""
        session.merge(cls(jti=jti, exp=exp))

    @classmethod
    @provide_session
    def is_revoked(cls, jti: str, session: Session = NEW_SESSION) -> bool:
        """Check if a token JTI has been revoked."""
        cls._maybe_cleanup_expired(session)
        return bool(session.scalar(select(exists().where(cls.jti == jti))))

    @classmethod
    def _maybe_cleanup_expired(cls, session: Session) -> None:
        """
        Periodically clean up expired revoked tokens.

        Runs approximately once per hour during token validation to prevent
        unbounded table growth. Uses monotonic time to track intervals.
        """
        global _last_cleanup_time
        now = time.monotonic()
        if now - _last_cleanup_time >= _CLEANUP_INTERVAL_SECONDS:
            _last_cleanup_time = now
            try:
                session.execute(delete(cls).where(cls.exp < datetime.now(tz=timezone.utc)))
            except Exception:
                log.exception("Failed to clean up expired revoked tokens")
