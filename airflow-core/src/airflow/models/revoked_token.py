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

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import String, select
from sqlalchemy.orm import Mapped

from airflow.models.base import Base
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime, mapped_column

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


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
        result = session.execute(select(cls).where(cls.jti == jti).limit(1))
        return result.first() is not None
