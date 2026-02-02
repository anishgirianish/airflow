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

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from airflow.models.revoked_token import RevokedToken


class TestRevokedTokenModel:
    def test_revoke_inserts_row(self):
        """Test that revoke calls session.merge with a RevokedToken instance."""
        mock_session = MagicMock()
        exp = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        RevokedToken.revoke("test-jti-123", exp, session=mock_session)
        mock_session.merge.assert_called_once()
        arg = mock_session.merge.call_args[0][0]
        assert isinstance(arg, RevokedToken)
        assert arg.jti == "test-jti-123"
        assert arg.exp == exp

    def test_is_revoked_returns_true(self):
        """Test that a revoked JTI is detected."""
        mock_session = MagicMock()
        mock_session.scalar.return_value = True
        result = RevokedToken.is_revoked("known-jti", session=mock_session)
        assert result is True

    def test_is_revoked_returns_false(self):
        """Test that an unknown JTI returns False."""
        mock_session = MagicMock()
        mock_session.scalar.return_value = False
        result = RevokedToken.is_revoked("unknown-jti", session=mock_session)
        assert result is False
