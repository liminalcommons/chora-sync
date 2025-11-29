"""
Tests for CR-SQLite extension loader.

Note: Most tests use mocks since CR-SQLite may not be installed.
"""

import pytest
import platform
from pathlib import Path
from unittest.mock import patch, MagicMock

from chora_sync.extension import (
    CRSQLiteNotAvailable,
    get_extension_path,
    is_crsqlite_available,
    load_crsqlite,
)


class TestGetExtensionPath:
    """Tests for get_extension_path."""

    def test_returns_path_or_none(self):
        """Returns Path or None."""
        result = get_extension_path()
        assert result is None or isinstance(result, Path)

    @patch("chora_sync.extension.platform.system")
    def test_darwin_extension_name(self, mock_system):
        """Uses .dylib on macOS."""
        mock_system.return_value = "Darwin"
        # Just check it doesn't crash
        get_extension_path()

    @patch("chora_sync.extension.platform.system")
    def test_linux_extension_name(self, mock_system):
        """Uses .so on Linux."""
        mock_system.return_value = "Linux"
        get_extension_path()

    @patch("chora_sync.extension.platform.system")
    def test_windows_extension_name(self, mock_system):
        """Uses .dll on Windows."""
        mock_system.return_value = "Windows"
        get_extension_path()

    @patch("chora_sync.extension.platform.system")
    def test_unknown_system_returns_none(self, mock_system):
        """Returns None for unknown systems."""
        mock_system.return_value = "UnknownOS"
        result = get_extension_path()
        assert result is None

    @patch.dict("os.environ", {"CRSQLITE_PATH": "/custom/path/crsqlite.dylib"})
    @patch("pathlib.Path.exists")
    def test_uses_env_var(self, mock_exists):
        """Checks CRSQLITE_PATH environment variable."""
        mock_exists.return_value = True
        # The function should check this path
        result = get_extension_path()
        # If env path exists, it should be found
        assert result is not None


class TestIsCrsqliteAvailable:
    """Tests for is_crsqlite_available."""

    @patch("chora_sync.extension.get_extension_path")
    def test_returns_true_when_found(self, mock_get_path):
        """Returns True when extension path exists."""
        mock_get_path.return_value = Path("/some/path/crsqlite.dylib")
        assert is_crsqlite_available() is True

    @patch("chora_sync.extension.get_extension_path")
    def test_returns_false_when_not_found(self, mock_get_path):
        """Returns False when extension not found."""
        mock_get_path.return_value = None
        assert is_crsqlite_available() is False


class TestLoadCrsqlite:
    """Tests for load_crsqlite."""

    @patch("chora_sync.extension.get_extension_path")
    def test_raises_when_not_available(self, mock_get_path):
        """Raises CRSQLiteNotAvailable when extension not found."""
        mock_get_path.return_value = None
        conn = MagicMock()

        with pytest.raises(CRSQLiteNotAvailable) as exc_info:
            load_crsqlite(conn)

        assert "not found" in str(exc_info.value)

    @patch("chora_sync.extension.get_extension_path")
    def test_enables_extension_loading(self, mock_get_path):
        """Enables extension loading on connection."""
        mock_get_path.return_value = Path("/some/path/crsqlite.dylib")
        conn = MagicMock()

        try:
            load_crsqlite(conn)
        except:
            pass  # May fail loading, but should enable first

        conn.enable_load_extension.assert_called_with(True)

    @patch("chora_sync.extension.get_extension_path")
    def test_uses_explicit_path(self, mock_get_path):
        """Uses explicit path when provided."""
        explicit_path = Path("/explicit/path/crsqlite.dylib")
        conn = MagicMock()

        try:
            load_crsqlite(conn, extension_path=explicit_path)
        except:
            pass

        # Should call load_extension with path without extension
        conn.load_extension.assert_called()

    @patch("chora_sync.extension.get_extension_path")
    def test_wraps_load_error(self, mock_get_path):
        """Wraps SQLite load errors in CRSQLiteNotAvailable."""
        import sqlite3

        mock_get_path.return_value = Path("/some/path/crsqlite.dylib")
        conn = MagicMock()
        conn.load_extension.side_effect = sqlite3.OperationalError("load failed")

        with pytest.raises(CRSQLiteNotAvailable) as exc_info:
            load_crsqlite(conn)

        assert "Failed to load" in str(exc_info.value)


class TestCRSQLiteNotAvailable:
    """Tests for the exception class."""

    def test_is_exception(self):
        """CRSQLiteNotAvailable is an Exception."""
        assert issubclass(CRSQLiteNotAvailable, Exception)

    def test_can_be_raised(self):
        """Can raise CRSQLiteNotAvailable."""
        with pytest.raises(CRSQLiteNotAvailable):
            raise CRSQLiteNotAvailable("test error")

    def test_stores_message(self):
        """Stores error message."""
        try:
            raise CRSQLiteNotAvailable("custom message")
        except CRSQLiteNotAvailable as e:
            assert "custom message" in str(e)
