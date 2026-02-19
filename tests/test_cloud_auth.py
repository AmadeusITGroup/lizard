# tests/test_cloud_diagnostics.py
"""Unit tests for cloud/diagnostics.py — error hierarchy and serialisation."""
from __future__ import annotations

from cloud.diagnostics import (
    AuthenticationError,
    ClusterNotAvailableError,
    ConfigurationError,
    ConnectivityError,
    GatewayExposureError,
    LizardCloudError,
)


def test_base_error_message():
    err = LizardCloudError(message="boom", action="fix it", context={"k": "v"})
    assert "boom" in str(err)
    assert "fix it" in str(err)


def test_to_dict():
    err = LizardCloudError(message="msg", action="act", context={"x": 1})
    d = err.to_dict()
    assert d["error_type"] == "LizardCloudError"
    assert d["message"] == "msg"
    assert d["action"] == "act"
    assert d["context"] == {"x": 1}


def test_subclass_error_type():
    err = GatewayExposureError(message="not exposed", action="switch gateway")
    d = err.to_dict()
    assert d["error_type"] == "GatewayExposureError"


def test_all_subclasses_inherit():
    for cls in (
        GatewayExposureError,
        ConnectivityError,
        AuthenticationError,
        ClusterNotAvailableError,
        ConfigurationError,
    ):
        err = cls(message="test")
        assert isinstance(err, LizardCloudError)
        assert isinstance(err, Exception)


def test_error_without_action():
    err = ConfigurationError(message="bad config")
    d = err.to_dict()
    assert d["action"] == ""
    assert "bad config" in str(err)


def test_error_default_context():
    err = ConnectivityError(message="timeout")
    assert err.context == {}