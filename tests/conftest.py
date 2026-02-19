# tests/conftest.py
"""
Shared test fixtures for Lizard API tests.

Creates a fresh in-memory SQLite database for each test session,
patches the app's DB engine/session, and provides a pre-configured
httpx AsyncClient with ASGITransport.
"""
from __future__ import annotations

import pytest
import httpx

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from domain.models import Base


# ── In-memory DB engine (shared across test session) ─────────────────

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"

_test_engine = create_async_engine(TEST_DB_URL, echo=False, future=True)
_TestSession = async_sessionmaker(_test_engine, expire_on_commit=False)


@pytest.fixture(autouse=True)
async def _setup_test_db():
    """
    Before each test:
      1. Create all tables in the in-memory DB
      2. Patch app.main's engine/session to use the test DB
      3. After the test, drop all tables (clean slate)
    """
    # Create tables
    async with _test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Patch the app's DB dependencies
    import app.main as main_module

    original_engine = main_module.engine
    original_session = main_module.SessionLocal

    main_module.engine = _test_engine
    main_module.SessionLocal = _TestSession

    # Also patch the get_session dependency to use our test session
    async def _test_get_session():
        async with _TestSession() as session:
            yield session

    main_module.get_session = _test_get_session

    yield

    # Teardown: drop all tables and restore originals
    async with _test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    main_module.engine = original_engine
    main_module.SessionLocal = original_session


@pytest.fixture
async def client():
    """Provide an httpx AsyncClient wired to the FastAPI app."""
    from app.main import app

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac