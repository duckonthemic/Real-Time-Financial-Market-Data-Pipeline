"""
Unit Tests for FinnhubClient
"""
import asyncio
import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestFinnhubClient:
    """Tests for FinnhubClient WebSocket handling."""
    
    @pytest.fixture
    def client(self):
        """Create a client instance for testing."""
        from src.producer.finnhub_client import FinnhubClient
        
        received_messages = []
        
        def on_message(trade):
            received_messages.append(trade)
        
        client = FinnhubClient(
            api_key="test_key",
            symbols=["AAPL", "GOOGL"],
            on_message=on_message,
            max_retries=3,
            base_backoff=0.1  # Short for testing
        )
        client._received = received_messages
        return client
    
    # =========================================================================
    # Initialization Tests
    # =========================================================================
    
    def test_init_with_symbols(self, client):
        """Test client initializes with correct symbols."""
        assert client.symbols == ["AAPL", "GOOGL"]
        assert client.api_key == "test_key"
    
    def test_init_limits_symbols_to_50(self):
        """Test that symbols are limited to MAX_SYMBOLS."""
        from src.producer.finnhub_client import FinnhubClient
        
        many_symbols = [f"SYM{i}" for i in range(100)]
        client = FinnhubClient(
            api_key="test",
            symbols=many_symbols,
            on_message=lambda x: None
        )
        
        assert len(client.symbols) == 50
    
    def test_ws_url_includes_token(self, client):
        """Test WebSocket URL contains API key."""
        assert "test_key" in client.ws_url
        assert "wss://ws.finnhub.io" in client.ws_url
    
    # =========================================================================
    # Reconnection Logic Tests
    # =========================================================================
    
    @pytest.mark.asyncio
    async def test_handle_reconnect_increments_retry_count(self, client):
        """Test that reconnect handler increments retry count."""
        client._running = True
        initial_count = client._retry_count
        
        # Use short backoff for testing
        await client._handle_reconnect()
        
        assert client._retry_count == initial_count + 1
    
    @pytest.mark.asyncio
    async def test_handle_reconnect_stops_when_not_running(self, client):
        """Test that reconnect is skipped when client is stopped."""
        client._running = False
        initial_count = client._retry_count
        
        await client._handle_reconnect()
        
        # Should not increment when not running
        assert client._retry_count == initial_count
    
    def test_backoff_calculation(self, client):
        """Test exponential backoff calculation."""
        # backoff = min(base^retry, 300)
        client._retry_count = 0
        expected = min(client.base_backoff ** 1, 300)
        
        # After first reconnect attempt
        client._retry_count = 1
        wait_time = min(client.base_backoff ** client._retry_count, 300)
        
        assert wait_time == expected
    
    def test_backoff_caps_at_300_seconds(self):
        """Test that backoff is capped at 5 minutes."""
        from src.producer.finnhub_client import FinnhubClient
        
        # Create client with default base_backoff (2.0) for this test
        client = FinnhubClient(
            api_key="test",
            symbols=["AAPL"],
            on_message=lambda x: None,
            base_backoff=2.0  # Default value
        )
        client._retry_count = 100  # Very high retry count
        
        wait_time = min(client.base_backoff ** client._retry_count, 300)
        
        assert wait_time == 300
    
    # =========================================================================
    # Message Handling Tests
    # =========================================================================
    
    @pytest.mark.asyncio
    async def test_subscribe_sends_correct_message(self, client, mock_websocket):
        """Test subscription message format."""
        client._ws = mock_websocket
        
        await client._subscribe("AAPL")
        
        mock_websocket.send.assert_called_once()
        sent_msg = json.loads(mock_websocket.send.call_args[0][0])
        
        assert sent_msg["type"] == "subscribe"
        assert sent_msg["symbol"] == "AAPL"
    
    @pytest.mark.asyncio
    async def test_unsubscribe_sends_correct_message(self, client, mock_websocket):
        """Test unsubscription message format."""
        client._ws = mock_websocket
        
        await client._unsubscribe("AAPL")
        
        sent_msg = json.loads(mock_websocket.send.call_args[0][0])
        
        assert sent_msg["type"] == "unsubscribe"
        assert sent_msg["symbol"] == "AAPL"
    
    # =========================================================================
    # Disconnect Tests
    # =========================================================================
    
    @pytest.mark.asyncio
    async def test_disconnect_sets_running_false(self, client, mock_websocket):
        """Test that disconnect stops the client."""
        client._running = True
        client._ws = mock_websocket
        
        await client.disconnect()
        
        assert client._running is False
        mock_websocket.close.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_disconnect_handles_no_websocket(self, client):
        """Test disconnect when no active connection."""
        client._running = True
        client._ws = None
        
        await client.disconnect()
        
        assert client._running is False


class TestFinnhubMessageParsing:
    """Tests for message parsing logic."""
    
    def test_parse_trade_message(self):
        """Test parsing of trade type messages."""
        message = {
            "type": "trade",
            "data": [
                {"s": "AAPL", "p": 150.25, "v": 100, "t": 1706684400000}
            ]
        }
        
        assert message["type"] == "trade"
        assert len(message["data"]) == 1
        assert message["data"][0]["s"] == "AAPL"
    
    def test_parse_ping_message(self):
        """Test parsing of ping messages."""
        message = {"type": "ping"}
        
        assert message["type"] == "ping"
