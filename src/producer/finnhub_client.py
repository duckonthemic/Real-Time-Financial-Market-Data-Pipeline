"""
Finnhub WebSocket Client
Handles connection to Finnhub WebSocket API with reconnection logic.
"""
import asyncio
import json
import logging
from datetime import datetime
from typing import Callable, List, Optional

import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)


class FinnhubClient:
    """
    Resilient WebSocket client for Finnhub market data.
    
    Implements exponential backoff reconnection strategy to handle
    network interruptions and rate limiting.
    """
    
    WS_URL = "wss://ws.finnhub.io"
    MAX_SYMBOLS = 50  # Finnhub free tier limit
    
    def __init__(
        self,
        api_key: str,
        symbols: List[str],
        on_message: Callable[[dict], None],
        max_retries: int = 10,
        base_backoff: float = 2.0
    ):
        """
        Initialize Finnhub client.
        
        Args:
            api_key: Finnhub API key
            symbols: List of symbols to subscribe (max 50)
            on_message: Callback for received messages
            max_retries: Maximum reconnection attempts
            base_backoff: Base for exponential backoff (seconds)
        """
        self.api_key = api_key
        self.symbols = symbols[:self.MAX_SYMBOLS]
        self.on_message = on_message
        self.max_retries = max_retries
        self.base_backoff = base_backoff
        
        self._retry_count = 0
        self._running = False
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        
    @property
    def ws_url(self) -> str:
        """Construct WebSocket URL with API key."""
        return f"{self.WS_URL}?token={self.api_key}"
    
    async def connect(self) -> None:
        """Start WebSocket connection with automatic reconnection."""
        self._running = True
        
        while self._running and self._retry_count < self.max_retries:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    self._ws = ws
                    self._retry_count = 0  # Reset on successful connection
                    logger.info("Connected to Finnhub WebSocket")
                    
                    # Subscribe to symbols
                    await self._subscribe_all()
                    
                    # Handle incoming messages
                    await self._handle_messages()
                    
            except ConnectionClosed as e:
                logger.warning(f"Connection closed: {e.code} - {e.reason}")
                await self._handle_reconnect()
                
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                await self._handle_reconnect()
    
    async def _subscribe_all(self) -> None:
        """Subscribe to all configured symbols."""
        for symbol in self.symbols:
            await self._subscribe(symbol)
            await asyncio.sleep(0.1)  # Rate limiting
            
    async def _subscribe(self, symbol: str) -> None:
        """Subscribe to a single symbol."""
        message = {"type": "subscribe", "symbol": symbol}
        await self._ws.send(json.dumps(message))
        logger.debug(f"Subscribed to {symbol}")
        
    async def _unsubscribe(self, symbol: str) -> None:
        """Unsubscribe from a symbol."""
        message = {"type": "unsubscribe", "symbol": symbol}
        await self._ws.send(json.dumps(message))
        logger.debug(f"Unsubscribed from {symbol}")
        
    async def _handle_messages(self) -> None:
        """Process incoming WebSocket messages."""
        async for message in self._ws:
            try:
                data = json.loads(message)
                
                if data.get("type") == "trade":
                    for trade in data.get("data", []):
                        trade["received_at"] = datetime.utcnow().isoformat()
                        self.on_message(trade)
                        
                elif data.get("type") == "ping":
                    logger.debug("Received ping from server")
                    
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON received: {e}")
                
    async def _handle_reconnect(self) -> None:
        """Handle reconnection with exponential backoff."""
        if not self._running:
            return
            
        self._retry_count += 1
        wait_time = min(self.base_backoff ** self._retry_count, 300)  # Max 5 min
        
        logger.info(
            f"Reconnecting in {wait_time:.1f}s "
            f"(attempt {self._retry_count}/{self.max_retries})"
        )
        await asyncio.sleep(wait_time)
        
    async def disconnect(self) -> None:
        """Gracefully disconnect from WebSocket."""
        self._running = False
        if self._ws:
            await self._ws.close()
            logger.info("Disconnected from Finnhub WebSocket")


# Example usage
if __name__ == "__main__":
    import os
    
    def handle_trade(trade: dict):
        print(f"Trade: {trade['s']} @ ${trade['p']} x {trade['v']}")
    
    client = FinnhubClient(
        api_key=os.getenv("FINNHUB_API_KEY", "demo"),
        symbols=["AAPL", "GOOGL", "MSFT"],
        on_message=handle_trade
    )
    
    asyncio.run(client.connect())
