import logging
from collections import defaultdict
from typing import Dict, List, Set, Any

from fastapi import WebSocket

logger = logging.getLogger("asset-allocation.api.realtime")


class RealtimeManager:
    def __init__(self) -> None:
        # Keep track of all active connections
        self.active_connections: Set[WebSocket] = set()
        # Map topic -> connected clients interested in that topic
        self.subscriptions: Dict[str, Set[WebSocket]] = defaultdict(set)

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info("Client connected. Active: %d", len(self.active_connections))

    def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        
        # Remove from all topics
        for subscribers in self.subscriptions.values():
            if websocket in subscribers:
                subscribers.discard(websocket)
                
        logger.info("Client disconnected. Active: %d", len(self.active_connections))

    async def subscribe(self, websocket: WebSocket, topics: List[str]) -> None:
        """Subscribe a client to one or more topics."""
        for topic in topics:
            self.subscriptions[topic].add(websocket)
        logger.debug("Client subscribed to: %s", topics)

    async def unsubscribe(self, websocket: WebSocket, topics: List[str]) -> None:
        """Unsubscribe a client from one or more topics."""
        for topic in topics:
            if websocket in self.subscriptions[topic]:
                self.subscriptions[topic].discard(websocket)

    def has_subscribers(self, topic: str) -> bool:
        return bool(self.subscriptions.get(str(topic or "").strip()))

    async def broadcast(self, topic: str, message: Dict[str, Any]) -> None:
        """
        Broadcast a message to all clients subscribed to the specific topic.
        The message will be wrapped: {"topic": topic, "payload": message}
        """
        targets = self.subscriptions.get(topic)
        if not targets:
            return

        payload = {"topic": topic, "data": message}
        
        # Snapshot to allow removing dead connections during iteration
        for connection in list(targets):
            try:
                await connection.send_json(payload)
            except Exception as exc:
                logger.warning("Failed to send to client (disconnecting): %s", exc)
                self.disconnect(connection)


manager = RealtimeManager()
