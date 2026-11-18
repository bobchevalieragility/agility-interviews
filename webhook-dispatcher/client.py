# =====================================================================
# Simulates sending webhooks and retrieving responses from customer endpoints.
# DO NOT MODIFY THIS FILE (it simulates external network behavior).
# =====================================================================

import random
import time

from models import Event, HttpResponse, NetworkTimeoutError

class MockWebhookClient:
    def __init__(self, seed: int = 7):
        self._rng = random.Random(seed)
        self._customer_rate_limits: dict[str, float] = {}
        self.delivered_log: list[Event] = []  # Tracks what actually hit customer servers

    def send(self, event: Event) -> HttpResponse:
        now = time.time()

        # Check if customer is currently rate-limited
        if event.customer_id in self._customer_rate_limits:
            reset_time = self._customer_rate_limits[event.customer_id]
            if now < reset_time:
                retry_after = max(1, int(reset_time - now))
                return HttpResponse(
                    status_code=429,
                    headers={"Retry-After": str(retry_after)},
                    body="Too Many Requests",
                )
            else:
                del self._customer_rate_limits[event.customer_id]

        roll = self._rng.random()

        # 15% chance: Trigger rate limit for this customer for 2 seconds
        if roll < 0.15:
            self._customer_rate_limits[event.customer_id] = now + 2.0
            return HttpResponse(
                status_code=429,
                headers={"Retry-After": "2"},
                body="Rate limit exceeded",
            )

        # 15% chance: Transient server error
        if roll < 0.30:
            return HttpResponse(status_code=500, body="Internal Server Error")

        # 10% chance: Permanent client error (bad endpoint / payload)
        if roll < 0.40:
            return HttpResponse(status_code=400, body="Bad Request")

        # 15% chance: Network timeout AFTER server already processed the event!
        if roll < 0.55:
            self.delivered_log.append(event)
            raise NetworkTimeoutError(f"Request timed out for event {event.id}")

        # 45% chance: Clean success
        self.delivered_log.append(event)
        return HttpResponse(status_code=200, body="OK")
