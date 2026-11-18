# =====================================================================
# CANDIDATE WORKSPACE: Refactor and improve the dispatcher below
# =====================================================================

from client import MockWebhookClient
from models import Event, NetworkTimeoutError


class WebhookDispatcher:
    def __init__(self, client: MockWebhookClient):
        self.client = client

    def process_events(self, events: list[Event]) -> None:
        for event in events:
            try:
                response = self.client.send(event)
                if response.status_code == 200:
                    print(f"[SUCCESS] Delivered {event.id} to {event.customer_id}")
                else:
                    print(f"[FAILED]  {event.id} -> HTTP {response.status_code}")
            except NetworkTimeoutError as e:
                print(f"[ERROR]   {e}")
