# =====================================================================
# Main entry point for running the code.
# DO NOT MODIFY THIS FILE
# =====================================================================

import time

from client import MockWebhookClient
from dispatcher_mod import WebhookDispatcherMod
from models import Event


def generate_sample_events() -> list[Event]:
    return [
        Event("evt_101", "cust_A", {"type": "invoice.paid", "amount": 500}, priority="high"),
        Event("evt_102", "cust_B", {"type": "user.created"}, priority="normal"),
        Event("evt_103", "cust_A", {"type": "invoice.sent"}, priority="high"),
        Event("evt_101", "cust_A", {"type": "invoice.paid", "amount": 500}, priority="high"),  # Duplicate!
        Event("evt_104", "cust_C", {"type": "order.shipped"}, priority="normal"),
        Event("evt_105", "cust_A", {"type": "subscription.updated"}, priority="high"),
        Event("evt_106", "cust_B", {"type": "password.reset"}, priority="normal"),
        Event("evt_107", "cust_C", {"type": "order.delivered"}, priority="normal"),
        Event("evt_108", "cust_A", {"type": "account.closed"}, priority="high"),
        Event("evt_106", "cust_B", {"type": "password.reset"}, priority="normal"),  # Duplicate!
    ]


if __name__ == "__main__":
    start_time = time.time()
    client = MockWebhookClient(seed=5)
    # dispatcher = WebhookDispatcher(client)
    dispatcher = WebhookDispatcherMod(client)

    events = generate_sample_events()
    print(f"Starting dispatch of {len(events)} events...\n")
    dispatcher.process_events(events)

    elapsed = time.time() - start_time
    print("\n--- Summary ---")
    print(f"Total time elapsed: {elapsed:.2f}s")
    print(f"Total server deliveries (including duplicates): {len(client.delivered_log)}")
    delivered_ids = [e.id for e in client.delivered_log]
    print(f"Delivered Event IDs: {delivered_ids}")
