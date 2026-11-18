# =====================================================================
# CANDIDATE WORKSPACE: Refactor and improve the dispatcher below
# =====================================================================

import time
from collections import deque
from dataclasses import dataclass

from client import MockWebhookClient
from models import Event, NetworkTimeoutError


@dataclass
class DeliveryTask:
    event: Event
    attempts: int = 0
    next_attempt_at: float = 0.0


class WebhookDispatcherDeque:
    def __init__(
        self,
        client: MockWebhookClient,
        max_retries: int = 3,
        base_backoff: float = 0.2,
    ):
        self.client = client
        self.max_retries = max_retries
        self.base_backoff = base_backoff

        # Stage 2: Deduplication & observability
        self.seen_event_ids: set[str] = set()
        self.dead_letter_queue: list[tuple[Event, str]] = []

        # Stage 2: Per-customer rate limit cooldowns (prevents head-of-line blocking)
        self.customer_cooldowns: dict[str, float] = {}

        # Stage 3: Separate normal queue from per-customer FIFO queues for high-priority events
        self.normal_queue: deque[DeliveryTask] = deque()
        self.high_priority_queues: dict[str, deque[DeliveryTask]] = {}

    def process_events(self, events: list[Event]) -> None:
        self.enqueue(events)

        while self._has_pending_work():
            now = time.time()
            next_item = self._next_ready_task(now)

            if next_item is None:
                # Nothing is runnable right now; sleep only until the earliest timer expires
                sleep_duration = max(0.01, self._earliest_wake_time() - time.time())
                time.sleep(sleep_duration)
                continue

            task, is_high_priority = next_item
            event = task.event
            task.attempts += 1

            try:
                response = self.client.send(event)

                if response.status_code == 200:
                    print(f"[SUCCESS] Delivered {event.id} to {event.customer_id}")
                    self._complete_task(task, is_high_priority)

                elif response.status_code == 429:
                    retry_after = float(response.headers.get("Retry-After", 1))
                    # Note: Rate limiting shouldn't necessarily burn a failure attempt,
                    # or can share the retry budget depending on product requirements.
                    task.attempts -= 1
                    self._schedule_retry(
                        task, is_high_priority, "HTTP 429 Rate Limit", retry_after=retry_after
                    )

                elif 500 <= response.status_code < 600:
                    self._schedule_retry(
                        task, is_high_priority, f"HTTP {response.status_code}"
                    )

                else:
                    # 4xx client errors (400, 401, 404, etc.) are non-retryable
                    self._fail_permanently(
                        task, is_high_priority, f"HTTP {response.status_code}"
                    )

            except NetworkTimeoutError:
                # In production, pass `event.id` as an Idempotency-Key HTTP header
                # so the customer server safely deduplicates phantom timeouts.
                self._schedule_retry(task, is_high_priority, "NetworkTimeoutError")

    def enqueue(self, events: list[Event]) -> None:
        for event in events:
            if event.id in self.seen_event_ids:
                print(f"[SKIP]    Duplicate event {event.id} ignored")
                continue

            self.seen_event_ids.add(event.id)
            task = DeliveryTask(event=event)

            if event.priority == "high":
                if event.customer_id not in self.high_priority_queues:
                    self.high_priority_queues[event.customer_id] = deque()
                self.high_priority_queues[event.customer_id].append(task)
            else:
                self.normal_queue.append(task)

    def _has_pending_work(self) -> bool:
        if self.normal_queue:
            return True
        return any(len(q) > 0 for q in self.high_priority_queues.values())

    def _next_ready_task(self, now: float) -> tuple[DeliveryTask, bool] | None:
        """
        Finds the next task eligible to run right now without blocking on rate-limited customers.
        Returns (task, is_high_priority_head) or None if all pending tasks are waiting on timers.
        """
        # 1. Check the head of each customer's high-priority FIFO queue first
        for customer_id, queue in self.high_priority_queues.items():
            if not queue:
                continue
            if now < self.customer_cooldowns.get(customer_id, 0.0):
                continue
            head_task = queue[0]  # Peek only — never bypass the head of a high-priority queue
            if now >= head_task.next_attempt_at:
                return head_task, True

        # 2. Check normal-priority queue (rotate ready items, skip rate-limited/backoff items)
        for _ in range(len(self.normal_queue)):
            task = self.normal_queue.popleft()
            cust_cooldown = self.customer_cooldowns.get(task.event.customer_id, 0.0)
            if now >= cust_cooldown and now >= task.next_attempt_at:
                return task, False
            # Not ready yet; put back at end so other customers aren't blocked
            self.normal_queue.append(task)

        return None

    def _earliest_wake_time(self) -> float:
        """Calculates the exact minimum sleep needed until the next task or cooldown is ready."""
        wake_times: list[float] = []

        for customer_id, queue in self.high_priority_queues.items():
            if queue:
                cooldown = self.customer_cooldowns.get(customer_id, 0.0)
                wake_times.append(max(cooldown, queue[0].next_attempt_at))

        for task in self.normal_queue:
            cooldown = self.customer_cooldowns.get(task.event.customer_id, 0.0)
            wake_times.append(max(cooldown, task.next_attempt_at))

        return min(wake_times) if wake_times else time.time()

    def _complete_task(self, task: DeliveryTask, is_high_priority: bool) -> None:
        if is_high_priority:
            # Pop the head so the next high-priority event for this customer can proceed
            self.high_priority_queues[task.event.customer_id].popleft()

    def _fail_permanently(
        self, task: DeliveryTask, is_high_priority: bool, reason: str
    ) -> None:
        print(f"[DLQ]     {task.event.id} permanently failed ({reason})")
        self.dead_letter_queue.append((task.event, reason))
        if is_high_priority:
            # Unblock subsequent high-priority events for this customer
            self.high_priority_queues[task.event.customer_id].popleft()

    def _schedule_retry(
        self,
        task: DeliveryTask,
        is_high_priority: bool,
        reason: str,
        retry_after: float | None = None,
    ) -> None:
        if task.attempts >= self.max_retries:
            self._fail_permanently(
                task, is_high_priority, f"Max retries ({self.max_retries}) exceeded: {reason}"
            )
            return

        now = time.time()
        if retry_after is not None:
            delay = retry_after
            self.customer_cooldowns[task.event.customer_id] = now + delay
        else:
            # Exponential backoff
            delay = self.base_backoff * (2 ** (task.attempts - 1))

        task.next_attempt_at = now + delay
        print(
            f"[RETRY]   {task.event.id} (attempt {task.attempts}/{self.max_retries}) "
            f"scheduled in {delay:.2f}s due to {reason}"
        )

        # High-priority tasks stay at the head of their per-customer deque;
        # normal tasks go back into the normal queue.
        if not is_high_priority:
            self.normal_queue.append(task)
