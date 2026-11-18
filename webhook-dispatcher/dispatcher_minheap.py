import heapq
import itertools
import time
from dataclasses import dataclass, field

from client import MockWebhookClient
from models import Event, NetworkTimeoutError


@dataclass(order=True)
class DeliveryTask:
    # Only next_attempt_at and seq take part in comparisons, so tasks can be heap items:
    # earliest next_attempt_at first, ties broken by enqueue order (seq).
    event: Event = field(compare=False)
    attempts: int = field(default=0, compare=False)
    next_attempt_at: float = 0.0
    seq: int = 0


class WebhookDispatcherMinHeap:
    """
    Same delivery semantics as WebhookDispatcherDeque, but ready-time ordering is
    handled by min-heaps of DeliveryTask (ordered by next_attempt_at, then seq):

    - normal_heap: every pending normal task.
    - high_heap: only the head task of each customer's high-priority FIFO.
    - high_backlog: per customer, the high-priority tasks waiting behind the head. A
      customer has a key here exactly while it has a head in high_heap or in flight.

    A task's next_attempt_at is its heap key, so it is only changed while the task is
    out of its heap. Customer cooldowns can change while a task is queued, so they are
    applied lazily: when a task reaches the top early, its next_attempt_at is pushed
    out to the cooldown and it is re-inserted.
    """

    def __init__(
        self,
        client: MockWebhookClient,
        max_retries: int = 3,
        base_backoff: float = 0.2,
    ):
        self.client = client
        self.max_retries = max_retries
        self.base_backoff = base_backoff

        self.seen_event_ids: set[str] = set()
        self.dead_letter_queue: list[tuple[Event, str]] = []

        self.customer_cooldowns: dict[str, float] = {}

        self._seq = itertools.count()
        self.normal_heap: list[DeliveryTask] = []
        self.high_heap: list[DeliveryTask] = []
        self.high_backlog: dict[str, list[DeliveryTask]] = {}

    def process_events(self, events: list[Event]) -> None:
        self.enqueue(events)

        while self._has_pending_work():
            now = time.time()
            next_item = self._next_ready_task(now)

            if next_item is None:
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
                    # Rate limiting doesn't burn a failure attempt.
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
            task = DeliveryTask(event=event, seq=next(self._seq))

            if event.priority != "high":
                heapq.heappush(self.normal_heap, task)
            elif event.customer_id in self.high_backlog:
                # Customer already has an active head; wait behind it to keep FIFO order.
                self.high_backlog[event.customer_id].append(task)
            else:
                self.high_backlog[event.customer_id] = []
                heapq.heappush(self.high_heap, task)

    def _has_pending_work(self) -> bool:
        return bool(self.normal_heap or self.high_heap)

    def _next_ready_task(self, now: float) -> tuple[DeliveryTask, bool] | None:
        """
        Returns (task, is_high_priority) for the next task eligible to run, or None if
        everything pending is waiting on a timer. High-priority tasks win over normal ones.
        The returned task is removed from its heap; callers must re-push it on retry.
        """
        for heap, is_high_priority in ((self.high_heap, True), (self.normal_heap, False)):
            while heap and heap[0].next_attempt_at <= now:
                task = heapq.heappop(heap)
                cooldown = self.customer_cooldowns.get(task.event.customer_id, 0.0)
                if cooldown <= now:
                    return task, is_high_priority
                # Customer is rate-limited; defer this task until the cooldown ends.
                task.next_attempt_at = cooldown
                heapq.heappush(heap, task)

        return None

    def _earliest_wake_time(self) -> float:
        """Heap keys are lower bounds on readiness, so the tops give the earliest possible wake."""
        heaps = [h for h in (self.high_heap, self.normal_heap) if h]
        return min(h[0].next_attempt_at for h in heaps) if heaps else time.time()

    def _advance_high_priority(self, customer_id: str) -> None:
        """Called when a customer's head finishes; promotes the next waiting task, if any."""
        backlog = self.high_backlog[customer_id]
        if backlog:
            heapq.heappush(self.high_heap, backlog.pop(0))
        else:
            del self.high_backlog[customer_id]

    def _complete_task(self, task: DeliveryTask, is_high_priority: bool) -> None:
        if is_high_priority:
            self._advance_high_priority(task.event.customer_id)

    def _fail_permanently(
        self, task: DeliveryTask, is_high_priority: bool, reason: str
    ) -> None:
        print(f"[DLQ]     {task.event.id} permanently failed ({reason})")
        self.dead_letter_queue.append((task.event, reason))
        if is_high_priority:
            self._advance_high_priority(task.event.customer_id)

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
            delay = self.base_backoff * (2 ** (task.attempts - 1))

        task.next_attempt_at = now + delay
        print(
            f"[RETRY]   {task.event.id} (attempt {task.attempts}/{self.max_retries}) "
            f"scheduled in {delay:.2f}s due to {reason}"
        )

        heapq.heappush(self.high_heap if is_high_priority else self.normal_heap, task)
