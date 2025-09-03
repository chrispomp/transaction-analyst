# src/txn_agent/common/cancellation.py

class CancellationToken:
    def __init__(self):
        self._is_cancelled = False

    def request_cancellation(self):
        self._is_cancelled = True

    def is_cancellation_requested(self):
        return self._is_cancelled

    def reset(self):
        self._is_cancelled = False

cancellation_token = CancellationToken()