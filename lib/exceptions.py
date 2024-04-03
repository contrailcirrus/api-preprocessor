"""
Custom exceptions.
"""


class QueueEmptyError(Exception):
    """
    pubsub subscription queue is empty.
    """

    pass
