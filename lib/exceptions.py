"""
Custom exceptions.
"""


class QueueEmptyError(Exception):
    """
    pubsub subscription queue is empty.
    """

    pass


class ZarrStoreDownloadError(Exception):
    """
    failure to download complete zarr store.
    """
