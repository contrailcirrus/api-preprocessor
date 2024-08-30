import concurrent.futures
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, List, Callable

from google.cloud import pubsub_v1
import google.auth
import google.api_core.exceptions

from lib.schemas import ApiPreprocessorJob


class PubSubSubscriptionHandler:
    """
    Handler for managing consumption and marshalling of jobs from a pubsub subscription queue.
    """

    def __init__(
        self,
        subscription: str,
        pull_timeout_sec: float = 60.0,
    ):
        """
        Parameters
        ----------
        subscription
            The fully-qualified URI for the pubsub subscription.
            e.g. 'projects/contrails-301217/subscriptions/api-preprocessor-sub-dev'

        pull_timeout_sec
            Seconds the subscriber client will block for messages before retrying.
        """
        self.subscription = subscription
        self.pull_timeout_sec = pull_timeout_sec

        self._client = pubsub_v1.SubscriberClient()

    @dataclass(frozen=True)
    class Message:
        data: bytes
        ack_id: str
        ordering_key: str

    def fetch(self, count: int = 1) -> List[Message]:
        """Fetch a message from the subscription queue.

        This method will hang and wait until a message is available. If an exception is
        raised, it will retry indefinitely.

        Parameters
        -------
        count
            The max number of message to grab in the fetch

        Returns
        -------
        List[Message]
            The dequeued messages from the pubsub subscription.
        """

        resp = self._client.pull(
            request={"subscription": self.subscription, "max_messages": count},
            timeout=self.pull_timeout_sec,  # default: 60
            retry=google.api_core.retry.Retry(
                initial=0.1,  # default: 0.1
                maximum=60.0,  # default: 60
                multiplier=1.3,  # default: 1.3
                predicate=google.api_core.retry.if_exception_type(
                    # Non-default exceptions:
                    google.api_core.exceptions.DeadlineExceeded,
                    # Default exceptions:
                    google.api_core.exceptions.Aborted,
                    google.api_core.exceptions.InternalServerError,
                    google.api_core.exceptions.ServiceUnavailable,
                    google.api_core.exceptions.Unknown,
                ),
                deadline=60.0,  # default: 60
            ),
        )
        messages = []
        for itm in resp.received_messages:
            message = self.Message(
                data=itm.message.data,
                ack_id=itm.ack_id,
                ordering_key=itm.message.ordering_key,
            )
            messages.append(message)
        return messages

    def ack(self, message: Message):
        """Acknowledge the message to remove from the queue."""
        self._client.acknowledge(
            request={"subscription": self.subscription, "ack_ids": [message.ack_id]},
            timeout=30.0,  # default: 60
            retry=google.api_core.retry.Retry(
                initial=0.1,  # default: 0.1
                maximum=60.0,  # default: 60
                multiplier=1.3,  # default: 1.3
                predicate=google.api_core.retry.if_exception_type(
                    # Non-default exceptions:
                    google.api_core.exceptions.DeadlineExceeded,
                    # Default exceptions:
                    google.api_core.exceptions.ServiceUnavailable,
                ),
            ),
        )


class PubSubPublishHandler:
    def __init__(self, topic_id: str, ordered_queue: bool) -> None:
        self._topic_id = topic_id

        self._publisher = pubsub_v1.PublisherClient(
            # Batch settings increase payload size to execute fewer, larger requests.
            # See: https://cloud.google.com/pubsub/docs/batch-messaging
            batch_settings=pubsub_v1.types.BatchSettings(
                max_messages=1000,
                max_bytes=20 * 1000 * 1000,  # 20 MB max server-side request size
                max_latency=1,  # default: 10 ms = 0.01
            ),
            publisher_options=pubsub_v1.types.PublisherOptions(
                enable_message_ordering=ordered_queue,
                # Flow control applies rate limits by blocking any time the staged data
                # exceeds the following settings. Once the records are received by GCP
                # PubSub, additional publish calls are unblocked.
                # See: https://cloud.google.com/pubsub/docs/flow-control-messages
                flow_control=pubsub_v1.types.PublishFlowControl(
                    message_limit=10,
                    byte_limit=1024 * 1024 * 1024,  # 1 GiB
                    limit_exceeded_behavior=pubsub_v1.types.LimitExceededBehavior.BLOCK,
                ),
                # Retry defaults depend on gRPC method, see default for publish here:
                # https://github.com/googleapis/python-pubsub/blob/ff229a5fdd4deaff0ac97c74f313d04b62720ff7/google/pubsub_v1/services/publisher/transports/base.py#L164-L183
                retry=google.api_core.retry.Retry(
                    initial=0.1,
                    maximum=10,
                    multiplier=2,
                    predicate=google.api_core.retry.if_exception_type(
                        google.api_core.exceptions.Aborted,
                        google.api_core.exceptions.Cancelled,
                        google.api_core.exceptions.DeadlineExceeded,
                        google.api_core.exceptions.InternalServerError,
                        google.api_core.exceptions.ResourceExhausted,
                        google.api_core.exceptions.ServiceUnavailable,
                        google.api_core.exceptions.Unknown,
                    ),
                ),
            ),
        )

        self._publish_futures: list[concurrent.futures.Future] = []

    def publish_async(
        self,
        data: bytes,
        timeout_seconds: float,
        ordering_key: str = "",
        log_context: dict[str, Any] | None = None,
    ) -> None:
        """Add data to the current publish batch.

        Batches are pushed asynchronously to GCP PubSub in a separate thread. To wait
        for one or more publish calls until they have been received by the server, call
        wait_for_publish.

        Parameters
        ----------
        data
            byte encoded string payload
        ordering_key
            payloads sharing the same ordering_key are guaranteed to be delivered to
            consumers in the order they are published. the publisher client,
            and the subscription bound to the receiving topic,
            must be configured to use ordered messages.
        timeout_seconds
            timeout applied to each gRPC call to the PubSub API
        ordering_key
            if publishing to an ordered queue, this is the ordering key
        log_context
            any additional k-vs that contextualize the publish event.
            these will be added as context to the publisher callback,
            which includes them in any failure logs.
        """
        future: concurrent.futures.Future = self._publisher.publish(
            topic=self._topic_id,
            data=data,
            ordering_key=ordering_key,
            timeout=timeout_seconds,
        )

        done_callback = self._done_callback_factory(log_context)
        future.add_done_callback(done_callback)
        self._publish_futures.append(future)

    def wait_for_publish(self, timeout_seconds: float | None = None) -> None:
        """Block until all current publish batches are received by server.

        Parameters
        ----------
        timeout_seconds
            Duration to wait for all publish jobs to complete. If timeout_seconds is
            exceeded, the process will be force exited with os._exit(1).
        """
        _, not_done = concurrent.futures.wait(
            self._publish_futures,
            timeout=timeout_seconds,
        )

        # Exit if any publish futures have not completed before configured timeout.
        #
        # We cannot raise an exception or invoke sys.exit from the parent while child
        # threads are still running, because cpython configures a shutdown handler to
        # wait for spawned threads to complete before exiting:
        # https://github.com/python/cpython/blob/8f25cc992021d6ffc62bb110545b97a92f7cb295/Lib/concurrent/futures/thread.py#L18-L37
        #
        # Errors in child threads trigger a separate exit using a future done_callback.
        if not_done:
            print("Futures did not complete before timeout: %s", not_done)
            os._exit(1)

        # All futures completed without error, reset pending futures state.
        self._publish_futures = []

    @staticmethod
    def _done_callback_factory(
        log_context: dict[str, Any] | None,
    ) -> Callable[[concurrent.futures.Future], None]:
        """
        returns a function to use as a callback.
        Constructs a log message annotating with any k-vs passed to this method.
        """
        msg = ""
        if log_context:
            for k, v in log_context.items():
                msg += f" {k}={v} "

        def _exit_on_error(future: concurrent.futures.Future) -> None:
            """Re-raise any exceptions raised by the future's execution thread.

            This should be registered as a callback that will only be invoked when the future
            has already completed using:
                future.add_done_callback(_raise_exception_if_failed)
            """
            try:
                future.result(timeout=0)
            except Exception:
                print(f"Publish future failed: {msg}. Unhandled exception:")
                os._exit(1)

        return _exit_on_error


class FlightsReinjectSvc:
    """
    Service for extracting dead-lettered jobs, and re-injecting them into the worker queue.
    """

    WORKER_JOB_DEAD_LETTER_SUBSCRIPTION = "projects/contrails-301217/subscriptions/prod-mpl-api-preprocessor-sub-dead-letter"
    DEAD_LETTER_ACK_DEADLINE_SEC = 10  # reference subscriber settings
    API_PREPROCESSOR_PARCEL_TOPIC = (
        "projects/contrails-301217/topics/prod-mpl-api-parcel-topic"
    )

    def __init__(
        self,
        count,
        verbose,
        dryrun,
    ):
        self._count: str = count
        self._verbose = verbose
        self._dryrun = dryrun
        self._subscriber_handler = PubSubSubscriptionHandler(
            self.WORKER_JOB_DEAD_LETTER_SUBSCRIPTION,
        )
        self._publish_handler = PubSubPublishHandler(
            self.API_PREPROCESSOR_PARCEL_TOPIC,
            ordered_queue=False,
        )

    def run(self):
        """
        Pulls messages from the dead-letter subscription,
        and dispatches jobs back to the worker queue
        based on the flight_id of the dead-lettered job.
        """
        messages = self._subscriber_handler.fetch(int(self._count))
        start_time = datetime.now()
        print(f"📜 fetched {len(messages)} messages from dead-letter queue.")
        msg: PubSubSubscriptionHandler.Message
        for msg in messages:
            record = ApiPreprocessorJob.from_utf8_json(msg.data)
            print(f"💦 re-injecting job {record}")
            if not self._dryrun:
                self._publish_handler.publish_async(
                    msg.data,
                    timeout_seconds=45,
                )
        if self._dryrun:
            print("🌵dry run... exiting before submission")
            return

        print("⏲️ waiting for publish to finish...")
        self._publish_handler.wait_for_publish(timeout_seconds=300)
        for msg in messages:
            self._subscriber_handler.ack(msg)
        if (datetime.now() - start_time) > timedelta(
            seconds=self.DEAD_LETTER_ACK_DEADLINE_SEC
        ):
            print(
                f"pull to ack period exceeded {self.DEAD_LETTER_ACK_DEADLINE_SEC} seconds."
            )
        print("🙌 DONE!")


if __name__ == "__main__":
    reinject_svc = FlightsReinjectSvc(count=1, verbose=True, dryrun=True)
    reinject_svc.run()
