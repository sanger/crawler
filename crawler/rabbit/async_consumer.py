import functools
import logging
import os
import ssl

from pika import ConnectionParameters, PlainCredentials, SelectConnection, SSLOptions
from pika.adapters.utils.connection_workflow import (
    AMQPConnectionWorkflowFailed,
    AMQPConnectorPhaseErrorBase,
    AMQPConnectorSocketConnectError,
)
from pika.exceptions import AMQPConnectionError

LOGGER = logging.getLogger(__name__)


class AsyncConsumer(object):
    """This is an async consumer that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.
    If RabbitMQ closes the connection, this class will stop and indicate
    that reconnection is necessary. You should look at the output, as
    there are limited reasons why the connection may be closed, which
    usually are tied to permission related issues or socket timeouts.
    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.
    """

    def __init__(self, server_details, queue, process_message):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.
        :param RabbitServerDetails server_details: The RabbitMQ server connection details.
        :param str queue: The AMQP queue to consume from.
        :param func process_message: A function to call with details of any messages consumed from the queue.
                                     This function will be passed the message headers and the message body and should
                                     return a boolean indicating whether the message was processed successfully (True)
                                     or failed to be processed and should be dead-lettered (False).
        """
        self.should_reconnect = False
        self.was_consuming = False

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._server_details = server_details
        self._queue = queue
        self._process_message = process_message
        self._consuming = False
        # In production, experiment with higher prefetch values
        # for higher consumer throughput
        self._prefetch_count = 1

    @property
    def is_healthy(self):
        return self._consuming or self.should_reconnect

    @staticmethod
    def _reap_last_connection_workflow_error(error):
        """Extract exception value from the last connection attempt

        :param Exception error: error passed by the `AMQPConnectionWorkflow`
            completion callback.

        :returns: Exception value from the last connection attempt
        :rtype: Exception
        """
        if isinstance(error, AMQPConnectionWorkflowFailed):
            # Extract exception value from the last connection attempt
            error = error.exceptions[-1]
            if isinstance(error, AMQPConnectorSocketConnectError):
                error = AMQPConnectionError(error)
            elif isinstance(error, AMQPConnectorPhaseErrorBase):
                error = error.exception

        return error

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.
        :rtype: pika.SelectConnection
        """
        LOGGER.info("Connecting to %s", self._server_details.host)
        credentials = PlainCredentials(self._server_details.username, self._server_details.password)
        connection_params = ConnectionParameters(
            host=self._server_details.host,
            port=self._server_details.port,
            virtual_host=self._server_details.vhost,
            credentials=credentials,
        )
        if self._server_details.uses_ssl:
            cafile = os.getenv("REQUESTS_CA_BUNDLE")
            ssl_context = ssl.create_default_context(cafile=cafile)
            connection_params.ssl_options = SSLOptions(ssl_context)

        return SelectConnection(
            parameters=connection_params,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed,
        )

    def close_connection(self):
        self._consuming = False
        if not self._connection or self._connection.is_closing or self._connection.is_closed:
            LOGGER.info("Connection is closing or already closed")
        else:
            LOGGER.info("Closing connection")
            self._connection.close()

    def on_connection_open(self, _unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.
        :param pika.SelectConnection _unused_connection: The connection
        """
        LOGGER.info("Connection opened")
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.
        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error
        """
        # Note that err is likely to be an AMQPConnectionWorkflowFailed error.  Unfortunatley this means the actual
        # cause of the error is wrapped a few layers deep and the type of err does not generate a useful string
        # description.  As such, we'll use a static method pulled from Pika source code to extract the description.
        LOGGER.error("Connection open failed: %s", AsyncConsumer._reap_last_connection_workflow_error(err))
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.
        :param pika.connection.Connection connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.
        """
        self._channel = None
        if self._connection and self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning("Connection closed, reconnect necessary: %s", reason)
            self.reconnect()

    def reconnect(self):
        """Will be invoked if the connection can't be opened or is
        closed. Indicates that a reconnect is necessary then stops the
        ioloop.
        """
        self.should_reconnect = True
        self.stop()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.
        """
        if self._connection:
            LOGGER.info("Creating a new channel")
            self._connection.channel(on_open_callback=self.on_channel_open)
        else:
            LOGGER.error("No connection to open channel with")

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.
        Since the channel is now open, we'll set the QoS before starting consuming.
        :param pika.channel.Channel channel: The channel object
        """
        LOGGER.info("Channel opened")
        self._channel = channel
        self.add_on_channel_close_callback()
        self.set_qos()

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.
        """
        if self._channel:
            LOGGER.info("Adding channel close callback")
            self._channel.add_on_close_callback(self.on_channel_closed)
        else:
            LOGGER.error("No channel to add close callback to")

    def on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.
        :param pika.channel.Channel: The closed channel
        :param Exception reason: why the channel was closed
        """
        LOGGER.warning("Channel %i was closed: %s", channel, reason)
        self.close_connection()

    def set_qos(self):
        """This method sets up the consumer prefetch to only be delivered
        one message at a time. The consumer must acknowledge this message
        before RabbitMQ will deliver another one. You should experiment
        with different prefetch values to achieve desired performance.
        """
        if self._channel:
            self._channel.basic_qos(prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame):
        """Invoked by pika when the Basic.QoS method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.
        :param pika.frame.Method _unused_frame: The Basic.QosOk response frame
        """
        LOGGER.info("QOS set to: %d", self._prefetch_count)
        self.start_consuming()

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.
        """
        if self._channel:
            LOGGER.info("Issuing consumer related RPC commands")
            self.add_on_cancel_callback()
            self._consumer_tag = self._channel.basic_consume(self._queue, self.on_message)
            self.was_consuming = True
            self._consuming = True
        else:
            LOGGER.error("No channel to consume from")

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.
        """
        if self._channel:
            LOGGER.info("Adding consumer cancellation callback")
            self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
        else:
            LOGGER.error("No channel to add cancel callback to")

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.
        :param pika.frame.Method method_frame: The Basic.Cancel frame
        """
        LOGGER.info("Consumer was cancelled remotely, shutting down: %r", method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.
        :param pika.channel.Channel channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param bytes body: The message body
        """
        LOGGER.info("Received message # %s from %s", basic_deliver.delivery_tag, properties.app_id)
        delivery_tag = basic_deliver.delivery_tag

        if self._process_message(properties.headers, body):
            LOGGER.info("Acknowledging message %s", delivery_tag)
            channel.basic_ack(delivery_tag)
        else:
            LOGGER.info("Rejecting message %s", delivery_tag)
            channel.basic_nack(delivery_tag, requeue=False)

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.
        """
        if self._channel:
            LOGGER.info("Sending a Basic.Cancel RPC command to RabbitMQ")
            cb = functools.partial(self.on_cancelok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)

    def on_cancelok(self, _unused_frame, userdata):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.
        :param pika.frame.Method _unused_frame: The Basic.CancelOk frame
        :param str|unicode userdata: Extra user data (consumer tag)
        """
        self._consuming = False
        LOGGER.info("RabbitMQ acknowledged the cancellation of the consumer: %s", userdata)
        self.close_channel()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.
        """
        if self._channel:
            LOGGER.info("Closing the channel")
            self._channel.close()
        else:
            LOGGER.error("No channel to close")

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.
        """
        self._connection = self.connect()
        if self._connection:
            self._connection.ioloop.start()
        else:
            LOGGER.error("Connection was not established to start the IOLoop on")

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.
        """
        if not self._closing:
            self._closing = True
            LOGGER.info("Stopping")
            if self._consuming:
                self.stop_consuming()
                if self._connection:
                    self._connection.ioloop.start()
            elif self._connection:
                self._connection.ioloop.stop()
            LOGGER.info("Stopped")
