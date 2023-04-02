import sys
import threading

from bacpypes.apdu import ReadPropertyRequest, APDU, ReadPropertyMultipleRequest, SubscribeCOVRequest, SimpleAckPDU
from bacpypes.errors import ExecutionError
from bacpypes.object import get_datatype
from bacpypes.iocb import IOCB

from bacpypes.core import deferred, run, stop
from bacpypes.pdu import Address

from bacpypes.app import BIPSimpleApplication
from bacpypes.local.device import LocalDeviceObject
import concurrent.futures
from typing import Callable, Tuple, Any
import logging
import time

FORMATTER = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

LOGGER = None


def setup_logger() -> logging.Logger:
    global LOGGER
    if LOGGER:
        return LOGGER

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)

    LOGGER = logging.getLogger("BACnetClient")
    LOGGER.setLevel(logging.DEBUG)
    LOGGER.addHandler(console_handler)
    return LOGGER


class IocbHelper(object):
    def __init__(self, iocb: IOCB, request_io: Callable[[IOCB], Any]):
        self._iocb = iocb
        self._request_io = request_io

    def __enter__(self):
        deferred(self._request_io, self._iocb)
        self._iocb.wait()

    def __exit__(self, exc_type, exc_val, exc_tb):
        deferred(stop)


# Define the BACnet client application
class BACnetClient(BIPSimpleApplication):
    def __init__(self, local_address: Address):
        self._logger = setup_logger()
        super(BACnetClient, self).__init__(LocalDeviceObject(
            objectName="BACpypes Client",
            objectIdentifier=("device", 1),
            maxApduLengthAccepted=1024,
            segmentationSupported="segmentedBoth",
            vendorIdentifier=15,
        ), local_address)

    def _init_iocb(self, iocb: IOCB, successful_callback: Callable[[APDU], Any]) -> Any:
        with IocbHelper(iocb, self.request_io):
            if iocb.ioError:
                # do something for success
                self._logger.error(str(iocb.ioError))
            elif iocb.ioResponse:
                return successful_callback(iocb.ioResponse)
            else:
                self._logger.error("something wrong")
            return None

    def _do_read_property(self, read_property_request: ReadPropertyRequest, property_identifier: str):
        def callback(apdu: APDU) -> Any:
            datatype = get_datatype(apdu.objectIdentifier[0], property_identifier)
            self._logger.debug("Data Type: " + str(datatype))
            if not datatype:
                raise TypeError("unknown datatype")

            # special case for array parts, others are managed by cast_out
            value = apdu.propertyValue.cast_out(datatype)
            self._logger.debug("Value: " + str(value))
            return value

        return self._init_iocb(IOCB(read_property_request), callback)

    def _do_read_property_multiple(self, read_property_multiple_request: ReadPropertyMultipleRequest):
        def callback(apdu: APDU) -> Any:
            return None

        return self._init_iocb(IOCB(read_property_multiple_request), callback)

    def make_request_read_property(self, device_address: Address, object_identifier: Tuple[str, int],
                                   property_identifier: str):
        # Create a BACnet ReadPropertyRequest
        request = ReadPropertyRequest(
            objectIdentifier=object_identifier,
            propertyIdentifier=property_identifier,
        )
        request.pduDestination = device_address

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(self._do_read_property, request, property_identifier)
            run()
            return future.result()

    def make_request_read_property_multiple(self, device_address: Address):
        # Create a BACnet ReadPropertyMultipleRequest
        request = ReadPropertyMultipleRequest()
        request.pduDestination = device_address

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(self._do_read_property, request)
            run()
            return future.result()


class SubscriptionContext:
    def __init__(self, address: Address, objid, subscription_context: dict, property_identifier: str,
                 confirmed: bool = False, lifetime: int = 20, proc_id: int = 1000):
        # destination for subscription requests
        self.address = address

        # assign a unique process identifer and keep track of it
        self._subscription_context = subscription_context
        self._property_identifier = property_identifier
        self.subscriberProcessIdentifier = proc_id
        self._subscription_context[self.subscriberProcessIdentifier] = self

        self.monitoredObjectIdentifier = objid
        self.issueConfirmedNotifications = confirmed
        self.lifetime = lifetime
        self._logger = setup_logger()
        self._value_list = []

    def cov_notification(self, apdu):
        self._logger.debug("{} {} changed\n    {}".format(
            apdu.pduSource,
            apdu.monitoredObjectIdentifier,
            ",\n    ".join("{} = {}".format(
                element.propertyIdentifier,
                str(element.value.tagList[0].app_to_object().value),
            ) for element in apdu.listOfValues),
        ))
        for element in apdu.listOfValues:
            if element.propertyIdentifier != self._property_identifier:
                continue
            self._value_list.append(str(element.value.tagList[0].app_to_object().value))

    @property
    def values(self):
        return self._value_list


class SubscribeCOVApplication(BIPSimpleApplication):
    def __init__(self, subscription_context: dict, local_address: Address):
        self._logger = setup_logger()
        self._subscription_context = subscription_context
        super(SubscribeCOVApplication, self).__init__(LocalDeviceObject(
            objectName="BACpypes Client",
            objectIdentifier=("device", 1),
            maxApduLengthAccepted=1024,
            segmentationSupported="segmentedBoth",
            vendorIdentifier=15,
        ), local_address)

    def send_subscription(self, context):
        # build a request
        request = SubscribeCOVRequest(
            subscriberProcessIdentifier=context.subscriberProcessIdentifier,
            monitoredObjectIdentifier=context.monitoredObjectIdentifier,
        )
        request.pduDestination = context.address

        # optional parameters
        if context.issueConfirmedNotifications is not None:
            request.issueConfirmedNotifications = context.issueConfirmedNotifications
        if context.lifetime is not None:
            request.lifetime = context.lifetime

        # make an IOCB
        iocb = IOCB(request)
        self._logger.debug("    - iocb: %r", iocb)

        # callback when it is acknowledged
        iocb.add_callback(self.subscription_acknowledged)

        # give it to the application
        self.request_io(iocb)

    def subscription_acknowledged(self, iocb):
        self._logger.debug("Subscription Acknowledged!")

        # do something for success
        if iocb.ioResponse:
            self._logger.debug("    - response: %r", iocb.ioResponse)

        # do something for error/reject/abort
        if iocb.ioError:
            self._logger.debug("    - error: %r", iocb.ioError)

    def do_ConfirmedCOVNotificationRequest(self, apdu):
        self._logger.debug("do_ConfirmedCOVNotificationRequest %r", apdu)

        # look up the process identifier
        context = self._subscription_context.get(apdu.subscriberProcessIdentifier, None)
        if not context or apdu.pduSource != context.address:
            self._logger.debug("    - no context")

            # this is turned into an ErrorPDU and sent back to the client
            raise ExecutionError('services', 'unknownSubscription')

        # now tell the context object
        context.cov_notification(apdu)

        # success
        response = SimpleAckPDU(context=apdu)
        self._logger.debug("    - simple_ack: %r", response)

        # return the result
        self.response(response)

    def do_UnconfirmedCOVNotificationRequest(self, apdu):
        self._logger.debug("do_UnconfirmedCOVNotificationRequest %r", apdu)

        # look up the process identifier
        context = self._subscription_context.get(apdu.subscriberProcessIdentifier, None)
        if not context or apdu.pduSource != context.address:
            self._logger.debug("    - no context")
            return

        # now tell the context object
        context.cov_notification(apdu)


def run_bacpypes_for_x_seconds(x: int):
    logger = setup_logger()
    start_time = time.time()

    def stop_after_x_seconds():
        logger.debug("Starting the timeout thread")
        while True:
            if time.time() - start_time > (x + 1):
                logger.debug("Stopping the Thread")
                deferred(stop)
                return
            time.sleep(1)

    thread = threading.Thread(target=stop_after_x_seconds)
    thread.start()
    logger.info("Started up Bacpypes")
    run()
    thread.join()
    logger.info("Stopped up Bacpypes")


def get_property_value(local_address: str, device_address: str, object_type: str, object_identifier: int,
                       property_identifier: str) -> Any:
    # Define the BACnet device information
    device_address = Address(device_address)
    local_address = Address("{}/24:47910".format(local_address))
    object_identifier = (object_type, object_identifier)

    client = BACnetClient(local_address)
    return client.make_request_read_property(device_address, object_identifier, property_identifier)


def do_cov_subscription(local_address: str, device_address: str, object_type: str, object_identifier: int,
                        property_identifier: str, confirmed: bool = False, duration: int = 20) -> list[str]:
    logger = setup_logger()
    # Define the BACnet device information
    subscription_context = {}
    device_address = Address(device_address)
    local_address = Address("{}/24:47911".format(local_address))
    object_identifier = (object_type, object_identifier)

    # initialize SubscribeCOVApplication
    client = SubscribeCOVApplication(subscription_context, local_address)
    # initialize a subscription context
    context = SubscriptionContext(device_address, object_identifier, subscription_context, property_identifier,
                                  confirmed, duration)

    logger.info("Running CoV Subscription")
    deferred(client.send_subscription, context)
    run_bacpypes_for_x_seconds(duration)
    logger.info("Finished CoV Subscription")

    return context.values
