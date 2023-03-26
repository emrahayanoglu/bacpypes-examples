import sys

from bacpypes.apdu import ReadPropertyRequest, APDU, ReadPropertyMultipleRequest
from bacpypes.object import get_datatype
from bacpypes.iocb import IOCB

from bacpypes.core import deferred, run, stop
from bacpypes.pdu import Address

from bacpypes.app import BIPSimpleApplication
from bacpypes.local.device import LocalDeviceObject
import concurrent.futures
from typing import Callable, Tuple, Any
import logging

FORMATTER = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def setup_logger() -> logging.Logger:
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)

    logger = logging.getLogger("BACnetClient")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)
    return logger


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
    def __init__(self, local_address):
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


def get_property_value(local_address: str, device_address: str, object_type: str, object_identifier: int,
                       property_identifier: str) -> Any:
    # Define the BACnet device information
    device_address = Address(device_address)
    local_address = Address("{}/24:47909".format(local_address))
    object_identifier = (object_type, object_identifier)

    client = BACnetClient(local_address)
    return client.make_request_read_property(device_address, object_identifier, property_identifier)
