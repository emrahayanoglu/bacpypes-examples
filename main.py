from bacpypes_helpers import get_property_value, do_cov_subscription


def simple_get_property_value():
    value = get_property_value("192.168.0.165", "192.168.0.165", "analogValue", 1, "presentValue")
    print("Returned value: " + str(value))


def simple_unconfirmed_cov_request():
    values = do_cov_subscription("192.168.0.165", "192.168.0.165", "analogValue", 1, "presentValue")
    list_of_values = ",".join(values)
    print(f"Returned values: {list_of_values}, Total Number of Values: {len(values)}")


def main():
    simple_get_property_value()
    simple_unconfirmed_cov_request()


if __name__ == "__main__":
    main()
