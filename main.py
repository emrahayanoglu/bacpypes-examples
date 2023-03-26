from bacpypes_helpers import get_property_value


def main():
    value = get_property_value("192.168.1.65", "192.168.1.65", "analogValue", 1, "presentValue")
    print("Returned value: " + str(value))


if __name__ == "__main__":
    main()
