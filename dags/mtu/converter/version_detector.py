from mtu.converter.readers.new_reader import V3Reader
from mtu.converter.readers.old_reader import V2Reader


def check_version(input_data: dict):
    """
    Determines the version of the EDGE JSON data format.

    Checks for the presence of the 'Footer' key to distinguish between V2 and V3 formats.

    Args:
        input_data (dict): The JSON data to inspect.

    Returns:
        str: The version string, either "v2" or "v3".
    """

    if "Footer" in input_data.keys():
        return "v3"
    else:
        return "v2"
    return "?"


def create_reader(input_data: dict):
    """
    Instantiates the appropriate reader class based on the JSON version.

    Uses `check_version` to determine whether the data follows V2 or V3 format,
    and returns the corresponding reader instance.

    Args:
        input_data (dict): The JSON data to inspect.

    Returns:
        BaseReader: An instance of either V2Reader or V3Reader.

    Raises:
        TypeError: If the version is not recognized as "v2" or "v3".
    """

    version = check_version(input_data)
    if version == "v2":
        return V2Reader()
    elif version == "v3":
        return V3Reader()
    else:
        raise TypeError("The JSON readers only support EDGE Data of versions v2 or v3.")


if __name__ == "__main__":
    print("Test")
  