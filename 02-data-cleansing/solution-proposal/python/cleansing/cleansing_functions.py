class Cleanser:
    @staticmethod
    def cleanse_source(row_dict):
        source_to_cleanse = None if "site" not in row_dict else row_dict["site"]
        if source_to_cleanse and source_to_cleanse.startswith("www."):
            source_to_cleanse = source_to_cleanse[4:]
        return source_to_cleanse

    @staticmethod
    def cleanse_technical(technical_to_cleanse):
        browser, language = Cleanser._cleanse_browser_and_language(technical_to_cleanse)
        technical_to_cleanse["browser"] = browser
        technical_to_cleanse["lang"] = language

        network = Cleanser._cleanse_network(technical_to_cleanse)
        technical_to_cleanse["network"] = network

        device = Cleanser._cleanse_device(technical_to_cleanse)
        technical_to_cleanse["device"] = device

        return technical_to_cleanse

    @staticmethod
    def _cleanse_browser_and_language(technical_to_cleanse):
        browser = None if "browser" not in technical_to_cleanse else technical_to_cleanse["browser"]
        language = technical_to_cleanse["lang"]
        if Cleanser._is_json_field(browser):
            return browser['name'], browser['language']
        else:
            return browser, language

    @staticmethod
    def _cleanse_network(technical_struct):
        network = None if "network" not in technical_struct else technical_struct["network"]
        if network and Cleanser._is_json_field(network):
            return network['long_name']
        else:
            return network

    @staticmethod
    def _cleanse_device(technical_struct):
        device = None if "device" not in technical_struct else technical_struct["device"]
        if device and Cleanser._is_json_field(device['type']):
            device['type'] = device['type']['name']
        return device

    @staticmethod
    def _is_json_field(field):
        return field and isinstance(field, dict)