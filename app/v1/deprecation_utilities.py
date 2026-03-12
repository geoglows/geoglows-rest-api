DEPRECATION_MESSAGE = "Version 1 of the GEOGLOWS API has been in a legacy state for some time and is no longer recommended. " \
"Users should migrate to Version 2 (see: https://geoglows.ecmwf.int/documentation)."


def add_deprecation_warning_json(json_content):
    if isinstance(json_content, dict):
        response_data = dict(json_content)
        response_data['deprecation_warning'] = DEPRECATION_MESSAGE
        return response_data
    return json_content


def add_deprecation_warning_header(response):
    response.headers['Warning'] = DEPRECATION_MESSAGE
    return response