# pylint: disable=line-too-long

import importlib

def import_external_data(data_source, request_identifier, path):
    try:
        importer = importlib.import_module('passive_data_kit_external_data.importers.' + data_source)

        return importer.import_data(request_identifier, path)
    except ImportError:
        pass
    except AttributeError:
        pass

    return False
