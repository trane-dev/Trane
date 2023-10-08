import importlib


def import_or_none(library):
    try:
        return importlib.import_module(library)
    except ImportError:
        return None
