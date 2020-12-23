import importlib
import pkgutil

from django.conf import settings

def pdk_annotate_field(container, field_name=None, field_value=None, context=None): # pylint: disable=unused-argument
    for app in settings.INSTALLED_APPS:
        try:
            annotators = importlib.import_module(app + '.annotators')

            prefix = annotators.__name__ + '.'

            for importer, modname, ispkg in pkgutil.iter_modules(annotators.__path__, prefix): # pylint: disable=unused-variable
                module = __import__(modname, fromlist='dummy')

                annotations = module.annotate(field_value, field_name)

                if annotations is not None:
                    for key in annotations.keys():
                        container[key] = annotations[key]
        except ImportError:
            pass
        except AttributeError:
            pass

def fetch_annotation_fields():
    fields = []

    for app in settings.INSTALLED_APPS:
        try:
            annotators = importlib.import_module(app + '.annotators')

            prefix = annotators.__name__ + '.'

            for importer, modname, ispkg in pkgutil.iter_modules(annotators.__path__, prefix): # pylint: disable=unused-variable
                module = __import__(modname, fromlist='dummy')

                fields.extend(module.fetch_annotation_fields())
        except ImportError:
            pass
        except AttributeError:
            pass

    return fields

def fetch_annotations(properties):
    annotations = {}

    for app in settings.INSTALLED_APPS:
        try:
            annotators = importlib.import_module(app + '.annotators')

            prefix = annotators.__name__ + '.'

            for importer, modname, ispkg in pkgutil.iter_modules(annotators.__path__, prefix): # pylint: disable=unused-variable
                module = __import__(modname, fromlist='dummy')

                new_annotations = module.fetch_annotations(properties)

                if new_annotations is not None:
                    annotations.update(new_annotations)
        except ImportError:
            pass
        except AttributeError:
            pass

    return annotations
