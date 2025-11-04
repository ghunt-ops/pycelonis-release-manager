import warnings


def deprecation_warning(method_name: str, text: str) -> None:
    """Outputs deprecation warning for given method name and text."""
    warnings.warn(
        f"Deprecation\nThe method `{method_name}` has been deprecated and will be removed in future versions.\n{text}"
    )


def deprecation_class_warning(class_name: str, text: str) -> None:
    """Outputs deprecation warning for given class name and text."""
    warnings.warn(
        f"Deprecation\nThe class `{class_name}` has been deprecated and will be removed in future versions.\n{text}"
    )
