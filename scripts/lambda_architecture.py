"""Explicit architecture on creation; existing functions require a reviewed opt-in."""
import json
import sys


def architecture(config, operation):
    values = config.get('architectures')
    if values is not None and values not in (['x86_64'], ['arm64']):
        raise ValueError('single_supported_architecture_required')
    flag = config.get('update_architecture', False)
    if type(flag) is not bool:
        raise ValueError('architecture_update_flag_must_be_boolean')
    if flag and values is None:
        raise ValueError('explicit_architecture_required')
    if operation not in ('create', 'update'):
        raise ValueError('unknown_architecture_operation')
    return values[0] if values and (operation == 'create' or flag) else ''


if __name__ == '__main__':
    try:
        with open(sys.argv[1]) as stream:config=json.load(stream)
        print(architecture(config, sys.argv[2]))
    except (ValueError, TypeError):
        raise SystemExit('Invalid explicit Lambda architecture configuration') from None
