"""
Mutation testing configuration for mutmut.

Configures which files to mutate and which tests to run.
Optimized to skip low-value mutations for faster execution.
"""


def pre_mutation(context):
    """
    Hook called before each mutation.

    Can be used to skip certain mutations or modify context.
    Optimized to skip low-value code patterns.
    """
    # Skip mutations in test files
    if 'tests/' in context.filename:
        context.skip = True

    # Skip mutations in __init__.py files
    if context.filename.endswith('__init__.py'):
        context.skip = True

    # Skip mutations in migration scripts
    if 'migrations/' in context.filename:
        context.skip = True

    # Skip logging statements (low business value)
    line = context.current_source_line.strip()
    if line.startswith('logger.') or line.startswith('logging.'):
        context.skip = True

    # Skip print statements (typically debug code)
    if line.startswith('print('):
        context.skip = True

    # Skip pass statements
    if line == 'pass':
        context.skip = True

    # Skip docstring mutations (doesn't affect logic)
    if '"""' in line or "'''" in line:
        context.skip = True


def post_mutation(context):
    """
    Hook called after each mutation test.

    Can be used for custom reporting or cleanup.
    """
    pass
