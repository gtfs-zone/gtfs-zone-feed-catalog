"""Dagster assets, one module each.

Deliberately empty of imports. Re-exporting the asset objects here would shadow
the submodule names on the package, and then anything addressing a module by its
dotted path - a test patching `geometry_car.assets.check_history`, for one -
would get the asset object instead of the module. `definitions.py` collects them
by walking the package.
"""
