import json
import glob
import os
import pandas as pd
import copy
import logging

logger = logging.getLogger(__name__)


class HorizonPool:

    part_columns = [
        "uuid", "MPN", "manufacturer", "description", "value", "datasheet", "parametric", "prefix", "tags", "flags",
        "orderable_MPNs", "base", "entity", "package", "pad_map", "model", "override_prefix", "inherit_tags",
        "inherit_model", "type", "version"
    ]

    class Inheritable:
        def __init__(self, func, *args):
            self.func = func
            self.args = copy.deepcopy(args)

        def __repr__(self):
            return str(self.Value)

        @property
        def Value(self):
            if not hasattr(self, "value"):
                value = self.func(*self.args)
                value = value.Value if type(value) is HorizonPool.Inheritable else value
                self.value = copy.deepcopy(value)
            return self.value

    def __init__(self, path):
        self.path = path

    def get_pool_parts(self, fill_none=True, solve_inheritance=True, expand_flags=True):
        part_file_paths = glob.glob(os.path.join(self.path, "parts", "**", "*.json"), recursive=True)
        parts = HorizonPool.__get_pool_parts(part_file_paths)
        parts = HorizonPool.__fill_none(parts) if fill_none else parts
        parts = HorizonPool.__solve_inheritance(parts) if solve_inheritance else parts

        return parts

    @staticmethod
    def __get_pool_parts(part_file_paths):
        def read_json(file_path):
            with open(file_path) as file:
                return json.load(file)

        pool_parts = pd.DataFrame([read_json(part_file) for part_file in part_file_paths])
        pool_parts = pool_parts.reindex(columns=HorizonPool.part_columns)
        pool_parts = pool_parts.set_index("uuid")
        pool_parts = pool_parts.astype(object).mask(pool_parts.isnull(), None)

        return pool_parts

    @staticmethod
    def __fill_none(parts):
        def __fill_columns(columns, default_value):
            parts.loc[:, columns] = parts[columns].mask(parts[columns].isnull(), default_value)

        __fill_columns("parametric", {})
        __fill_columns("tags", [])
        __fill_columns("flags", {"base_part": "clear", "exclude_bom": "clear", "exclude_pnp": "clear"})
        __fill_columns("orderable_MPNs", {})
        __fill_columns("override_prefix", "no")
        __fill_columns("version", 0.0)

        return parts

    @staticmethod
    def __solve_inheritance(parts):
        def __resolve_columns(columns, solver, ignore_none=False):
            f = lambda part: pd.Series(
                [
                    HorizonPool.Inheritable(solver, col, part) if value is not None or ignore_none == False else None
                    for col, value in part[columns].items()
                ],
                index=columns)

            parts.loc[:, [*columns]] = parts.apply(f, axis="columns")

            return parts

        def __getBaseValue(part, column, default=None):
            if part["base"] is None or part["base"] not in parts.index:
                HorizonPool.__log_part_msg(
                    logging.WARNING,
                    "Part has invalid 'base' attribute, or referenced base-part does not exist in the pool!", ["base"],
                    part)
                return default

            value = parts.loc[part["base"], column]
            return value.Value if type(value) is HorizonPool.Inheritable else value

        columns = ["MPN", "datasheet", "description", "manufacturer", "value"]
        solver = lambda col, part: part[col][1] if part[col][0] == False else __getBaseValue(part, col)
        __resolve_columns(columns, solver, ignore_none=True)

        columns = ["entity", "package", "pad_map"]
        solver = lambda col, part: part[col] if part["base"] is None else __getBaseValue(part, col)
        __resolve_columns(columns, solver)

        columns = ["model"]
        solver = lambda col, part: part[col] if part["inherit_model"] == False else __getBaseValue(part, col)
        __resolve_columns(columns, solver)

        columns = ["prefix"]
        solver = lambda col, part: part[col] if part["override_prefix"] == "yes" else __getBaseValue(part, col) if part[
            "override_prefix"] == "inherit" else None
        __resolve_columns(columns, solver)

        columns = ["tags"]
        solver = lambda col, part: part[col] if part[
            "inherit_tags"] == False else [*__getBaseValue(part, col, default=[]), *(part[col] or [])]
        __resolve_columns(columns, solver)

        columns = ["flags"]
        solver = lambda col, part: {
            key: value if value != "inherit" else __getBaseValue(part, col)[key]
            for (key, value) in part[col].items()
        }
        __resolve_columns(columns, solver, ignore_none=True)

        parts = parts.apply(
            lambda part: pd.Series([cell.Value if type(cell) is HorizonPool.Inheritable else cell for cell in part]))

        return parts

    @staticmethod
    def expand_columns(parts, columns):
        pass

    @staticmethod
    def __log_part_msg(level, msg, columns, part):
        data = {"uuid": part.name, **{col: part[col] for col in columns}}
        logger.log(level, msg + f" {data}")
