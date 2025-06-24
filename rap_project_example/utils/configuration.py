"""Define functions to read configuration settings."""
import toml

def load_schema_from_toml(schema_path):
    raw_schema = toml.load(schema_path)
    type_map = {"str": str, "int": int, "float": float, "bool": bool}
    return {v["old_name"]: type_map[v["Deduced_Data_Type"]] for v in raw_schema.values()}

def validate_schema(df, expected_schema):
    for column, expected_type in expected_schema.items():
        if column not in df.columns:
            raise ValueError(f"Missing expected column: {column}")
        if not df[column].map(type).eq(expected_type).all():
            raise TypeError(f"Column '{column}' does not match expected type {expected_type.__name__}")

