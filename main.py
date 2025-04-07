from pathlib import Path
from subprocess import run

import duckdb
from dotenv import load_dotenv

load_dotenv(override=True)

cwd = Path(__file__).parent
input_path = cwd / "inputs"
input_path.mkdir(exist_ok=True, parents=True)
output_path = cwd / "outputs"
output_path.mkdir(exist_ok=True, parents=True)


def duckdb_split(dataset_path: Path, category: str, boundary_path: Path) -> None:
    """Filters a GeoParquet file using a GeoJSON file for the geometry."""
    with duckdb.connect() as con:
        con.sql("INSTALL spatial;")
        con.sql("LOAD spatial;")
        query = f"""
            SELECT a.*
            FROM "{dataset_path.resolve()}" AS a,
            "{boundary_path.resolve()}" AS b
            WHERE ST_Intersects(a.geometry, b.geometry);
        """
        output_file = (
            output_path / category / dataset_path.stem / f"{boundary_path.stem}.parquet"
        )
        con.sql(query).to_parquet(str(output_file), compression="zstd")
    run(["ogr2ogr", output_file.with_suffix(".gpkg"), output_file], check=False)
    run(
        [
            "sozip",
            "--junk-paths",
            output_file.with_suffix(".gpkg.zip"),
            output_file.with_suffix(".gpkg"),
        ],
        check=False,
    )
    output_file.with_suffix(".gpkg").unlink(missing_ok=True)


def main() -> None:
    """Splits building footprint dataset by Admin 1.

    Requires 2 types of inputs to work:
    1. building footprints: inputs/buildings/overture.parquet
    2. admin boundaries: inputs/boundaries/*.parquet

    To download overture building footprints:
        overturemaps download --bbox=... -f geoparquet --type=buildings -o output.parquet
    """
    for boundary_file in sorted((input_path / "boundaries").glob("*.parquet")):
        duckdb_split(
            input_path / "buildings/overture.parquet",
            "buildings",
            boundary_file,
        )


if __name__ == "__main__":
    main()
