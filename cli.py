from cmd.utils.db.base import DB_TYPE
from cmd.utils.db.clickhouse import ClickHouseHelper
from cmd.utils.db.rdb import RDBHelper
from collections import namedtuple
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, TypeVar

import click
import inquirer
import yaml
from click import Context
from pydantic import BaseModel, SecretStr

from plugins.dynamic_dag.config.v1 import (
    DataIngestionConfigV1,
    DataIngestionParams,
    YamlConfig,
)
from plugins.helpers.commons import owner_data_engineer

T = TypeVar("T")

TimestampConf = namedtuple(
    "TimestampConf", ["timestamp_columns", "timestamp_column_type"]
)


class InstanceConfig(BaseModel):
    name: str
    host: str
    port: int
    username: str
    password: SecretStr
    conn_id: str = "<TBU>"

    def __str__(self) -> str:
        return self.host


def prompt_choices(name: str, choices: list[T]) -> T:
    questions = [
        inquirer.List(name, message=f"Please choose {name}", choices=choices),
    ]
    return inquirer.prompt(questions)[name]


def prompt_checkbox(name: str, message: str, choices: list[T]) -> list[T]:
    questions = [
        inquirer.Checkbox(name, message, choices),
    ]
    return inquirer.prompt(questions)[name]


def prompt_general_choices(config: dict[str, Any]) -> tuple[str, InstanceConfig]:
    db_type_choices = list(config.keys())
    chosen_db_type = prompt_choices("db_type", db_type_choices)
    instance_choices = [
        InstanceConfig(**params) for params in config[chosen_db_type]["instances"]
    ]
    chosen_instance = prompt_choices("instance", instance_choices)
    return chosen_db_type, chosen_instance


def get_timestamp_conf(db_type: str) -> TimestampConf:
    conf_mapper = dict(
        postgres=TimestampConf(
            timestamp_columns=["updated_at", "created_at"],
            timestamp_column_type="timestamp",
        ),
        mariadb=TimestampConf(
            timestamp_columns=["updated", "created"],
            timestamp_column_type="integer",
        ),
    )
    return conf_mapper[db_type]


def write_to_sql(
    content: str,
    database: Optional[str] = None,
    table: Optional[str] = None,
    path: Optional[str] = None,
) -> str:
    if not path and not (database and table):
        raise ValueError("You must specify either a path or database and table name")
    path = path if path else f"{database}_{table}.sql"
    with open(path, "w") as f:
        f.write(content)
    return path


@click.group()
@click.option("--conf", type=str, default=".cmd.yaml")
@click.pass_context
def cli(ctx: Context, conf: str):
    ctx.ensure_object(dict)
    with open(conf, "r") as f:
        global_config = yaml.safe_load(f)
    ctx.obj["CONFIG"] = global_config


@cli.command("gen-ddl")
@click.option("--output", type=str, default=None)
@click.pass_context
def gen_ddl(ctx: Context, output: Optional[str] = None) -> None:
    chosen_db_type, chosen_instance = prompt_general_choices(
        ctx.obj["CONFIG"]["gen"]["general"]
    )
    instance_conf = chosen_instance.model_dump()
    instance_conf.pop("name")
    database = click.prompt("Please enter a database name", type=str)
    table = click.prompt("Please enter a table name", type=str)
    helper = ClickHouseHelper()  # type: ignore
    ddl = helper.generate_ddl(database=database, table=table)
    file_path = write_to_sql(ddl, database, table, output)
    click.echo(f"DDL generated and saved to: {file_path}")


@cli.command("gen-dynamic")
@click.pass_context
def gen_dynamic(ctx: Context) -> None:
    chosen_db_type, chosen_instance = prompt_general_choices(
        ctx.obj["CONFIG"]["gen"]["general"]
    )
    database = click.prompt("Please enter a database name", type=str)
    options = chosen_instance.model_dump()
    options.pop("name")
    options["password"] = chosen_instance.password.get_secret_value()
    options["db_type"] = DB_TYPE[chosen_db_type.upper()]
    options["database"] = database
    available_tables = RDBHelper.from_options(options).get_tables()
    mode = prompt_choices(
        "Please choose a generation mode",
        ["full (with optional exclusions)", "partial"],
    )
    flag = "generate" if mode == "partial" else "exclude"
    chosen_tables = prompt_checkbox(
        mode, f"Please select tables to {flag}", available_tables
    )
    tables = (
        chosen_tables
        if mode == "partial"
        else set(available_tables) - set(chosen_tables)
    )
    base_conf = dict(
        database=database,
        owner=owner_data_engineer,
        s3_conn_id="s3-conn",
        db_conn_id=chosen_instance.conn_id,
        clickhouse_conn_id="clickhouse_conn",
    )
    timestamp_conf = get_timestamp_conf(chosen_db_type)
    for table in tables:
        yaml_config = YamlConfig(
            version=1,
            config=DataIngestionConfigV1(
                dags=[
                    DataIngestionParams(
                        id=f"insert_{database}_{table}",
                        start_date=datetime.today(),
                        target_type="clickhouse",
                        target_table=table,
                        s3_key=table,
                        catchup=False,
                        source_type=chosen_db_type,
                        source_table=table,
                        timestamp_columns=timestamp_conf.timestamp_columns,
                        timestamp_column_type=timestamp_conf.timestamp_column_type,
                        tasks={
                            "sql_to_s3": {
                                "file_format": "parquet",
                                "replace": True,
                            },
                            "s3_to_clickhouse": {
                                "incremental_method": "upsert",
                                "s3_format": "Parquet",
                                "upsert_key": "id",
                            },
                        },
                    )
                ],
                **base_conf,
            ),
        )
        file_path = Path(
            Path.cwd(),
            "dags/templates/dag/data_ingestion",
            chosen_db_type,
            database,
            f"{table}.yaml",
        )
        yaml_config.to_yaml(file_path)
        click.echo(f"Dynamic DAG configuration generated and saved to: {file_path}")


if __name__ == "__main__":
    cli(obj={})
