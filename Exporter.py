from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.snowflake import Snowflake
from pandas import DataFrame
from os import path

import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_snowflake(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Snowflake warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#snowflake
    """


    database = 'UBER'
    schema = 'DATA_ANALYTICS'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'



    for key,value in df.items():
        table_name = key
        with Snowflake.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
            loader.export(
                DataFrame(value),
                table_name,
                database,
                schema,
                if_exists='replace',  # Specify resolution policy if table already exists
                )
