#!/usr/bin/env python

import argparse
import enum
import logging
import os
import importlib
import sys

from corm import register_table
from corm.etl.utils import generate_sqlalchemy_metadata, generate_sqlalchemy_table, sync_sqlalchemy_schema, migrate_data_to_sqlalchemy_table

logger = logging.getLogger(__name__)

def obtain_options() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--tables', type=str, required=True)
    options = parser.parse_args()
    options.tables = [tbl.strip() for tbl in options.tables.split(',')]
    return options

def main() -> None:
    options = obtain_options()
    paired_tables = []
    sql_metadata = generate_sqlalchemy_metadata()
    for table in options.tables:
        module_path, object_name = table.rsplit('.', 1)
        module = importlib.import_module(module_path)
        corm_table = getattr(module, object_name)
        register_table(corm_table)
        sql_table = generate_sqlalchemy_table(corm_table, sql_metadata)
        paired_tables.append((corm_table, sql_table))

    sync_sqlalchemy_schema(sql_metadata)
    for corm_table, sql_table in paired_tables:
        logger.info(f'Migrating Table {corm_table._corm_details.table_name} to PostgreSQL')
        migrate_data_to_sqlalchemy_table(corm_table, sql_table)

def run_from_cli():
    sys.path.append(os.getcwd())
    main()
