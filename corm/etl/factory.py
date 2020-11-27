#!/usr/bin/env python

import argparse
import enum
import logging
import os
import importlib
import sys

from corm import register_table
from corm.constants import CLUSTER_IPS, CLUSTER_PORT
from corm.etl.constants import PSQL_URI
from corm.etl.utils import generate_sqlalchemy_metadata, generate_sqlalchemy_table, sync_sqlalchemy_schema, \
        migrate_data_to_sqlalchemy_table, ConnectionInfo, export_to_csv

logger = logging.getLogger('')
sysHandler = logging.StreamHandler()
sysHandler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(sysHandler)
logger.setLevel(logging.INFO)
logger = logging.getLogger(__name__)

class Mode(enum.Enum):
    CassandraToPostgreSQL = 'cassandra-to-postgresql'
    CassandraToCSV = 'cassandra-to-csv'
    CassandraGenerateEntries = 'cassandra-generate-data'

formatted_modes = ', '.join([val.value for val in Mode.__members__.values()])
def obtain_options() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mode', type=Mode, default=Mode.CassandraToPostgreSQL,
            help=f'Available Modes: {formatted_modes}')
    parser.add_argument('-t', '--tables', type=str, required=True)
    options = parser.parse_args()
    options.tables = [tbl.strip() for tbl in options.tables.split(',')]
    return options

def main() -> None:
    options = obtain_options()
    if options.mode is Mode.CassandraToPostgreSQL:
        from corm import register_table
        from corm.etl.factory_testing.utils import load_table

        cassandra_uri = f'cassandra://{CLUSTER_IPS[0]}:{CLUSTER_PORT}/'
        cassandra_info = ConnectionInfo.From_URI(cassandra_uri)
        psql_info = ConnectionInfo.From_URI(PSQL_URI)

        paired_tables = []
        sql_metadata = generate_sqlalchemy_metadata(psql_info)
        for table_path in options.tables:
            corm_table = load_table(table_path)
            register_table(corm_table)
            sql_table = generate_sqlalchemy_table(corm_table, sql_metadata)
            paired_tables.append((corm_table, sql_table))

        sync_sqlalchemy_schema(sql_metadata)
        for corm_table, sql_table in paired_tables:
            logger.info(f'Migrating Table {corm_table._corm_details.table_name} to PostgreSQL')
            migrate_data_to_sqlalchemy_table(corm_table, sql_table, cassandra_info, psql_info)

    elif options.mode is Mode.CassandraToCSV:
        import tempfile

        from corm import register_table
        from corm.etl.constants import CORM_EXPORT_DIR
        from corm.etl.factory_testing.utils import load_table

        for ip_address in CLUSTER_IPS:
            conn_info = ConnectionInfo.From_URI(f'cassandra://{ip_address}:{CLUSTER_PORT}/')
            for table_path in options.tables:
                table = load_table(table_path)
                register_table(table)
                table_filename = f'{table._corm_details.table_name}.csv'
                table_filepath = os.path.join(CORM_EXPORT_DIR, table._corm_details.keyspace, table_filename)
                table_dirpath = os.path.dirname(table_filepath)
                if not os.path.exists(table_dirpath):
                    os.makedirs(table_dirpath)

                export_to_csv(table, table_filepath, conn_info)
                
    elif options.mode is Mode.CassandraGenerateEntries:
        from corm import register_table, insert, sync_schema
        from corm.models import CORMBase
        from corm.etl.constants import ETL_CLUSTER_URIS
        from corm.etl.utils import cluster_uris_to_parts
        from corm.etl.factory_testing.utils import generate_entries, load_table

        for table_path in options.tables:
            table = load_table(table_path)
            register_table(table)
            sync_schema()
            for entry in generate_entries(table):
                insert([entry])

def run_from_cli():
    sys.path.append(os.getcwd())
    main()

if __name__ == '__main__':
    main()
