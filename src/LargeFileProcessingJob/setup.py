""" This module sets up the infra needed for the job """

# imports
from sqlalchemy import create_engine
import json


def main():
    with open('config/config.json', 'r') as fp:
        config = json.load(fp)
    engine = create_engine(config['connection_strings']['dummy'])

    engine.execute("""Create Table IF NOT EXISTS products(
                name nvarchar(50),
                sku nvarchar(50) PRIMARY KEY,
                description TEXT)""")

if __name__ == '__main__':
    main()
