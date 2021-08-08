from sqlalchemy import create_engine
import logging
import json
from datetime import datetime

class AggregateJob:

    def __init__(self) -> None:
        with open('config/config.json', 'r') as fp:
            config = json.load(fp)
        self.engine = create_engine(config['connection_strings']['dummy'])
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel('INFO')
        logging.basicConfig(filename=config['log_path'], format='%(asctime)s - %(levelname)-8s %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    
    def aggregate(self):
        try:
            self.logger.info(":::::::::: Aggregate Job started ::::::::::")
            timestamp = datetime.strftime(datetime.now(), format='%Y%m%d_%H%M%S')
            self.logger.info("Creating products aggregate table")
            create_query = f"""
            create table product_agg_{timestamp}(
            name nvarchar(50) PRIMARY KEY,
            no_products INT);
            """
            self.engine.execute(create_query)
            self.logger.info("Products aggregate table created successfully")

            self.logger.info("Inserting data into Products aggregate table")
            insert_query = f"""
            insert into product_agg_{timestamp} (name, no_products) Select name, count(1) as no_products from products group by name;"""
            self.engine.execute(insert_query)
            self.logger.info("Data insertion into Products aggregate table successful")
        except Exception as err:
            self.logger.exception(
                "Error doing aggregate load. Message: %s", str(err))
            return False
        finally:
            self.logger.info(":::::::::: Aggregate Job completed ::::::::::")

def main():
    job = AggregateJob().aggregate()

if __name__ == '__main__':
    main()
