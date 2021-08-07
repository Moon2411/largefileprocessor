import pandas as pd
import json
from sqlalchemy import create_engine
import logging


class IngestionJob:

    def __init__(self) -> None:
        with open('config/config.json', 'r') as fp:
            config = json.load(fp)
        self.engine = create_engine(config['connection_strings']['dummy'])
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel('INFO')
        logging.basicConfig(filename=config['log_path'], format='%(asctime)s - %(levelname)-8s %(message)s', datefmt="%Y-%m-%d %H:%M:%S")

    def __get_existing_products(self, new_skus: list):
        try:
            query = """select * from products where sku in %s""" % str(
                tuple(new_skus))
            self.logger.info("Getting existing data")
            existing_products = pd.read_sql(sql=query, con=self.engine)
            self.logger.info(
                "Existing data fetch successfull. Shape: %s", existing_products.shape)
            return existing_products
        except Exception as err:
            self.logger.exception(
                "Error reading existing data. Message: %s", str(err))
            raise Exception(err)

    def __get_new_data(self, new_products, existing_products):
        try:
            self.logger.info('Finding new data')
            new_data = new_products.loc[~new_products['sku'].isin(
                existing_products['sku'])]
            self.logger.info('New data found. Shape: %s', new_data.shape)
            return new_data
        except Exception as err:
            self.logger.exception(
                "Error getting new data. Message: %s", str(err))
            raise Exception(err)

    def __get_updated_data(self, new_products, existing_products):
        try:
            self.logger.info("Getting updated products")
            existing_records = new_products.loc[new_products['sku'].isin(
                existing_products['sku'])]
            updated_data = new_products.loc[(new_products['name'] != existing_records['name']) | (
                new_products['description'] != existing_records['description'])]
            self.logger.info("Update products found. Shape: %s",
                             updated_data.shape)
            return updated_data
        except Exception as err:
            self.logger.exception(
                "Error getting updated products. Message: %s", str(err))
            raise Exception(err)

    def __append_new_data(self, new_data):
        try:
            self.logger.info('Appending new data')
            new_data.to_sql('products', self.engine,
                            if_exists='append', index=False)
            self.logger.info('New data appended successfully')
            return True
        except Exception as err:
            self.logger.exception(
                'Error appending data. Message: %s', str(err))
            return False

    def __upsert_updated_data(self, updated_data):
        try:
            self.logger.info("Updating records")
            update_query = ""
            for i in range(updated_data.shape[0]):
                update_query += "update products set name = '%s', description = '%s' where sku = '%s';" % (
                    updated_data['name'][i], updated_data['description'][i], updated_data['sku'][i])
            self.engine.execute(update_query)
            self.logger.info('Records updated successfully.')
            return True
        except Exception as err:
            self.logger.exception('Error updating records. Message: %s', str(err))
            return False

    def full_load(self, path):
        try:
            self.logger.info(":::::::::: Full Load Job started ::::::::::")
            self.logger.info("Reading Data")
            products = pd.read_csv(path)
            self.logger.info("Read successfull")

            self.logger.info("Processing Data")
            products.drop_duplicates(subset=['sku'], keep='last', inplace=True)
            self.logger.info(
                "Data process successfull. Shape: %s", products.shape)

            self.logger.info("Inserting data")
            products.to_sql('products', self.engine,
                            if_exists='append', index=False)
            self.logger.info("Data insert successfull")
            return True
        except Exception as err:
            self.logger.exception(
                "Error doing full load. Message: %s", str(err))
            return False
        finally:
            self.logger.info(":::::::::: Full Load Job complete ::::::::::")

    def incremental_load(self, path):
        try:
            self.logger.info(":::::::::: Incremental Job started ::::::::::")
            self.logger.info("Reading New Data")
            new_products = pd.read_csv(path)
            new_products.drop_duplicates(subset=['sku'], keep='last', inplace=True)
            self.logger.info(
                "Data process successfull. Shape: %s", new_products.shape)

            existing_products = self.__get_existing_products(
                new_products['sku'].tolist())
            if existing_products.shape[0]==0:
                return False

            new_data = self.__get_new_data(new_products, existing_products)
            if new_data.shape[0]==0:
                self.logger.info("No new data. Skipping append")
            else:
                append_status = self.__append_new_data(new_data)
                if not append_status:
                    return False

            updated_data = self.__get_updated_data(
                new_products, existing_products)
            if updated_data.shape[0]==0:
                self.logger.info("No updated data. Skipping upsert")
            else:
                update_status = self.__upsert_updated_data(updated_data)
                if not update_status:
                    return False
        except Exception as err:
            self.logger.exception(
                "Error doing incremental load. Message: %s", str(err))
            return False
        finally:
            self.logger.info(":::::::::: Incremental Job complete ::::::::::")
