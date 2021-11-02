import pandas as pd

from app.database.postgres import Postgres
from app.utils.logger import logger


class Product(Postgres):
    def __init__(self):
        super().__init__()
        self.table_name = "products"
        self.table_name_rev = "reviews"

        self.initialize(
            {
                "table_name": self.table_name,
                "columns": [
                    "id SERIAL PRIMARY KEY",
                    "asin varchar(20)",
                    "title varchar(500)",
                ],
            }
        )

    def write_products_to_database(self):
        date_from_prod = pd.read_csv(f"../file/Products.csv")
        date_from_prod = date_from_prod.rename(
            columns={"Title": "title", "Asin": "asin"}
        )
        return self.insert(date_from_prod, self.table_name)

    def return_product(self, id_product):
        try:
            product = list(
                self.execute(
                    f"select * from {self.table_name} p join reviews r on r.product_id = p.id where p.id = {id_product}"
                )
            )
            if len(product) != 0:
                df = pd.DataFrame(product).to_json()
                return df
            return {"message": "Product not found or no reviews. Try another id."}
        except Exception as e:
            logger.error(e)
            return {"error": e}


if __name__ == "__main__":
    Product().write_products_to_database()
