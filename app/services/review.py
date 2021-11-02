import pandas as pd

from app.database.postgres import Postgres
from app.services.validator import ReviewsValidate
from app.utils.logger import logger


class Review(Postgres):
    def __init__(self):
        super().__init__()
        self.table_name = "reviews"
        self.initialize(
            {
                "table_name": self.table_name,
                "columns": [
                    "id SERIAL PRIMARY KEY",
                    "product_id int REFERENCES products",
                    "asin varchar(20)",
                    "title varchar(500)",
                    "review varchar",
                ],
            }
        )

    def write_reviews_to_database(self):
        data_from_rev = pd.read_csv(f"../file/Reviews.csv")
        data_from_rev = data_from_rev.rename(
            columns={"Asin": "asin", "Title": "title", "Review": "review"}
        )
        products_id = self.get_all_values("products", ["asin", "id"])
        data_from_rev = pd.merge(
            data_from_rev, products_id, how="left", left_on="asin", right_on="asin"
        ).fillna(0)
        data_from_rev = data_from_rev.rename(columns={"id": "product_id"})
        return self.insert(data_from_rev, self.table_name)

    def create_new_review(self, product_id, user_date):
        try:
            validate_date = ReviewsValidate(**user_date).dict()
            self.execute(
                f"insert into {self.table_name} (product_id, title, review, asin) values ({product_id}, '{validate_date['title']}', '{validate_date['review']}', (select asin from products where id={product_id}))"
            )
            return {"message": "Review save to database"}
        except Exception as e:
            logger.error(e)
            return {"error": e}


if __name__ == "__main__":
    Review().write_reviews_to_database()
