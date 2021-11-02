from flask import Flask
from flask import request

from app.services.product import Product
from app.services.review import Review

application = Flask(__name__)


@application.route("/products/<product_id>", methods=["GET"])
def return_product(product_id):
    product = Product().return_product(product_id)
    return f"{product}"


@application.route("/products/<product_id>/reviews", methods=["PUT"])
def save_product(product_id):
    user_date = request.get_json()
    dict_ = Review().create_new_review(product_id, user_date)
    return dict_


if __name__ == "__main__":
    application.run(host="0.0.0.0", port=8080)
