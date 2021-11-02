from pydantic import BaseModel


class ReviewsValidate(BaseModel):
    title: str
    review: str
