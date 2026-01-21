from pydantic import BaseModel

class OperatorInfo(BaseModel):
    name: str
    country: str
    city: str
    url: str

class FetchResult(BaseModel):
    ok: bool
    url: str | None = None
    text: str | None = None
    message: str | None = None

class ParseResult(BaseModel):
    ok: bool
    url: str | None = None
    hyperlink_key_text: str | None = None
    parsed_text: str | None = None
    emails: list[str] | None = None
    phones: list[str] | None = None
    socials: dict[str, str] | None = None # facebook, instagram, youtube, tiktok, x, tripadvisor
    message: str | None = None

class ClassifyResult(BaseModel):
    ok: bool
    category: str | None = None
    sub_category: str | None = None
    booking_method: str | None = None
    message: str | None = None
    