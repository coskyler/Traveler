from pydantic import BaseModel

class OperatorInfo(BaseModel):
    name: str
    country: str
    city: str = ""
    url: str = ""

class Profile(BaseModel):
    operator_name: str
    operator_country: str | None = None
    operator_city: str | None = None

    profile_type: str
    role: str | None = None
    profile_name: str | None = None
    email: str | None = None
    phone: str | None = None
    whatsapp: str | None = None

class FetchResult(BaseModel):
    ok: bool
    url: str | None = None
    text: str | None = None
    message: str | None = None

class ParseResult(BaseModel):
    ok: bool
    hyperlink_key_text: str | None = None
    parsed_text: str | None = None
    emails: list[str] | None = None
    phones: list[str] | None = None
    socials: dict[str, str] | None = None # facebook, instagram, youtube, tiktok, x, tripadvisor
    message: str | None = None

class ClassifyResult(BaseModel):
    ok: bool
    operator_type: str | None = None
    business_type: str | None = None
    experience_type: str | None = None
    booking_method: str | None = None
    operating_scope: str | None = None
    final_url: str | None = None
    follow_booking: str | None = None
    follow_contact: str | None = None
    profiles: list[Profile] | None = None
    message: str | None = None
    input_tokens: int = 0
    cached_input_tokens: int = 0
    output_tokens: int = 0
    searched: bool = False

class SearchResult(BaseModel):
    ok: bool
    url: str | None = None
    message: str | None = None