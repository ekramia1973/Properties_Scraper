from attr import asdict
from pydantic import BaseModel, Field
from decimal import Decimal
from typing import Dict, Union, List
from datetime import datetime

class Listing(BaseModel):
    id: str
    # property_type: str
    # price: Dict[str, Union[int, str]] = Field(default_factory=dict) #Dict[str, Union[Decimal, str]] = Field(default_factory=dict)
    # title: str
    # location: Dict[str, Union[str, List[float]]] = Field(default_factory=dict)
    # images: List[str] = Field(default_factory=list)
    # agent: Dict[str, Union[str, bool, List[str]]] = Field(default_factory=dict)
    # broker: Dict[str, str] = Field(default_factory=dict)
    # is_verified: bool
    # is_direct_from_developer: bool
    # is_new_construction: bool
    # is_available: bool
    # is_new_insert: bool
    # live_viewing: bool | None
    # bedrooms: float
    # bathrooms: float
    # size: Dict[str, Union[float, str]]
    share_url: str
    # reference: str
    # listed_date: datetime
    # contact_options: Dict[str, str]= Field(default_factory=dict)
    # images_count: int
    # completion_status: str
    # furnished: bool
    # has_view_360: bool
    # offering_type: str
    # video_url: str
    # is_under_offer_by_competitor: bool
    # description: str
    # amenity_names: List[str]
    
class DetailListing(BaseModel):
    id: str
    property_type: str
    price: str 
    ad_title: str
    location_description: str
    location_coordinates_lat_lon: str
    images: str | None 
    agent_name: str
    agent_email: str
    agent_social: str
    agent_languages: str
    broker_name: str
    broker_logo: str
    broker_address: str
    broker_email: str
    broker_phone: str
    is_verified: bool
    is_direct_from_developer: bool
    is_new_construction: bool
    is_available: bool
    is_new_insert: bool
    live_viewing: bool | None
    bedrooms: float | str | None
    bathrooms: float | str | None
    size: str
    share_url: str
    reference: str
    listed_date: datetime
    contact_options: str
    images_count: int
    project: Dict | None = Field(default_factory=dict)
    amenities: str | None 
    completion_status: str
    furnished: bool | str
    view_360: str | None
    offering_type: str
    video_id: str | None
    is_under_offer_by_competitor: bool
    description: str