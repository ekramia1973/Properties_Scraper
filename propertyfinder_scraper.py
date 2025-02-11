import asyncio
from curl_cffi.requests import AsyncSession, exceptions
import json
import pandas as pd
import pathlib
from parsel import Selector
from w3lib.html import remove_tags, replace_escape_chars
from html import unescape
from typing import List
from urllib.parse import urlparse
import time
from datetime import datetime
from property_classes import Listing, DetailListing


def cleanup(input_text):
    return unescape(remove_tags(replace_escape_chars(input_text)))


def get_tld(url):
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    tld = domain.rsplit(".", 1)[-1]
    return tld


def make_header(url):
    tld = get_tld(url)
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9",
        f"referer": f"https://www.propertyfinder.{tld}/",
        "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    }
    return headers


async def fetch(session, url):
    header = make_header(url)
    retries = 5
    for attempt in range(retries):
        try:
            response = await session.get(url, headers=header)
            response.raise_for_status()
            return response.text
        except exceptions.HTTPError as e:
            print(f"HTTP error {e.response} for {url}, retrying...")
        except exceptions.Timeout:
            print("The request timed out. Retrying...")
        except exceptions.RequestException as e:
            print(f"Request error {e} for {url}, retrying...")
        await asyncio.sleep(2 * (attempt + 1))
    return None


async def fetch_listing_details(session, url):
    page_content = await fetch(session, url)
    if not page_content:
        print(f"Warning! Failed to get page content for {url}. Skipping the record...")
        return None

    selector = Selector(text=page_content)
    details_script_json = selector.css("script[id='__NEXT_DATA__'] ::text").get("")
    if not details_script_json:
        return None

    try:
        details_script_dict = json.loads(details_script_json)
    except json.JSONDecodeError:
        print(f"Error decoding JSON for {url}")
        return None

    propertyResult = details_script_dict["props"]["pageProps"]["propertyResult"]["property"]
    return DetailListing(
        id=propertyResult["id"],
        property_type=propertyResult["property_type"].strip(),
        price=f'{str(propertyResult["price"]["value"])} {propertyResult["price"]["currency"]}',
        ad_title=cleanup(propertyResult["title"]),
        location_description=cleanup(propertyResult["location"]["full_name"]),
        location_coordinates_lat_lon=", ".join([
            str(propertyResult["location"]["coordinates"]["lat"]),
            str(propertyResult["location"]["coordinates"]["lon"])
        ]),
        images=", ".join([image["full"] for image in propertyResult["images"]["property"]]),
        agent_name=propertyResult["agent"]["name"],
        agent_email=propertyResult["agent"]["email"],
        agent_social=propertyResult["agent"]["social"],
        agent_languages=", ".join(propertyResult["agent"]["languages"]),
        broker_name=propertyResult["broker"]["name"],
        broker_address=cleanup(propertyResult["broker"]["address"]),
        broker_email=propertyResult["broker"]["email"],
        broker_phone=propertyResult["broker"]["phone"],
        broker_logo=propertyResult["broker"]["logo"],
        is_verified=propertyResult["is_verified"],
        is_direct_from_developer=propertyResult["is_direct_from_developer"],
        is_new_construction=propertyResult["is_new_construction"],
        is_available=propertyResult["is_available"],
        is_new_insert=propertyResult["is_new_insert"],
        live_viewing=propertyResult["live_viewing"],
        bedrooms=propertyResult["bedrooms"],
        bathrooms=propertyResult["bathrooms"],
        size=f'{str(propertyResult["size"]["value"])} {propertyResult["size"]["unit"]}',
        share_url=propertyResult["share_url"],
        reference=propertyResult["reference"],
        listed_date=propertyResult["listed_date"],
        contact_options=", ".join(f"'{k}': '{v}'" for k, v in {
            contact["type"]: contact["value"] for contact in propertyResult["contact_options"]
        }.items()),
        images_count=propertyResult["images_count"],
        project=propertyResult.get("project"),
        completion_status=propertyResult["completion_status"],
        furnished=propertyResult["furnished"],
        view_360=propertyResult["view_360"],
        offering_type=propertyResult["offering_type"],
        video_id=propertyResult["video_id"],
        is_under_offer_by_competitor=propertyResult["is_under_offer_by_competitor"],
        description=cleanup(propertyResult["description"]),
        amenities=", ".join([amenity["name"] for amenity in propertyResult["amenities"]]),
    )


async def process_url(session, url, file_name):
    current_page = 1
    all_details = []
    domain = url.split("/")[2]
    
    while True:
        page_url = f"{url}&page={current_page}"
        page_content = await fetch(session, page_url)
        if not page_content:
            break

        selector = Selector(text=page_content)
        script_json = selector.css("script[id='__NEXT_DATA__'] ::text").get()
        if not script_json:
            break

        try:
            script_dict = json.loads(script_json)
        except json.JSONDecodeError:
            break

        search_results = script_dict["props"]["pageProps"]["searchResult"]
        page_count = search_results["meta"]["page_count"]
        listings = search_results["listings"]
        
        print(f"Processing page {current_page} of {page_count} pages of results from {domain}.")
        
        listing_classes = [
            Listing(id=_listing["property"]["id"], share_url=_listing["property"]["share_url"])
            for _listing in listings
        ]

        detail_tasks = [fetch_listing_details(session, listing.share_url) for listing in listing_classes]
        details_results = await asyncio.gather(*detail_tasks)
        details_results = [d for d in details_results if d is not None]

        data_to_append = [details.model_dump(exclude={
            "share_url", "reference", "video_id", "live_viewing",
            "listed_date", "project", "is_under_offer_by_competitor"
        }) for details in details_results]

        all_details.extend(data_to_append)

        current_page += 1
        if current_page > page_count:
            break

    df = pd.DataFrame(all_details)
    df.drop_duplicates(inplace=True)
    df.columns = [column_name.upper() for column_name in df.columns]
    df.to_csv(file_name, mode="w", header=True, index=False, encoding="utf-8")


async def main(urls: List[str]):
    start_time = time.perf_counter()
    print(f"Starting at {datetime.now().strftime('%H:%M:%S')}")

    async with AsyncSession(timeout=30) as session:
        tasks = [process_url(session, url, f"{get_tld(url)}_database.csv") for url in urls]
        await asyncio.gather(*tasks)

    print(f"Completed in {(time.perf_counter() - start_time):.4f} seconds.")


urls = [
    "https://www.propertyfinder.ae/en/search?l=50&c=1&t=1&bdr%5B%5D=3&btr%5B%5D=4&fu=0&ob=mr",
    "https://www.propertyfinder.qa/en/search?l=9&c=1&t=1&bdr[]=3&btr[]=3&fu=0&ob=mr",
    "https://www.propertyfinder.sa/en/search?l=4&c=1&t=1&bdr[]=2&bdr[]=3&btr[]=1&btr[]=2&fu=0&ob=mr",
    "https://www.propertyfinder.bh/en/search?l=34&c=1&t=1&bdr[]=2&btr[]=1&btr[]=2&fu=0&ob=mr",
    "https://www.propertyfinder.eg/en/search?l=2255&c=1&t=1&bdr[]=4&btr[]=4&fu=0&am[]=VW&ob=mr"
]

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(main(urls))
