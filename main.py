import re
from pathlib import Path

import mwclient
import polars as pl
from bs4 import BeautifulSoup


def fetch_card_data(
        wiki: str,
        wiki_category: str,
        tool_name: str,
        tool_version: float,
        contact_information: str,
        output_file: str
):
    """
    Fetch all pages in a category for a given site.

    Parameters:
        wiki (str): The URI of the wiki to fetch pages from. Do not include "HTTPS://", "/w/", etc.
        wiki_category (str): The wiki category to fetch pages from. Do not include "Category:".

        The following parameters are required for all requests by the Wikimedia User-Agent Policy.
        tool_name (str): Unique or semi-unique script (bot) identifier.
        tool_version (float): The version of the script (bot).
        contact_information (str): One or more contact methods, delineated by semicolons.
            e.g. a userpage on the local wiki,
            a userpage on a related wiki using interwiki linking syntax,
            a URI for a relevant external website,
            and/or an email address.

        output_file (str): The path the fetched pages should be written to.
    """

    user_agent = f"{tool_name}/{tool_version} ({contact_information})"
    site = mwclient.Site(host=wiki, clients_useragent=user_agent)

    # Check if card data has already been collected.
    if Path(output_file).is_file():
        card_data = pl.read_csv(output_file, missing_utf8_is_empty_string=True)
    else:
        card_data = pl.DataFrame()

    # Only fetch data if there is a mismatch between the catalogued cards and the pages on the wiki.
    card_names = set(card_data.get_column(name="name", default=pl.Series("name", [""])).to_list())
    page_names = {page.name for page in site.categories[wiki_category]}

    if card_names != page_names:
        card_data = pl.concat(
            items = [pl.DataFrame(text_to_dict(page.name, page.text())) for page in site.categories[wiki_category]],
            how="diagonal"
        )
        card_data.write_csv(output_file)


def text_to_dict(page_name, page_contents) -> dict:
    """
    Return a dict containing the attributes of a TES:L card, given the contents its wiki page (as text).
    """

    # Capture details from the card summary block. Filter out any HTML tags.
    data = {
        key: BeautifulSoup(value, "lxml").text
        for key, value in re.findall(r'\|(\w+)=(.*?)\n', page_contents)
    }

    # Add the card's name to the collected card details.
    data["name"] = page_name.replace("Legends:", "")

    # Set "availability" for cards from the Core set.
    if not data.get("availability"):
        data["availability"] = "Core"

    # Return a dictionary containing the desired card details.
    details = [
        "name", "availability", "deckcode", "type", "attribute", "class", "ability", "cost", "rarity", "image"
    ]

    keywords = [
        "activate", "asilence", "assemble", "banish", "battle", "beast form", "betray", "breakthrough", "change",
        "charge", "consume", "copy", "cover", "drain", "empower", "equip", "exalt", "expertise", "guard", "heal",
        "indestructible", "invade", "last gasp", "lethal", "mobilize", "move", "pilfer", "plot", "prophecy",
        "rally", "regenerate", "sacrifice", "shackle", "shout", "silence", "slay", "steal", "summon", "transform",
        "treasure hunt", "uniqueability", "unsummon", "veteran", "ward", "wax and wane", "wounded"
    ]

    return {key: data[key] for key in details + keywords if key in data}


if __name__ == "__main__":

    fetch_card_data(
        wiki="en.uesp.net",
        wiki_category="Legends-Cards-Obtainable",
        tool_name="tesl_card_data_fetcher",
        tool_version=0.1,
        contact_information="carter@rineberg.net",
        output_file="tesl_card_data.csv"
    )
