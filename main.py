import re
from pathlib import Path

import mwclient
import polars as pl


details = pl.read_csv("details.csv")["details"].to_list()
keywords = pl.read_csv("keywords.csv")["keywords"].to_list()


def fetch_card_data(
        wiki: str,
        wiki_category: str,
        tool_name: str,
        tool_version: float,
        contact_information: str,
        output_file: str
):
    """
    Generate a CSV file containing the details of each obtainable TES:L card.

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

    # Aggregate card information
    card_data = pl.concat(
        items = [pl.DataFrame(text_to_dict(page.name, page.text())) for page in site.categories[wiki_category]],
        how="diagonal"
    )

    # Output to a CSV with columns in a specific order
    card_data.select(details + keywords).write_csv(output_file)


def text_to_dict(page_name, page_contents) -> dict:
    """
    Return a dict containing the details of a TES:L card from its wiki page.

    Parameters:
        page_name (str): The name of the card.
        page_contents (str): The contents of the card's wiki page.
    """

    # Capture details from the card summary block and filter out any HTML tags
    data = {
        key: re.sub(r"<.*?>", "", value)
        for key, value in re.findall(r"\|(\w+)=(.*?)\n", page_contents)
    }

    # Add the card's name to the collected card details
    data["name"] = page_name.replace("Legends:", "")

    # Set "availability" for cards from the Core set
    if not data.get("availability"):
        data["availability"] = "Core"

    # Clean "ability" text.
    if data.get("ability"):
        data["ability"] = clean_ability_text(data["ability"])

    # Return a dictionary containing the card's information
    return {key: data[key] for key in details + keywords if key in data}


def clean_ability_text(ability_text: str) -> str:
    # Remove triple single quotes
    ability_text = re.sub(r"'''", "", ability_text)

    # Remove tildes
    ability_text = re.sub(r"~", "", ability_text)

    # Remove &#32;
    ability_text = re.sub(r"&#32;", "", ability_text)

    # Remove . . .
    ability_text = re.sub(r"\.\s*\.\s*\.\s*", "", ability_text)

    # Replace {{LG Attribute Icon|Example}} with Example
    ability_text = re.sub(r"{{LG Attribute Icon\|([^}]+)}}", r"\1", ability_text)

    # Replace [[Legends:Example|Example]] or [[LG:Example|Example]] with Example
    ability_text = re.sub(r"\[\[(?:Legends|LG):[^|]*\|([^]]+)]]", r"\1", ability_text)

    # Replace ExampleExamples with Examples and ExampleExample with Example
    ability_text = re.sub(r"(\b\w+)\1", r"\1", ability_text)

    # Add space between a word ending in a lowercase letter/number and a word starting in an uppercase letter
    ability_text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", ability_text)

    # Add a space after punctuation marks if there isn't one
    ability_text = re.sub(r"([.,;:])(?=\S)", r"\1 ", ability_text)

    # Remove {{New Left}}
    ability_text = re.sub(r"{{New ?Left}}", " ", ability_text)

    return ability_text


if __name__ == "__main__":

    fetch_card_data(
        wiki="en.uesp.net",
        wiki_category="Legends-Cards-Obtainable",
        tool_name="tesl_card_data_fetcher",
        tool_version=0.1,
        contact_information="carter@rineberg.net",
        output_file="tesl_card_data.csv"
    )
