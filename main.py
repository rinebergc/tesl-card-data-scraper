import fnmatch
import io
import os
import re

import mwclient
import polars as pl
from bs4 import BeautifulSoup
from pathvalidate import sanitize_filepath


class WikiParser:
    @staticmethod
    def fetch_wiki_pages_by_category(
            wiki: str,
            wiki_category: str,
            tool_name: str,
            tool_version: float,
            contact_information: str,
            output_directory: str,
            existing_card_data: str = None
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

            output_directory (str): The directory the fetched pages should be written to.
            existing_card_data (str, optional): A CSV file containing data to count against the count of pages.
        """

        user_agent = f"{tool_name}/{tool_version} ({contact_information})"
        site = mwclient.Site(host=wiki, clients_useragent=user_agent)
        category = site.categories[wiki_category]

        # Load the card data we've already collected, if it exists
        if existing_card_data is not None:
            card_data = pl.scan_csv(
                existing_card_data,
                missing_utf8_is_empty_string=True,
                infer_schema=False
            ).collect()
        else:
            card_data = None

        # Only fetch pages from the wiki if the count of pages does not match the count of card data records
        if card_data is not None and (sum(1 for _ in iter(category)) != card_data.height):
            for page in category:
                file_path = sanitize_filepath(output_directory + page.name.split(":")[1].strip() + ".txt")
                file_contents = page.text().encode("utf8")

                with open(f"{file_path}", "wb") as file:
                    file.write(file_contents)
                    print(f"Success. Saved \"/wiki/{page.name}\" to {file_path}")

    @staticmethod
    def write_to_buffer(file_contents) -> io.BytesIO:
        file_buffer = io.BytesIO()
        file_buffer.write(file_contents)
        file_buffer.seek(0)
        return file_buffer

    @staticmethod
    def text_to_dict(file_contents) -> dict:
        """Extract TES:Legends card data from its wiki text."""

        # Remove ''' (triple single quotes).
        file_contents = re.sub(r"'''", "", file_contents)

        # Replace strings similar to [[Legends:Shackle|Shackled]] with Shackled.
        file_contents = re.sub(r'\[\[[^]|]+\|([^]]+)]]', r'\1', file_contents)

        # Capture attributes in the Legends Card Summary text block, removing any HTML tags that may be present.
        data = {
            key: BeautifulSoup(value, "lxml").text.strip()
            for key, value in re.findall(r'\|(\w+)=(.*?)\n', file_contents)
        }

        # Filter out any incidentally regex-matched data from the text, then return the final attributes as a dictionary.
        details = [
            "availability", "deckcode", "type", "attribute", "ability", "cost", "rarity", "image"
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
    WikiParser.fetch_wiki_pages_by_category(
        wiki="en.uesp.net",
        wiki_category="Legends-Cards-Obtainable",
        tool_name="legends_card_fetcher",
        tool_version=0.1,
        contact_information="carter@rineberg.net",
        output_directory="pages/"
    )
