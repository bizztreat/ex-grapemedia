"""GrapeMedia extractor"""

import os
import sys
from logging import getLogger, basicConfig, DEBUG, INFO
from argparse import ArgumentParser
import json
from datetime import datetime, timedelta
import csv
from requests import session


def generate_dates_between(start, end):
    """Generate list of dates in range from start to end

    Arguments:
        start {datetime or str} -- Start datetime or str in format %Y-%m-%d
        end {[type]} -- End datetime or str in format %Y-%m-%d

    Returns:
        list -- List of dates
    """
    if not isinstance(start, datetime):
        start = datetime.strptime(start, "%Y-%m-%d")
    if not isinstance(end, datetime):
        end = datetime.strptime(end, "%Y-%m-%d")
    if start > end:
        _s = start
        start = end
        end = start
    size = (end - start).days
    dates = [end - timedelta(days=i) for i in range(size)]

    return dates


def generate_days_before(end, increment):
    """[summary]

    Arguments:
        end {datetime or str} -- End datetime or str in format %Y-%m-%d
        increment {int} -- Number of days to go back to

    Returns:
        list -- List of dates
    """
    if not isinstance(end, datetime):
        end = datetime.strptime(end, "%Y-%m-%d")
    start = end - timedelta(days=increment)
    return generate_dates_between(start, end)


class Grape:
    """Main abstraction over Grape interface
    """

    def __init__(self, username, password):
        """Initialize class and setup credentials

        Arguments:
            username {str} -- User name
            password {str} -- Password
        """
        self.username = username
        self.password = password
        self.session = None
        self.authenticated = False
        self.token = None
        self.headers = {}

    def authenticate(self):
        """Send login request"""
        if self.session is None:
            self.session = session()
        resp = self.session.post(
            "https://adx.grapemedia.cz/api/account/login",
            json={
                "UserName": self.username,
                "Password": self.password
            }
        )
        resp.raise_for_status()
        body = resp.json()
        if not "Token" in body:
            raise ValueError("Response does not contain 'Token' field.")

        token = body["Token"]
        self.headers["Authorization"] = "Basic {}".format(token)
        self.authenticated = True

    def ensure_authenticated(self):
        """Ensure we are currently authenticated"""
        if not self.authenticated:
            raise PermissionError(
                "Not authenticated. Make sure you called Grape.authenticate first")

    def get_units(self, category, start=None, end=None):
        """Get units for specified category

        Arguments:
            category {str} -- Category name (e.g. ssp, sklik, google)
            start {datetime} -- Start date
            end {datetime} -- End date
        """
        self.ensure_authenticated()

        params = {}

        if start is not None and end is None:
            end = start
        if end is not None and start is None:
            start = end

        if start is not None:
            params["dateFrom"] = start.strftime("%d.%m.%Y")
            params["dateTo"] = start.strftime("%d.%m.%Y")

        resp = self.session.get(
            "https://adx.grapemedia.cz/api/{}/unit/".format(category),
            headers=self.headers,
            params=params
        )

        resp.raise_for_status()

        body = resp.json()
        return body["Rows"]

    def get_unit_details(self, category, unit, start=None, end=None):
        """Gets unit details (day-by-day data) for specified category/unit

        Arguments:
            category {str} -- Category name (e.g. ssp, sklik, google)
            unit {int} -- Unit id
            start {datetime} -- Start date
            end {datetime} -- End date
        """
        self.ensure_authenticated()

        params = {}

        if start is not None and end is None:
            end = start
        if end is not None and start is None:
            start = end

        if start is not None:
            params["dateFrom"] = start.strftime("%d.%m.%Y")
            params["dateTo"] = start.strftime("%d.%m.%Y")

        resp = self.session.get(
            "https://adx.grapemedia.cz/api/{0}/unit/{1}".format(category, unit),
            headers=self.headers,
            params=params
        )

        resp.raise_for_status()

        body = resp.json()
        return body["Rows"]


def main():
    """Fire-up main function"""
    aparser = ArgumentParser()
    aparser.add_argument(
        "--config", default="/data/config.json", help="Configuration file path")
    aparser.add_argument("--debug", default=False,
                         action="store_true", help="Run in high-verbosity mode")
    args = aparser.parse_args(sys.argv[1:])

    basicConfig(
        format="[{asctime}] [{levelname}] [{filename}:{lineno}]: {message}",
        style="{",
        level=DEBUG if args.debug else INFO
    )
    logger = getLogger(__name__)

    logger.debug("Running in debug mode")

    if not os.path.exists(args.config):
        raise FileNotFoundError(
            "Specified configuration file '{}' does not exist.".format(args.config))

    with open(args.config, "r", encoding="utf-8") as conf_file:
        conf = json.load(conf_file)["parameters"]

    ## Incremental / fixed
    if conf["date_type"] == "incremental":
        # Since yesterday, inclusive
        end_date = datetime.now().replace(hour=0, minute=0, second=0,
                                          microsecond=0) - timedelta(days=1)
        dates = generate_days_before(end_date, conf["increment"])
    else:
        dates = generate_dates_between(conf["date_start"], conf["date_end"])

    logger.info(
        "Running in %s date mode, will extract data between: %s -> %s",
        conf["date_type"],
        min(dates).strftime("%Y-%m-%d"),
        max(dates).strftime("%Y-%m-%d")
    )

    logger.info("Initiating Grape connection")
    grape = Grape(conf["username"], conf["#password"])
    grape.authenticate()
    logger.info("Authenticated as user %s", conf["username"])

    available_units = {}
    for category in conf["categories"]:
        logger.info("Get units for %s", category)
        for date in dates:
            units = grape.get_units(
                category,
                date
            )
            if not units:
                continue
            unit_ids = [u["ID"] for u in units]
            if not category in available_units:
                available_units[category] = unit_ids
            else:
                for unit in unit_ids:
                    if not unit in available_units[category]: # Filter our duplicities
                        available_units[category].append(unit)
    output_data = []
    # Extract unit details
    for category in available_units:
        if not available_units[category]:
            logger.info("Skipping %s - no units", category)
            continue
        logger.info("Extracting %s (%d units)", category,
                    len(available_units[category]))
        for unit in available_units[category]:
            for date in dates:
                details = grape.get_unit_details(
                    category,
                    unit,
                    date
                )
                # empty results
                if not details:
                    continue
                output_data.extend(
                    [{"UnitID": unit, "Category": category, **o} for o in details]
                )
    if not output_data:
        logger.warning("Extractor produced no data")
        return
    logger.info("Proceeding to export %d rows of data", len(output_data))
    with open("/data/out/tables/grape.csv", "w", encoding="utf-8") as outfile:
        writer = csv.DictWriter(
            outfile, fieldnames=output_data[0].keys(), dialect=csv.unix_dialect)
        writer.writeheader()
        writer.writerows(output_data)
    logger.info("Finished")


if __name__ == "__main__":
    main()
