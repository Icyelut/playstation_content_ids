import os
import time
import argparse
import sys
import pathlib
import logging
import logging_tree

import requests
import arrow
'''

made by `Landcross#5410`

modified slightly by mgrandi

'''
SECONDS_BETWEEN_RETRIES = 1
SESSION = requests.session()
BASE_URL = None

CONTAINER_LIST = []
PRODUCT_LIST = []

FILE_BASE = '.'

FILE = None


class ArrowLoggingFormatter(logging.Formatter):
    """ logging.Formatter subclass that uses arrow, that formats the timestamp
    to the local timezone (but its in ISO format)
    """

    def formatTime(self, record, datefmt=None):
        return arrow.get("{}".format(record.created), "X").to("local").isoformat()


def isFileType(strict=True):
    def _isFileType(filePath):
        ''' see if the file path given to us by argparse is a file
        @param filePath - the filepath we get from argparse
        @return the filepath as a pathlib.Path() if it is a file, else we raise a ArgumentTypeError'''

        path_maybe = pathlib.Path(filePath)
        path_resolved = None

        # try and resolve the path
        try:
            path_resolved = path_maybe.resolve(strict=strict).expanduser()

        except Exception as e:
            raise argparse.ArgumentTypeError("Failed to parse `{}` as a path: `{}`".format(filePath, e))

        # double check to see if its a file
        if strict:
            if not path_resolved.is_file():
                raise argparse.ArgumentTypeError("The path `{}` is not a file!".format(path_resolved))

        return path_resolved
    return _isFileType


def make_request(url: str) -> dict:
    logger = logging.getLogger("make_request")

    response = SESSION.get(url)
    tries = 1
    while response.status_code != 200 and tries <= 5:

        logger.warning(f'REQUEST FAILED ({response.status_code}). RETRYING in {SECONDS_BETWEEN_RETRIES}s...')

        time.sleep(SECONDS_BETWEEN_RETRIES)
        response = SESSION.get(url)
        tries += 1

    if tries > 5:
        logger.warning(f'GIVING UP ON {url}')
        return {}

    return response.json()


def parse_result(data: dict, is_product: bool = False) -> None:
    logger = logging.getLogger("parse_result")
    for item in data['data']['relationships']['children']['data']:
        item_type = item['type']
        item_id = item['id']

        if item_type == 'container':
            traverse_container(item_id)
        elif item_type == 'storefront':
            traverse_storefront(item_id)
        elif item_type in ['game', 'film', 'tv-series', 'tv-season']:
            add_product(item_id)
            if not is_product:
                traverse_container(item_id, is_product=True)
        else:
            add_product(item_id)


def traverse_storefront(storefront_id: str) -> None:
    logger = logging.getLogger("traverse_storefront")
    logger.info(f'Found storefront {storefront_id}')

    data = make_request(f'{BASE_URL}/storefront/{storefront_id}')

    if data:
        parse_result(data)
    else:
        logger.warning(f'FAILED TO GET DATA FOR {storefront_id} ({BASE_URL}/storefront/{storefront_id})')


def traverse_container(container_id: str, is_product: bool = False) -> None:
    logger = logging.getLogger("traverse_container")
    logger.info(f'---- Found container {container_id}')

    global CONTAINER_LIST
    if container_id in CONTAINER_LIST:
        return

    current_offset = 0
    page_size = 250

    data = make_request(f'{BASE_URL}/container/{container_id}?size={page_size}&start={current_offset}')
    if data:
        children = data['data']['relationships']['children']['data']

        while len(children) > 0:
            parse_result(data, is_product)

            current_offset += len(children)
            data = make_request(f'{BASE_URL}/container/{container_id}?size={page_size}&start={current_offset}')
            if data:
                children = data['data']['relationships']['children']['data']
            else:
                logger.warning(f'---- FAILED TO GET NEXT CHILDREN FOR CONTAINER {container_id}')
    else:
        logger.warning(f'---- FAILED TO GET CHILDREN FOR CONTAINER {container_id}')

    CONTAINER_LIST.append(container_id)


def add_product(product_id: str) -> None:
    logger = logging.getLogger("add_product")
    logger.info(f'-------- Found product {product_id}')

    global PRODUCT_LIST, FILE
    if product_id not in PRODUCT_LIST:
        PRODUCT_LIST.append(product_id)
        FILE.write(product_id + '\n')


def fetch_product(product_id: str) -> None:
    data = make_request(f'{BASE_URL}/resolve/{product_id}')


def main(args):
    logger = logging.getLogger("main")

    language_code = args.region_language
    country_code = args.region_country

    start_time = arrow.utcnow()

    logger.info("language code: `{}`, country code: `{}`".format(language_code, country_code))
    logger.info("starting at `{}`".format(start_time))

    global BASE_URL, FILE, FILE_BASE, PRODUCT_LIST, CONTAINER_LIST

    FILE_BASE = str(args.output_file_directory)
    BASE_URL = f'https://store.playstation.com/valkyrie-api/{language_code}/{country_code}/999'

    if os.path.exists(os.path.join(FILE_BASE, f'{language_code}-{country_code}.txt')):
        FILE = open(os.path.join(FILE_BASE, f'{language_code}-{country_code}.txt'), 'r+')
        PRODUCT_LIST = FILE.read().splitlines()

        CONTAINER_LIST = PRODUCT_LIST.copy()
        logger.info("opening existing file, got `{}` entries".format(len(PRODUCT_LIST)))

    else:
        FILE = open(os.path.join(FILE_BASE, f'{language_code}-{country_code}.txt'), 'w')

    tmpres = SESSION.post(
        url='https://store.playstation.com/kamaji/api/valkyrie_storefront/00_09_000/user/session',
        data={
            'country_code': country_code.upper(),
            'language_code': language_code,
        }
    )

    session_data = tmpres.json()

    stores_data = SESSION.get(url=session_data['data']['sessionUrl'] + 'user/stores').json()
    base_storefront = stores_data['data']['base_url'].split('/')[-1]

    traverse_storefront(base_storefront)

    FILE.close()


    end_time = arrow.utcnow()
    logger.info("finished at `{}`".format(end_time))

    elapsed_time = end_time - start_time
    logger.info("elapsed time: `{}`".format(elapsed_time))


def isDirectoryType(filePath):
    ''' see if the file path given to us by argparse is a directory
    @param filePath - the filepath we get from argparse
    @return the filepath as a pathlib.Path() if it is a directory, else we raise a ArgumentTypeError'''

    path_maybe = pathlib.Path(filePath)
    path_resolved = None

    # try and resolve the path
    try:
        path_resolved = path_maybe.resolve(strict=True).expanduser()

    except Exception as e:
        raise argparse.ArgumentTypeError("Failed to parse `{}` as a path: `{}`".format(filePath, e))

    # double check to see if its a file
    if not path_resolved.is_dir():
        raise argparse.ArgumentTypeError("The path `{}` is not a directory!".format(path_resolved))

    return path_resolved


if __name__ == "__main__":

    # set up logging stuff
    logging.captureWarnings(True)  # capture warnings with the logging infrastructure
    root_logger = logging.getLogger()
    logging_formatter = ArrowLoggingFormatter("%(asctime)s %(threadName)-10s %(name)-20s %(levelname)-8s: %(message)s")

    parser = argparse.ArgumentParser("old_psn_product_fetcher")

    parser.add_argument("--log-to-file-path", dest="log_to_file_path", type=isFileType(False), help="log to the specified file")
    parser.add_argument("--verbose", action="store_true", help="Increase logging verbosity")
    parser.add_argument("--no-stdout", dest="no_stdout", action="store_true", help="if true, will not log to stdout" )

    parser.add_argument("region_language", help="the region language, aka the `en` in `en-US`")
    parser.add_argument("region_country", help="the region country, aka the `US` in `en-US`")
    parser.add_argument("--output_file_directory", type=isDirectoryType, default=".",
        help="where to write the resulting file to, defaults to current directory")


    parsed_args = parser.parse_args()

    if parsed_args.log_to_file_path:
        file_handler = logging.FileHandler(parsed_args.log_to_file_path, encoding="utf-8")
        file_handler.setFormatter(logging_formatter)
        root_logger.addHandler(file_handler)

    if not parsed_args.no_stdout:
        logging_handler = logging.StreamHandler(sys.stdout)
        logging_handler.setFormatter(logging_formatter)
        root_logger.addHandler(logging_handler)

    # set logging level based on arguments
    if parsed_args.verbose:
        root_logger.setLevel("DEBUG")
    else:
        root_logger.setLevel("INFO")

    root_logger.info("########### STARTING ###########")

    root_logger.debug("Parsed arguments: %s", parsed_args)
    root_logger.debug("Logger hierarchy:\n%s", logging_tree.format.build_description(node=None))


    try:

        main(parsed_args)

    except Exception as e:
        root_logger.exception("something went wrong!")
        sys.exit(1)

    root_logger.info("Done!")

