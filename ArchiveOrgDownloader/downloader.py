__author__ = 'ernesto'

from json import loads as json_load
import sys
import os
import concurrent.futures
import urllib.request
import urllib.parse
import re
import queue
import argparse

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))
from ArchiveOrgDownloader.ansi_formatter import AnsiColorsFormater


class ArchiveOrgException(Exception):
    def __init__(self, *args, **kwargs):
        super(ArchiveJsonClient).__init__(self, *args, **kwargs)


def listify(gen):
    "Convert a generator into a function which returns a list"

    def patched(*args, **kwargs):
        return list(gen(*args, **kwargs))

    return patched


class Config(object):
    class __Config:
        def __init__(self):
            self.val = None

    instance = None

    def __new__(cls):  # __new__ always a classmethod
        if not Config.instance:
            Config.instance = Config.__Config()
        return Config.instance

    @classmethod
    def get_config(cls):
        return cls.instance

    def __getattr__(self, name):
        return getattr(self.instance, name)

    def __setattr__(self, name, value):
        return setattr(self.instance, name, value)


searchQueue, downloadQueue = queue.Queue(), queue.Queue()
formatter = AnsiColorsFormater()
formatter.enable_type()

DEFAULT_FILE_TYPES = ['3g2', '3gp', '3gp2', '3gpp', 'amv', 'asf', 'avi', 'bin', 'divx', 'drc', 'dv', 'f4v', 'flv',
                      'gxf', 'iso', 'm1v', 'm4v', 'm2t', 'm2v', 'mov', 'mp2', 'mp2v', 'mpa']
DEFAULT_TIMEOUT = 60


class FileDownloader(object):
    def __init__(self):
        self.request = ArchiveOrgRequest()

    def file_exists(self, path):
        return os.path.exists(self.build_path(path))

    def get_file_name_from(self, url):
        return url.split('/')[-1]

    def build_path(self, path):
        config = Config.get_config()
        return "{0}/{1}".format(config.repository_root, path)

    def is_html_file(self, data):
        try:
            return "<html" not in data[:4]
        except TypeError as e:
            return False

    def download_file(self, url):
        file = self.get_file_name_from(url)

        if not self.file_exists(file):
            opener = self.request.get_opener()
            with opener.open(url) as s:
                data = s.read()
                if not self.is_html_file(data):
                    with open(self.build_path(file), "wb") as f:
                        f.write(data)
            formatter.info_message("Wrote {0} to the filesystem".format(file))


class ArchiveOrgRequest(object):
    ARCHIVE_HOST = "https://archive.org/"
    DEFAULT_TIMEOUT = 60

    def __init__(self, host=ARCHIVE_HOST):
        self.host = host
        self.timeout = self.DEFAULT_TIMEOUT
        self.debug = Config.get_config().debug

    def get_opener(self):
        opener = urllib.request.build_opener()
        opener.addheaders = [('User-agent',
                              'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11')]
        return opener

    def get_advanced_search(self, term, rows=1):
        requester = self.get_opener()
        encoding = urllib.parse.urlencode(
            {'q': term, 'mediatype': None, 'rows': rows, 'page': 1, 'output': 'json', 'save': 'no#raw'})
        url = "{0}advancedsearch.php?{1}".format(self.host, encoding)
        if self.debug:
            formatter.debug_message("Call to Advanced search from Archive.org API URL: {0}".format(url))
        with requester.open(url, timeout=self.timeout) as f:
            data = f.read().decode("utf-8")
            data_json = json_load(data)
        return data_json

    def get_details(self, title):
        requester = self.get_opener()
        encoding = urllib.parse.urlencode({'output': 'json', 'callback': 'IAE.favorite'})
        url = "{0}details/{1}&{2}".format(self.host, title, encoding)

        if self.debug:
            formatter.debug_message("Call to details from Archive.org API URI: {0}".format(url))
        with requester.open(url, timeout=self.timeout) as f:
            data = f.read().decode("utf-8")
            data_json = json_load(data[13:-1])
        return data_json


class ArchiveJsonClient(object):
    def __init__(self):
        self.request = ArchiveOrgRequest()

    def get_tiles_and_quantity(self, term):

        json_data = self.request.get_advanced_search(term)
        results_num = json_data['response']['numFound']
        config = Config.get_config()
        if results_num > config.max_results:
            formatter.info_message(
                "For term {0} {1} results were found but the current maximum threshold is {2}. Capping to {3} results"
                    .format(term, results_num, config.max_results, config.max_results))
            results_num = config.max_results

        json_data = self.request.get_advanced_search(term, results_num)
        titles = []

        for i in range(results_num):
            try:
                title = json_data["response"]["docs"][i]["identifier"].replace(" ", "-") if \
                    (" " in json_data['response']['docs'][i]['title']) else json_data["response"]["docs"][i]["title"]
                titles.append(title)
            except Exception as e:
                formatter.warning_message(
                    "Method get_tiles_and_quantity({0}) raised Exception: {1}. Passing for now...".format(term, e))

        return results_num, titles

    def get_urls(self, titles):
        urls = []
        for title in titles:
            try:
                json_data = self.request.get_details(title)
                url = "https://{0}{1}".format(json_data['server'], json_data['dir'])
                urls.append(url)
            except Exception as e:
                formatter.warning_message(
                    "Method get_urls({0}) raised Exception: {1}. Passing for now...".format(title, e))
        return urls

    def get_files_links(self, urls):
        files = {}
        config = Config.get_config()
        debug = config.debug
        for url in urls:
            request = self.request.get_opener()
            with request.open(url) as f:
                data = f.read().decode("utf-8")
                files[url] = re.findall(r'href=[\'"]?([^\'" >]+)', data, re.UNICODE | re.MULTILINE)
                files[url] = files[url][1:]
                if debug:
                    formatter.debug_message("Found files: {0} for urls {1}".format(files, urls))
        return files


DEFAULT_WORKER_QUANTITY = 4
DEFAULT_MAX_RESULTS = 300


class Worker(object):
    def search(self):
        formatter.info_message("Starting search process...")
        item = searchQueue.get()
        config = Config.get_config()
        formatter.info_message("Searching for Term: {0}".format(item))
        if not item == "":
            client = ArchiveJsonClient()

            total_found, titles = client.get_tiles_and_quantity(item)
            formatter.info_message("Found {0} entries for {1}".format(total_found, item))
            formatter.info_message("Found {0} titles for {1}".format(len(titles), item))

            urls = client.get_urls(titles)
            formatter.info_message("Found {0} urls for {1}".format(len(urls), item))
            files = client.get_files_links(urls)
            formatter.info_message("Found {0} files entries for {1}".format(len(files), item))
            total = 0  # total written data
            total_selected = 0
            for url in files.keys():
                total += len(files[url])
                for file in files[url]:
                    extension = get_extension(file)
                    if config.search_by_extension and item in extension:
                        total_selected += 1
                        downloadQueue.put("{0}/{1}".format(url, file))
            formatter.info_message(
                "{0} files for {1} where found, {2} matches and are in the download queue".format(total, item,
                                                                                                  total_selected))

    def download(self):
        formatter.info_message("Starting download process...")
        while True:
            url = downloadQueue.get()
            if not url == "":
                client = FileDownloader()
                client.download_file(url)
            downloadQueue.task_done()


def build_initial_configuration(args, term_collection):
    config = Config()
    config.repository_root = args.rootDirectory
    config.timeout = args.timeout
    config.term_file = args.termFile
    config.verbose = args.verbose
    config.debug = args.debug
    config.term_collection = term_collection
    config.workers = DEFAULT_WORKER_QUANTITY  # TODO: change to a parameter based variable
    config.max_results = DEFAULT_MAX_RESULTS
    config.search_by_extension = args.searchByExtension


get_extension = lambda filename: os.path.splitext(filename)[1]
rel_to_abs_path = lambda rel_path: os.path.abspath(rel_path)


@listify
def get_term_collection(term_file):
    complete_path = rel_to_abs_path(term_file) if not (os.path.isabs(term_file)) else term_file

    if not os.path.exists(complete_path):
        raise ArchiveOrgException("Invalid term file passed as parameter, path used: {0}".format(complete_path()))

    with open(complete_path, 'r') as f:
        data = f.readlines()
        for line in data:
            yield line


def main():
    parser = argparse.ArgumentParser(description="Download files or resources from the Internet Archives API")

    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-d", "--debug", action="store_true")
    parser.add_argument("-s", "--searchByExtension", default=True, action="store_true")
    parser.add_argument("rootDirectory", type=str, help="root directory to store the downloaded resources")
    parser.add_argument("--termFile", type=str, nargs=1, default=None,
                        help="the file containing the term collection to search for.")
    parser.add_argument("--timeout", type=int, nargs=1, default=DEFAULT_TIMEOUT,
                        help="timeout of the request done to archive.org")
    parser.add_argument("--max_results", type=int, nargs=1, default=DEFAULT_MAX_RESULTS,
                        help="Maximum results per file term")
    parser.add_argument("--workers", type=int, nargs=1, default=DEFAULT_WORKER_QUANTITY, help="Worker threshold.")
    args = parser.parse_args()

    term_collection = DEFAULT_FILE_TYPES if args.termFile is None else get_term_collection(args.termFile)

    build_initial_configuration(args, term_collection)

    if args.verbose:
        formatter.enable_timestamp().enable_type()

    max_executors = Config.get_config().workers
    for term in term_collection:
        searchQueue.put(term)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_executors) as executor:
        for i in range(len(term_collection)):
            executor.submit(Worker().search())
            executor(Worker().download())


if __name__ == "__main__":
    main()