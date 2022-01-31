import asyncio
import cloudscraper
import math
import numpy as np
import platform
import sys
import time
import traceback as tb

from aiohttp import ClientResponse, ClientSession, TCPConnector
from aiohttp.client_exceptions import ClientOSError, ClientResponseError
from aiohttp.hdrs import CONTENT_TYPE
from asyncio import CancelledError
from collections import namedtuple

from log_utils import log_first_call
from stage2_extractor import Stage2Extractor

Context = namedtuple('Context', ['address', 'city_or_county', 'state'])

def _compute_wait(average_wait, rng_base):
    log_first_call()
    log_average_wait = math.log(average_wait, rng_base)
    fuzz = np.random.standard_normal(size=1)[0]
    return int(np.ceil(rng_base ** (log_average_wait + fuzz)))

def _status_from_exception(exc):
    if isinstance(exc, CancelledError):
        return '<canceled>'
    if isinstance(exc, ClientOSError) and platform.system() == 'Windows' and exc.errno == 10054:
        # WinError: An existing connection was forcibly closed by the remote host
        return '<conn closed>'
    if isinstance(exc, asyncio.TimeoutError):
        return '<timed out>'

    return ''

class Stage2Session(object):
    def __init__(self, **kwargs):
        self._extractor = Stage2Extractor()
        self._conn_options = kwargs

    async def __aenter__(self):
        conn = TCPConnector(**self._conn_options)
        self._sess = await ClientSession(connector=conn).__aenter__()
        return self

    async def __aexit__(self, type, value, tb):
        await self._sess.__aexit__(type, value, tb)

    def _log_retry(self, url, status, retry_wait):
        print("GET request to {} failed with status {}. Trying again in {}s...".format(url, status, retry_wait), file=sys.stderr)

    def _log_extraction_failed(self, url):
        print("ERROR! Extraction failed for the following url: {}".format(url), file=sys.stderr)

    async def _get(self, url, average_wait=10, rng_base=2):
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36',
            'cookie': 'cf_chl_2=06312e3a7700449; cf_chl_prog=x12; cf_clearance=5TsyRj6zU.EiXG.TuMJ92HjMexeXFP_U5bRxbEi0s.w-1643250104-0-150; has_js=1; _ga=GA1.2.1680382360.1643250106; _gid=GA1.2.44978531.1643250106; __cf_bm=Zj7jNdFAvLIiUvDiyvJn_lDu7a7hxq.Lu2mdla8wmS0-1643250106-0-AXy7mop3bR/JyyiLUdD89AdI2XTcoL/wYo0ACGAtnBeKd688E+5WM4BP8I30TF3emcFWnoxxh3lj+M900c+hy5SX67XowbdtvNxHSpypj/koGzVoPlPD+TqamazC2BPKfA==; __atssc=internetarchive;1; __atuvc=2|4; __atuvs=61f201ba16039189001',
            'authority': 'www.gunviolencearchive.org',
            'method': 'GET',
            'path': '/incident/1587783',
            'scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-encoding': 'gzip, deflate, br',

            }
        scraper = cloudscraper.create_scraper()
        while True:
            try:
                # resp = await self._sess.get(url)
                resp = await scraper.get(url)
                print(resp)
                exit()
            except Exception as exc:
                status = _status_from_exception(exc)
                if not status:
                    raise
            else:
                status = resp.status
                if status < 500: # Suceeded or client error
                    return resp
                # It's a server error. Dispose the response and retry.
                await resp.release()

            wait = _compute_wait(average_wait, rng_base)
            self._log_retry(url, status, wait)
            await asyncio.sleep(wait)

    async def _get_fields_from_incident_url(self, row):
        incident_url = row['incident_url']
        resp = await self._get(incident_url)
        async with resp:
            resp.raise_for_status()
            ctype = resp.headers.get(CONTENT_TYPE, '').lower()
            mimetype = ctype[:ctype.find(';')]
            if mimetype in ('text/htm', 'text/html'):
                text = await resp.text()
            else:
                raise NotImplementedError("Encountered unknown mime type {}".format(mimetype))

        ctx = Context(address=row['address'],
                      city_or_county=row['city_or_county'],
                      state=row['state'])
        return self._extractor.extract_fields(text, ctx)

    async def get_fields_from_incident_url(self, row):
        log_first_call()
        try:
            return await self._get_fields_from_incident_url(row)
        except Exception as exc:
            # Passing return_exceptions=True to asyncio.gather() destroys the ability
            # to print them once they're caught, so do that manually here.
            if isinstance(exc, ClientResponseError) and exc.code == 404:
                # 404 is handled gracefully by us so this isn't too newsworthy.
                pass
            else:
                self._log_extraction_failed(row['incident_url'])
                tb.print_exc()
            raise
