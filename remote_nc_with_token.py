#!/usr/bin/env python

"""
nc_read_with_token.py
===================

Python script for reading a NetCDF file remotely from the CEDA archive. It demonstrates fetching
and using a download token to authenticate access to CEDA Archive data, as well as how to load
and subset the Dataset from a stream of data (diskless), without having to download the whole file.

Pre-requisites:

 - Python3.x
 - Python libraries (installed by Pip):

```
netCDF4
```

Usage:

```
$ python nc_read_with_token.py <url> <var_id>
```

Add CEDA login credentials to your environment:

```
$ export CEDA_USERNAME=<my_username>
$ export CEDA_PASSWORD=<my_password>
```

Example:

```
$ URL=http://dap.ceda.ac.uk/thredds/dodsC/badc/ukcp18/data/marine-sim/skew-trend/rcp85/skewSurgeTrend/latest/skewSurgeTrend_marine-sim_rcp85_trend_2007-2099.nc
$ VAR_ID=skewSurgeTrend

$ python nc_read_with_token.py $URL $VAR_ID
```

"""

import json
import os
import requests
import sys

from base64 import b64encode
from datetime import datetime, timezone
from netCDF4 import Dataset
from urllib.parse import urlparse


TOKEN_URL = "https://services-beta.ceda.ac.uk/api/token/create/"
# Location on the filesystem to store a
TOKEN_CACHE = os.path.expanduser("~/.cedatoken")


def load_cached_token():
    """ Read the token back out from its cache file.

    Returns a tuple containing the token and its expiry timestamp
    """

    # Read the token back out from its cache file
    try:
        with open(TOKEN_CACHE, "r") as cache_file:
            data = json.loads(cache_file.read())

            token = data.get("access_token")
            expires = datetime.strptime(data.get("expires"), "%Y-%m-%dT%H:%M:%S.%f%z")
            return token, expires

    except FileNotFoundError:
        return None, None


def get_token():
    """ Fetches a download token, either from a cache file or 
     from the token API using CEDA login credentials.

    Returns an active download token
    """

    # Check the cache file to see if we already have an active token
    token, expires = load_cached_token()

    # If no token has been cached or the token has expired, we get a new one
    now = datetime.now(timezone.utc)
    if not token or expires < now:

        # Get CEDA username and password from environment variables
        username = os.environ.get("CEDA_USERNAME")
        password = os.environ.get("CEDA_PASSWORD")

        if not username or not password:
            print("Please set CEDA_USERNAME and CEDA_PASSWORD as environment variables")
            return

        token = b64encode(f"{username}:{password}".encode('utf-8')).decode("ascii")
        headers = {
            "Authorization": f'Basic {token}',
        }
        response = requests.request("POST", TOKEN_URL, headers=headers)
        if response.status_code == 200:

            # The token endpoint returns JSON
            response_data = json.loads(response.text)
            token = response_data["access_token"]

            # Store the JSON data in the cache file for future use
            with open(TOKEN_CACHE, "w") as cache_file:
                cache_file.write(response.text)

        else:
            print("Failed to fetch token, check your username and password.")

    return token


def open_dataset(url, download_token=None):
    """ Open a NetCDF dataset from a remote file URL. Files requiring authentication
     will require an active download token associated with an authorised CEDa user.

    Returns a Python NetCDF4 Dataset object
    """

    headers=None
    # Add the download token to the request header if one is available
    if download_token:
        headers = {"Authorization": f"Bearer {download_token}"}

    response = requests.request("GET", url, headers=headers, stream=True)
    if response.status_code != 200:
        print(f"Failed to fetch data. The response from the server was {response.status_code}")
        return

    filename = os.path.basename(urlparse(url).path)
    print(f"Opening Dataset from file {filename} ...")
    # To avoid downloading the whole file, we create an "in-memory" Dataset from the response
    # See: https://unidata.github.io/netcdf4-python/#in-memory-diskless-datasets
    return Dataset(filename, memory=response.content)


if __name__ == "__main__":

    url = sys.argv[1]
    var_id = sys.argv[2]

    token = get_token()
    if token:

        print(f"Fetching information about variable '{var_id}' using data URL: '{url}'")
        if token:
            print(f"Using download token '{token[:5]}...{token[-5:]}' for authentication.")
        else:
            print("No DOWNLOAD_TOKEN found in environment.")

        dataset = open_dataset(url, download_token=token)

        print("\n[INFO] Global attributes:")
        for attr in dataset.ncattrs():
            print("\t{}: {}".format(attr, dataset.getncattr(attr)))

        print("\n[INFO] Variables:\n{}".format(dataset.variables))
        print("\n[INFO] Dimensions:\n{}".format(dataset.dimensions))

        print("\n[INFO] Max and min variable: {}".format(var_id))
        variable = dataset.variables[var_id][:]
        units = dataset.variables[var_id].units
        print("\tMin: {:.6f} {}; Max: {:.6f} {}".format(variable.min(), units, variable.max(), units))
