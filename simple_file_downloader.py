# encoding: utf-8
"""
remote_nc_reader.py
===================

Python script for downloading a file from the CEDA archive.

Pre-requisites:

 - Python 2.7 or 3.X
 - Python libraries (installed by Pip):

```
ContrailOnlineCAClient
```

Usage:

```
$ python simple_file_downloader.py <url>
```

Example:

```
$ URL=http://dap.ceda.ac.uk/thredds/dodsC/badc/ukcp18/data/marine-sim/skew-trend/rcp85/skewSurgeTrend/latest/skewSurgeTrend_marine-sim_rcp85_trend_2007-2099.nc

$ python simple_file_downloader.py $URL
```

"""

# Import standard libraries
import os
import sys
import datetime
import requests

# Import third-party libraries
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from contrail.security.onlineca.client import OnlineCaClient


CERTS_DIR = os.path.expanduser('~/.certs')
if not os.path.isdir(CERTS_DIR):
    os.makedirs(CERTS_DIR)

TRUSTROOTS_DIR = os.path.join(CERTS_DIR, 'ca-trustroots')
CREDENTIALS_FILE_PATH = os.path.join(CERTS_DIR, 'credentials.pem')

TRUSTROOTS_SERVICE = 'https://slcs.ceda.ac.uk/onlineca/trustroots/'
CERT_SERVICE = 'https://slcs.ceda.ac.uk/onlineca/certificate/'


def cert_is_valid(cert_file, min_lifetime=0):
    """
    Returns boolean - True if the certificate is in date.
    Optional argument min_lifetime is the number of seconds
    which must remain.

    :param cert_file: certificate file path.
    :param min_lifetime: minimum lifetime (seconds)
    :return: boolean
    """
    try:
        with open(cert_file, 'rb') as f:
            crt_data = f.read()
    except IOError:
        return False

    try:
        cert = x509.load_pem_x509_certificate(crt_data, default_backend())
    except ValueError:
        return False

    now = datetime.datetime.now()

    return (cert.not_valid_before <= now
            and cert.not_valid_after > now + datetime.timedelta(0, min_lifetime))


def setup_credentials():
    """
    Download and create required credentials files.

    Return True if credentials were set up.
    Return False is credentials were already set up.

    :param force: boolean
    :return: boolean
    """

    # Test for DODS_FILE and only re-get credentials if it doesn't
    # exist AND `force` is True AND certificate is in-date.
    if cert_is_valid(CREDENTIALS_FILE_PATH):
        print('[INFO] Security credentials already set up.')
        return False

    # Get CEDA username and password from environment variables
    username = os.environ['CEDA_USERNAME']
    password = os.environ['CEDA_PASSWORD']

    onlineca_client = OnlineCaClient()
    onlineca_client.ca_cert_dir = TRUSTROOTS_DIR

    # Set up trust roots
    trustroots = onlineca_client.get_trustroots(
        TRUSTROOTS_SERVICE,
        bootstrap=True,
        write_to_ca_cert_dir=True)

    # Write certificate credentials file
    key_pair, certs = onlineca_client.get_certificate(
        username,
        password,
        CERT_SERVICE,
        pem_out_filepath=CREDENTIALS_FILE_PATH)

    print('[INFO] Security credentials set up.')
    return True


def main(file_url):
    """
    Main controller function.

    :param nc_file_url: URL to a NetCDF4 opendap end-point.
    :param var_id: Variable ID [String]
    :return: None
    """

    try:
        setup_credentials()
    except KeyError:
        print("CEDA_USERNAME and CEDA_PASSWORD environment variables required")
        return

    # Download file to current working directory
    response = requests.get(file_url, cert=(CREDENTIALS_FILE_PATH), verify=False)
    filename = file_url.rsplit('/', 1)[-1]
    with open(filename, 'wb') as file_object:
        file_object.write(response.content)


if __name__ == '__main__':

    try:
        main(sys.argv[1])
    except IndexError:
        print("Please provide a file URL as input")
