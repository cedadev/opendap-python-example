# encoding: utf-8
"""
nc_read_with_cert.py
===================

Python script for reading a NetCDF file remotely from the CEDA archive.

Pre-requisites:

 - Python2.7
 - Python libraries (installed by Pip):

```
ContrailOnlineCAClient
netCDF4
```

Usage:

```
$ python nc_read_with_cert.py <url> <var_id>
```

Example:

```
$ URL=http://dap.ceda.ac.uk/thredds/dodsC/badc/ukcp18/data/marine-sim/skew-trend/rcp85/skewSurgeTrend/latest/skewSurgeTrend_marine-sim_rcp85_trend_2007-2099.nc
$ VAR_ID=skewSurgeTrend

$ python nc_read_with_cert.py $URL $VAR_ID
```

"""

# Import standard libraries
import os
import sys
import datetime

# Get CEDA username and password from environment variables
username = os.environ['CEDA_USERNAME']
password = os.environ['CEDA_PASSWORD']

# Import third-party libraries
from cryptography import x509
from cryptography.hazmat.backends import default_backend

from contrail.security.onlineca.client import OnlineCaClient
from netCDF4 import Dataset


# Credentials defaults
DODS_FILE_CONTENTS = """HTTP.COOKIEJAR=./dods_cookies
HTTP.SSL.CERTIFICATE=./credentials.pem
HTTP.SSL.KEY=./credentials.pem
HTTP.SSL.CAPATH=./ca-trustroots
"""

DODS_FILE_PATH = os.path.expanduser('~/.dodsrc')
CERTS_DIR = os.path.expanduser('~/.certs')

if not os.path.isdir(CERTS_DIR):
    os.makedirs(CERTS_DIR)

TRUSTROOTS_DIR = os.path.join(CERTS_DIR, 'ca-trustroots')
CREDENTIALS_FILE_PATH = os.path.join(CERTS_DIR, 'credentials.pem')

TRUSTROOTS_SERVICE = 'https://slcs.ceda.ac.uk/onlineca/trustroots/'
CERT_SERVICE = 'https://slcs.ceda.ac.uk/onlineca/certificate/'


def write_dods_file_contents():

    DODS_FILE_CONTENTS = """
    HTTP.COOKIEJAR=./dods_cookies
    HTTP.SSL.CERTIFICATE={credentials_file_path}
    HTTP.SSL.KEY={credentials_file_path}
    HTTP.SSL.CAPATH={trustroots_dir}
    """.format(credentials_file_path=CREDENTIALS_FILE_PATH, trustroots_dir=TRUSTROOTS_DIR)

    with open(DODS_FILE_PATH, 'w') as dods_file:
        dods_file.write(DODS_FILE_CONTENTS)


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


def setup_credentials(force=False):
    """
    Download and create required credentials files.

    Return True if credentials were set up.
    Return False is credentials were already set up.

    :param force: boolean
    :return: boolean
    """
    # Test for DODS_FILE and only re-get credentials if it doesn't
    # exist AND `force` is True AND certificate is in-date.
    if os.path.isfile(DODS_FILE_PATH) and not force and cert_is_valid(CREDENTIALS_FILE_PATH):
        print('[INFO] Security credentials already set up.')
        return False

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

    # Write the dodsrc credentials file
    write_dods_file_contents()

    print('[INFO] Security credentials set up.')
    return True


def get_nc_dataset(url, var_id):
    """
    Open a remote connection to a NetCDF4 Dataset at `url`.
    Show information about variable `var_id`.
    Print metadata / data in the file and return the Dataset object.

    :param url: URL to a NetCDF OpenDAP end-point.
    :param var_id: Variable ID in NetCDF file [string]
    :return: netCDF4 Dataset object
    """
    print("before")
    dataset = Dataset(url)
    print("after")


    print('\n[INFO] Global attributes:')
    for attr in dataset.ncattrs():
        print('\t{}: {}'.format(attr, dataset.getncattr(attr)))

    print('\n[INFO] Variables:\n{}'.format(dataset.variables))
    print('\n[INFO] Dimensions:\n{}'.format(dataset.dimensions))

    print('\n[INFO] Max and min variable: {}'.format(var_id))
    #variable = dataset.variables[var_id][0:0]
    #print('\tMin: {:.6f}; Max: {:.6f}'.format(variable.min(), variable.max()))
    variable = dataset.variables[var_id][:]
    units = dataset.variables[var_id].units
    print('\tMin: {:.6f} {}; Max: {:.6f} {}'.format(variable.min(), units, variable.max(), units))

    return dataset


def main(nc_file_url, var_id):
    """
    Main controller function.

    :param nc_file_url: URL to a NetCDF4 opendap end-point.
    :param var_id: Variable ID [String]
    :return: None
    """
    setup_credentials(force=False)
    ds = get_nc_dataset(nc_file_url, var_id)


if __name__ == '__main__':

    args = sys.argv[1:3]
    main(*args)
