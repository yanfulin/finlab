import os
import logging
import requests
from finlab import get_token
import os
import requests
import re
import finlab

# Get an instance of a logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

__finlab_check_version = False

def get_module_version():
    res = requests.get('https://pypi.org/project/finlab/')
    res.encoding = 'utf-8'

    m = re.findall("finlab\s([a-z0-9.]*)\s*</h1>", res.text)
    return m[0] if m else None

def check_version(func):

    global __finlab_check_version

    if __finlab_check_version == True:
        return func

    pip_version = get_module_version()

    if pip_version != finlab.__version__:
        logger.warning(f'Your version is {finlab.__version__}, please isntall a newer version.\n Use "pip install finlab=={pip_version}" '
                           f'to update the latest version.')

    __finlab_check_version = True
    return func

def raise_permission_error(*args, **kwargs):
    raise Exception(
        "backtest.sim server is down. Please contact us on discord: https://discord.gg/tAr4ysPqvR")


def auth_permission(allow_roles=None):
    def decorator(func):
        def warp(*args, **kwargs):
            if allow_roles is None:
                return func(*args, **kwargs)
            role = os.environ.get('finlab_role')
            if role in allow_roles:
                return func(*args, **kwargs)
            else:
                logger.error(
                    f"Your role is {role} that don't have permission to use this function.")
        return warp
    return decorator


def download_encrypted_py_file(folder, module_name):

    pye_file_name = f'{folder}__{module_name}.pye'
    encrypted_folder = 'encrypted_py_files'

    request_args = {
        'api_token': get_token(),
        'bucket_name': 'finlab_tw_stock_item',
        'blob_name': encrypted_folder + '/' + pye_file_name
    }

    url = 'https://asia-east2-fdata-299302.cloudfunctions.net/auth_generate_data_url'
    auth_url = requests.get(url, request_args)
    try:
        from sourcedefender.tools import getUrl
        getUrl(auth_url.text)
    except:
        print('Install sourcedefender to download the old backtest scripts')
