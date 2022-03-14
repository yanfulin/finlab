import os
import time
import logging
import threading

# Get an instance of a logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

__version__ = '0.2.2.dev1'


class LoginPanel():

    def __init__(self):
        pass

    def gui_supported(self):
        try:
            from IPython.display import IFrame, display, clear_output
            return True
        except:
            return False

    def display_gui(self):

        from IPython.display import IFrame, display, HTML, clear_output
        iframe = IFrame(
            f'https://ai.finlab.tw/api_token/?version={__version__}', width=620, height=300)
        display(iframe)

        token = input('請從 https://ai.finlab.tw/api_token 複製驗證碼: ')

        clear_output()
        self.login(token)

    def display_text_input(self):
        token = input('請從 https://ai.finlab.tw/api_token 複製驗證碼: ')
        self.login(token)

    @staticmethod
    def login(token):

        # check
        if '#' not in token or token.split('#')[1] not in ['vip', 'free']:
            raise Exception('The api_token format is wrong, '
                            'please paste the api_token after re-run the process or '
                            'check api token from https://ai.finlab.tw/api_token/.')

        # set token
        role = token[token.index('#') + 1:]
        token = token[:token.index('#')]
        os.environ['finlab_id_token'] = token
        os.environ['finlab_role'] = role
        print('輸入成功!')


def login(api_token=None):

    if api_token is None:
        lp = LoginPanel()
        if lp.gui_supported():
            lp.display_gui()
        else:
            lp.display_text_input()
    else:
        LoginPanel.login(api_token)


def get_token():
    if 'finlab_id_token' not in os.environ:
        login()

    return os.environ['finlab_id_token']


def get_role():
    if 'finlab_role' not in os.environ:
        login()

    return os.environ['finlab_role']
