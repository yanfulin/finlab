import io

def get_cell_inputs(file_path):
    from nbformat import read
    with io.open(file_path) as f:
        nb = read(f, 4)
        ip = get_ipython()
        ret = []
        for cell in nb['cells']:
            if cell['cell_type'] != 'code':
                continue
            ret.append(cell['source'])

    return ret

def get_colab_cells():

    # Load the notebook JSON.
    from google.colab import _message
    nb = _message.blocking_request('get_ipynb')

    # Search for the markdown cell with the particular contents.
    return ["".join(cell['source']) for cell in nb['ipynb']['cells']]

def get_cells(path=None):
    if 'google.colab' in str(get_ipython()):
        cells = get_colab_cells()
    elif isinstance(path, str):
        cells = get_cell_inputs(path)
    else:
        print('Source code upload not support!')
        cells = []

    return cells
