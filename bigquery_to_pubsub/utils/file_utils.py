import os


def delete_file(file):
    try:
        os.remove(file)
    except OSError:
        pass
