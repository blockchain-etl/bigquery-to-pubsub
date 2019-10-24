import string
import random


def random_string(k):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=k))
