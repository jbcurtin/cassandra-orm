import random
import string

def generate_string(str_length: int) -> str:
    ascii_pool = string.ascii_letters + string.digits + string.punctuation
    return ''.join([random.choice(ascii_pool) for idx in range(0, str_length)])
