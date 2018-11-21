import string, random
from datetime import datetime

def get_random_email():
    email = ''.join(random.sample(string.ascii_lowercase, 10))
    return email+"@gmail.com"

def get_timestamp():
    return datetime.now().strftime(("%Y-%m-%d %H:%M:%S"))

def get_random_amount():
    amount = random.randint(10000, 999999)
    return amount
