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


def get_random_name():
    name = ''.join(random.sample(string.ascii_lowercase, 10))
    return name

def get_random_age():
    age = random.randint(17, 50)
    return age

def get_random_cust_id():
    cust_id = random.randint(1, 3)
    return cust_id
