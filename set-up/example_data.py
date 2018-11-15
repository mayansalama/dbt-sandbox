import sys
import random
import faker
import datetime

from star_schema_generator import DummyStarSchema

num_iterations = 3000
scale_factor = 4
folder = 'sample-data'

fake = faker.Faker()
low_date = datetime.datetime(2018, 11, 1)
high_date = datetime.datetime(2018, 12, 1)
num_days = (high_date - low_date).days
num_currencies = 5

"""
Usage: python generate_dummy_date.py

Generates a some sample star schema entities for a given number of rows in a specified file. The size of the data 
will be proportional to the number of iterations and the specified scale_factor.
"""


def generate_customer():
    gender = random.sample(["male", "female"], 1)[0]
    if gender == "male":
        name = fake.name_male()
    else:
        name = fake.name_female()
    address = fake.address()

    first, last = name.split(' ')[-2:]
    return {"first_name": first,
            "last_name": last,
            "gender": gender,
            "address": address.replace('\n', ', ')}


def generate_product():
    name = fake.bs().split(' ')[-1]
    desc = fake.paragraph()
    return {
        "name": name,
        "long_desc": desc
    }


def generate_order():
    return {
        'order_time': fake.date_time_between(low_date, high_date)
    }


def generate_order_item():
    return {
        "ammount": random.weibullvariate(1, 0.5) * 100
    }


def generate_days():
    cur_day = low_date
    while True:
        yield {
            "day_value": cur_day
        }
        cur_day += datetime.timedelta(days=1)


def generate_currency():
    return {
        "currency": fake.currency()[0]
    }


def generate_currency_conv():
    return {
        "to_aud": random.weibullvariate(1, 0.5)
    }


def get_num_products(num_iterations, scale_factor):
    return random.randint(
        min(random.randint(50, 100), int(num_iterations / scale_factor - num_iterations / scale_factor / 2)),
        min(random.randint(150, 200), int(num_iterations / scale_factor + num_iterations / scale_factor / 2))
    )


def main():
    num_products = get_num_products(num_iterations, scale_factor)

    schema = [
        #  DIMS
        {
            'name': 'customer',  # the name of the entity/table
            'generator_function': generate_customer,  # function that defines entity
            'num_iterations': num_iterations  # How many times to run that function
        },
        {
            'name': 'product',
            'generator_function': generate_product,
            'num_iterations': num_products
        },
        {
            'name': 'currency',
            'generator_function': generate_currency,
            'num_iterations': 10
        },
        {
            'name': 'days',
            'generator_function': generate_days,
            'num_iterations': num_days
        },
        #  FACTS
        {
            'name': 'orders',
            'generator_function': generate_order,
            'num_iterations': num_iterations * scale_factor,
            'relations': [{'name': 'customer'}]  # Entity relations (by default many to one)
        },
        {
            'name': 'order_item',
            'generator_function': generate_order_item,
            'num_iterations': num_iterations * scale_factor,
            'num_facts_per_iter': lambda: random.randint(1, 3),  # Number of facts per iteration (e.g. 3 items 1 order)
            'relations': [{'name': 'orders'},
                          {'name': 'currency'},
                          {'name': 'product', 'type': 'many_to_many', 'unique': True}]
            # Each iteration has the same entity link for one_to_many relations (e.g. one order_id per order_item)
            # For many_to_many this link is sampled - if unique then it is sampled without replacement.
            # In this example an order has multiple order items, each linked to a unique product within that order
            # If an order could have multiple of the same product then unique would be false
        },
        {
            'name': 'currency_conversion',
            'generator_function': generate_currency_conv,
            'num_iterations': num_currencies,
            'num_facts_per_iter': num_days,  # We get one record per currency per day
            'relations': [{'name': 'currency'},
                          {'name': 'days', 'type': 'many_to_many', 'unique': True}]
        }
    ]

    dummy_data = DummyStarSchema.initiate_from_entity_list(schema)
    dummy_data.to_csv(folder)
    dummy_data.to_schemas(folder)
    print("Done")


if __name__ == "__main__":
    main()
