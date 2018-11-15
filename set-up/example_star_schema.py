import sys
import random
import faker

from dummy_star_schema import DummyStarSchema

fake = faker.Faker()

"""
Usage: python generate_dummy_date.py --num-custs 30 --folder sample-data --scale-factor 4

Generates a some sample star schema entities for a given number of rows in a specified file. The size of the data 
will be proportional to the number of custs and the specified scale_factor.
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
    name = fake.bs()
    desc = fake.paragraph()
    return {
        "name": name,
        "desc": desc
    }


def generate_order():
    return {
        'order_time': fake.date_time_this_month()
    }


def generate_order_item():
    return {
        "ammount": random.weibullvariate(1, 0.5) * 100,
        "currency": fake.currency()[0]
    }


def get_num_products(num_iterations, scale_factor):
    return random.randint(
        min(random.randint(50, 100), int(num_iterations / scale_factor - num_iterations / scale_factor / 2)),
        min(random.randint(150, 200), int(num_iterations / scale_factor + num_iterations / scale_factor / 2))
    )


def main():
    num_iterations = 30
    scale_factor = 4
    folder = 'sample-data'

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
        #  FACTS
        {
            'name': 'order',
            'generator_function': generate_order,
            'num_iterations': num_iterations * scale_factor,
            'relations': [{'name': 'customer'}]  # Entity relations (by default many to one)
        },
        {
            'name': 'order_item',
            'generator_function': generate_order_item,
            'num_iterations': num_iterations * scale_factor,
            'num_facts_per_iter': lambda: random.randint(1, 3),  # Number of facts per iteration (e.g. 3 items 1 order)
            'relations': [{'name':'order'},
                          {'name': 'product', 'type': 'many_to_many', 'unique': True}]  # Each order item has a single product
        }
    ]

    dummy_data = DummyStarSchema.initiate_from_entity_list(schema)
    dummy_data.to_json(folder)
    print("Done")


if __name__ == "__main__":
    main()