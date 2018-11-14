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


def get_num_products(num_ents, scale_factor):
    return random.randint(
        min(random.randint(50, 100), int(num_ents / scale_factor - num_ents / scale_factor / 2)),
        min(random.randint(150, 200), int(num_ents / scale_factor + num_ents / scale_factor / 2))
    )


if __name__ == "__main__":
    arg_list = sys.argv
    arg_dict = {arg_list[i]: arg_list[i + 1] for i in range(1, len(arg_list), 2)}

    num_ents = arg_dict.get("--num-entities", 30)
    if num_ents > 12 ** 36:
        raise ValueError("Too many entities: this will result in id collisions")

    folder = arg_dict.get("--folder", "sample-data")
    scale_factor = arg_dict.get("--scale-factor", 4)

    schema = [
        {
            'name': 'customer',
            'generator_function': generate_customer,
            'num_ents': num_ents
        },
        {
            'name': 'product',
            'generator_function': generate_product,
            'num_ents': get_num_products(num_ents, scale_factor)
        },
        {
            'name': 'order',
            'generator_function': generate_order,
            'num_ents': num_ents * scale_factor,
            'relations': ['customer']
        },
        {
            'name': 'order_item',
            'generator_function': generate_order_item,
            'num_ents': num_ents * scale_factor,
            'relations': ['order'],
            'one_to_many': {'relation': 'product', 'fuzz': lambda: random.randint(1, 3), "unique": True}
        }
    ]

    dummy_data = DummyStarSchema.initiate_from_entity_list(schema)
    dummy_data.to_csv(folder)
    print("Done")