import os
import sys
import random
import faker
import datetime

from labgrownsheets.model import StarSchemaModel, PostgresSchemaAdapter, BigquerySchemaAdapter

num_iterations = 1000
scale_factor = 4
data_path = os.path.join('data')
schema_path = os.path.join('schema')

fake = faker.Faker()
low_date = datetime.datetime(2018, 1, 1)
high_date = datetime.datetime(2018, 12, 31)
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


def generate_order(*args, **kwargs):
    #  If args are supported then arg[0] will be the full datasets dictionary
    #  Kwargs will have 'inst': {dict of relations and ids}
    if not kwargs:  # Note that functions are tested to see if they compile, so the first run through will be null
        ld = low_date
        hd = high_date
    else:  # This way we never get an order created before a customer is
        ld = args[0]['customer'][kwargs['customer_id']][0]['valid_from_timestamp']
        hd = high_date
    return {
        'order_time': fake.date_time_between(ld, hd)
    }


def generate_order_item():
    return {
        "ammount": random.weibullvariate(1, 0.5) * 100
    }


def generate_currency():
    curs = ["AUD"]
    yield {
        "currency": "AUD"
    }
    while True:
        new_cur = fake.currency()[0]
        if new_cur not in curs:
            curs.append(new_cur)
            yield {
                "currency": new_cur
            }


def generate_currency_conv():
    def daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + datetime.timedelta(n)

    while True:
        root_value = random.weibullvariate(1, 3)

        for cur_day in daterange(low_date, high_date):
            yield {
                "day_value": cur_day,
                "to_aud": root_value
            }
            root_value += random.gauss(0, root_value / 100)


def get_num_products(num_iterations, scale_factor):
    return random.randint(
        min(random.randint(50, 100), int(num_iterations / scale_factor - num_iterations / scale_factor / 2)),
        min(random.randint(150, 200), int(num_iterations / scale_factor + num_iterations / scale_factor / 2))
    )


def main():
    num_products = get_num_products(num_iterations, scale_factor)

    schema = [
        #  DIMS
        ('naive_type2_scd', {
            'name': 'customer',
            'min_valid_from': low_date,
            'max_valid_from': high_date,
            'entity_generator': generate_customer,
            'num_iterations': num_iterations,
            'mutation_rate': 0.1,  # Will update mutate cols 10% of the time
            'mutating_cols': ['address']  # Only address will update
        }),
        ('naive', {
            'name': 'product',
            'entity_generator': generate_product,
            'num_iterations': num_products
        }),
        ('naive', {
            'name': 'currency',
            'entity_generator': generate_currency,
            'num_iterations': num_currencies
        }),
        #  FACTS
        ('naive', {
            'name': 'orders',
            'entity_generator': generate_order,
            'num_iterations': num_iterations * scale_factor,
            'relations': [{'name': 'customer'},
                          {'name': 'currency'}]
        }),
        ('naive', {
            'name': 'order_item',
            'entity_generator': generate_order_item,
            'num_iterations': num_iterations * scale_factor,
            'num_entities_per_iteration': lambda: random.randint(1, 3),
        # Number of facts per iteration (e.g. 3 items 1 order)
            'relations': [{'name': 'orders', 'unique': True},
                          {'name': 'product', 'type': 'many_to_many', 'unique': True}]
            # Each iteration has the same entity link for one_to_many relations (e.g. one order_id per order_item)
            # For many_to_many this link is sampled - if unique_per_fact then it is sampled without replacement.
            # In this example an order has multiple order items, each linked to a unique_per_fact product within that order
            # If an order could have multiple of the same product then unique_per_fact would be false
        }),
        ('naive', {
            'name': 'currency_conversion',
            'entity_generator': generate_currency_conv,
            'num_iterations': num_currencies,
            'num_entities_per_iteration': num_days,  # We get one record per currency per day
            'relations': [{'name': 'currency', 'unique': True}]
            # Here the default type is one_to_many - in this case there will be a unique value for each iteration
            # Sampled from the source table - note this will fail if there are more iterations that values in
            # The original table.
        })
    ]

    dummy_data = StarSchemaModel.from_list(schema)
    dummy_data.generate_all_datasets(print_progress=True)
    dummy_data.to_csv(data_path)

    padapter = PostgresSchemaAdapter(dummy_data)
    padapter.to_dbt_schema(path=schema_path)

    bqadapter = BigquerySchemaAdapter(dummy_data)
    bqadapter.to_dbt_schema(path=schema_path)

    print("Done")


if __name__ == "__main__":
    args = sys.argv
    if len(args) == 2:
        num_iterations = int(args[1])
    elif len(args) > 2:
        raise ValueError("Unable to process input args")

    main()
