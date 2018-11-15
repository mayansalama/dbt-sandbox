import os
import uuid
import random
import csv
import json
import pickle
from datetime import datetime, date
from typing import Dict

import networkx


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


class Relation:
    def __init__(self, name, type=None, unique=False):
        self.name = name
        self.type = type or "one_to_many"
        self.unique = bool(unique)

    @staticmethod
    def from_dict(dict):
        return Relation(dict['name'],
                        dict.get('type'),
                        dict.get("unique"))

    @property
    def id(self):
        return self.name + "_id"


class Entity:
    def __init__(self, name, generator_function, num_iterations, relations, num_facts_per_iter=None):
        self.name = name
        self.gen = generator_function
        self.num_iterations = num_iterations
        self.relations = relations
        if not num_facts_per_iter:
            num_facts_per_iter = 1
        self.num_facts_per_iter = num_facts_per_iter

    @staticmethod
    def from_dict(dict):
        name = dict['name']
        gen = dict['generator_function']
        num_iterations = dict['num_iterations']
        relations = dict.get('relations', [])
        num_facts_per_iter = dict.get('num_facts_per_iter')

        return Entity(name, gen, num_iterations, relations, num_facts_per_iter)

    @property
    def id(self):
        return self.name + "_id"

    @property
    def relations(self):
        return self._relations

    @relations.setter
    def relations(self, relations):
        if relations and isinstance(relations, list):
            self._relations = [Relation.from_dict(rel) if isinstance(rel, dict) else rel for rel in relations]
        else:
            self._relations = []

    @property
    def one_to_many_relations(self):
        return [rel for rel in self.relations if rel.type == "one_to_many"]

    @property
    def many_to_many_relations(self):
        return [rel for rel in self.relations if rel.type == "many_to_many"]

    @property
    def num_facts_per_iter(self):
        return self._num_facts_per_iter

    @num_facts_per_iter.setter
    def num_facts_per_iter(self, val):
        if not callable(val):
            if str(val).isnumeric():
                val = int(float(str(val)))
                func = lambda: val
            else:
                raise ValueError("Num facts per iteration must be either numeric or a function")
        else:
            func = val

        if not isinstance(func(), int):
            raise ValueError("Num facts per iteration must return an integer")
        self._num_facts_per_iter = func


class DummyStarSchema:

    ##################################################################
    # Init and props
    ##################################################################

    def __init__(self, entity_list):
        self.entity_dict: Dict[Entity] = {
            entity.name: entity for entity in entity_list
        }
        self.dag = self.generate_dag()
        self.datasets = None
        self.generate()

    @staticmethod
    def initiate_from_entity_list(list):
        return DummyStarSchema([
            Entity.from_dict(d) for d in list
        ])

    def generate_dag(self):
        dag = networkx.DiGraph()

        # Add our nodes
        for entity in self.entity_dict.values():
            for relation in entity.relations:
                # entity is child of relation
                try:
                    dag.add_edge(self.entity_dict[relation.name], entity)
                except KeyError as e:
                    raise KeyError("Unable to find relation: '{}'".format(str(e)))
            dag.add_node(entity)  # Just in case there's a standalone

        if not networkx.is_directed_acyclic_graph(dag):
            raise ValueError("Circular dependencies in relations")

        return dag

    ##################################################################
    # Create Entities
    ##################################################################

    def generate_entity_data(self, entity, datasets):
        ents = {}
        relation_id_lists = {rel.id: list(datasets[rel.name].keys()) for rel in entity.relations}

        for i in range(entity.num_iterations):
            base = {}

            for relation in entity.one_to_many_relations:  # These will be the same per fact per instance
                base[relation.id] = random.sample(relation_id_lists[relation.id], 1)[0]

            num_facts = entity.num_facts_per_iter()  # Defaults to uniform 1
            many_to_many_ids = {}
            for rel in entity.many_to_many_relations:  # These will be the same per fact
                if rel.unique:
                    many_to_many_ids[rel.id] = random.sample(relation_id_lists[rel.id], num_facts)
                else:
                    many_to_many_ids[rel.id] = [random.sample(relation_id_lists[rel.id], 1)[0]
                                                for i in range(num_facts)]

            for i in range(num_facts):
                while True:  # Get a unique id for this instance
                    uid = str(uuid.uuid4())[-12:]  # 36 ** 12 is max num entities...
                    if uid not in ents:
                        break
                inst = {entity.id: uid}
                inst.update(base)

                for relation in entity.many_to_many_relations:
                    inst[relation.id] = many_to_many_ids[relation.id].pop(0)

                inst.update(entity.gen())
                ents[uid] = inst

        return ents

    def generate(self):
        datasets = {}

        for entity in networkx.topological_sort(self.dag):
            datasets[entity.name] = self.generate_entity_data(entity, datasets)

        self.datasets = datasets

    ##################################################################
    # Save File
    ##################################################################

    def to_csv(self, folder):
        if folder and not os.path.exists(folder):
            os.makedirs(folder)

        for name, uids in self.datasets.items():
            with open((folder + '/' if folder else '') + name + ".csv", "w+") as f:
                wr = csv.writer(f)
                headers = True
                for row in uids.values():
                    if headers:
                        headers = False
                        wr.writerow(list(row.keys()))

                    wr.writerow(list(row.values()))

    def to_json(self, folder):
        if folder and not os.path.exists(folder):
            os.makedirs(folder)

        for name, uids in self.datasets.items():
            with open((folder + '/' if folder else '') + name + ".json", "w+") as f:
                json.dump(list(uids.values()), f, default=json_serial)

    def to_schemas(self, folder):
        if folder and not os.path.exists(folder):
            os.makedirs(folder)

        for name, uids in self.datasets.items():
            with open((folder + '/' if folder else '') + name + ".schema", "wb") as f:
                first_row = next(iter(uids.values()))
                schema = {}
                for name, val in first_row.items():
                    schema[name] = type(val)
                pickle.dump(schema, f)
