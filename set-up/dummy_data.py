import os
import uuid
import random
import csv
import copy
from typing import Dict

import networkx


class Multi:
    def __init__(self, relation, fuzz=None, unique=None):
        self.relation = relation
        self.fuzz = fuzz
        if not fuzz:
            self.fuzz = lambda: 1
        self.unique = unique or True

    @staticmethod
    def from_dict(dict):
        return Multi(dict['relation'],
                     dict.get('fuzz'),
                     dict.get("unique"))


class Entity:
    def __init__(self, name, generator_function, num_ents, relations, multi):
        self.name = name
        self.gen = generator_function
        self.num_ents = num_ents
        self.relations = relations
        self.multi = multi

    @property
    def multi(self):
        return self._multi

    @multi.setter
    def multi(self, val):
        if not val:
            self._multi = None
        elif isinstance(val, dict):
            self._multi = Multi.from_dict(val)
        elif isinstance(val, list):
            self._multi = Multi(*val)
        elif isinstance(val, Multi):
            self._multi = Multi
        else:
            raise ValueError("Unable to parse multi_depedency")

    @staticmethod
    def from_dict(dict):
        name = dict['name']
        gen = dict['generator_function']
        num_ents = dict['num_ents']

        relations = dict.get('relations', [])
        multi = dict.get('one_to_many')

        return Entity(name, gen, num_ents, relations, multi)


class DummyStarSchema:

    ##################################################################
    # Init and props
    ##################################################################

    def __init__(self, entity_list):
        self.entities: Dict[Entity] = {
            entity.name: entity for entity in entity_list
        }
        self.dag = self.generate_dag()
        self.datasets = None
        self.instantiate()

    @staticmethod
    def initiate_from_entity_list(list):
        return DummyStarSchema([
            Entity.from_dict(d) for d in list
        ])

    def generate_dag(self):
        dag = networkx.DiGraph()

        # Add our nodes
        for entity in self.entities.values():
            for relation in entity.relations:
                # entity is child of relation
                dag.add_edge(self.entities[relation], entity)
            if entity.multi:
                dag.add_edge(self.entities[entity.multi.relation], entity)
            dag.add_node(entity)  # Just in case there's a standalone

        if not networkx.is_directed_acyclic_graph(dag):
            raise ValueError("Circular dependencies")

        return dag

    ##################################################################
    # Create Entities
    ##################################################################

    def generate_entity_data(self, entity, datasets):
        ents = {}

        if entity.multi:
            keys = list(datasets[entity.multi.relation].keys())

        for i in range(entity.num_ents):
            while True:
                uid = str(uuid.uuid4())[-12:]  # 36 ** 12 is max num entities...
                if uid not in ents:
                    break
            ents[uid] = []

            base = {entity.name + "_id": uid}
            for relation in entity.relations:
                field_name = relation + "_id"
                base[field_name] = random.sample(list(datasets[relation].keys()), 1)[0]

            if entity.multi:
                multi_count = entity.multi.fuzz()
                if entity.multi.unique:
                    mid = random.sample(keys, multi_count)
                else:
                    mid = [random.sample(keys, 1)[0] for i in multi_count]

                for i in range(multi_count):
                    base_copy = copy.deepcopy(base)
                    base_copy[entity.multi.relation + "_id"] = mid[i]

                    data = entity.gen()
                    base_copy.update(data)
                    ents[uid].append(base_copy)
            else:
                data = entity.gen()
                base.update(data)
                ents[uid].append(base)

        return ents

    def instantiate(self):
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
                # WRITE HEADERS
                first_uid = list(uids.values())[0]
                to_write = list(first_uid[0].keys())
                wr.writerow(to_write)

                # WRITE VALUES
                for rows in uids.values():
                    to_write = [row.values() for row in rows]
                    wr.writerows(to_write)
