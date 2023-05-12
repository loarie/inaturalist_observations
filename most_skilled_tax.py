from __future__ import absolute_import, division, print_function

import argparse
import csv
import json
import os
import sys
import time

import numpy as np

import sys
sys.path.append('..')
from crowdsourcing.annotations.classification import multiclass_single_binomial as MSB

def parse_args():

    parser = argparse.ArgumentParser(description='Interactively test the person classifier')

    parser.add_argument('--model_path', dest='model_path',
                        help='Path to a trained model.', type=str,
                        required=True)

    args = parser.parse_args()
    return args

def only_same_number(lst):
    if len(lst) == 0:
        return False
    return all(elem == lst[0] for elem in lst)

def print_list(list, indent=0):
    # first print all nodes with no parent
    for node in list:
        if node['parent'] is None:
            print(" " * indent + str(node['taxon_id']) + ", max: " + 
              str(node['most_skilled']) + " (" + 
              (str(round(node['most_skilled_val'], 4)) if node['most_skilled_val'] is not None else "") + "), min: " + 
              str(node['least_skilled']) + " (" + 
              (str(round(node['least_skilled_val'], 4)) if node['least_skilled_val'] is not None else "") + ")")
            print_children(list, node['label'], indent+2)

def print_children(list, parent_key, indent):
    # print all children of the given parent recursively
    for node in list:
        if node['parent'] == parent_key:
            print(" " * indent + str(node['taxon_id']) + ", max: " + 
              str(node['most_skilled']) + " (" + 
              (str(round(node['most_skilled_val'], 4)) if node['most_skilled_val'] is not None else "") + "), min: " + 
              str(node['least_skilled']) + " (" + 
              (str(round(node['least_skilled_val'], 4)) if node['least_skilled_val'] is not None else "") + ")")
            print_children(list, node['label'], indent+2)


def main():

    args = parse_args()
    model_path = args.model_path

    model = MSB.CrowdDatasetMulticlassSingleBinomial()
    model.load(model_path,
        load_dataset=True,
        load_workers=True,
        load_images=False,
        load_annos=False,
        load_gt_annos=False,
        load_combined_labels=False
    )

    # Initialize the class priors.
    # NOTE: this might be different than the train dataset!
    if hasattr(model, 'global_class_priors'):
        class_probs = model.global_class_priors
    else:
        #assert False
        #class_probs = np.ones(model.num_classes) * (1. / model.num_classes)
        class_probs = {node.key : 1. / model.num_classes for node in model.taxonomy.leaf_nodes()}

    model.class_probs = class_probs
    model.class_probs_prior = class_probs

    model.initialize_default_priors()
    model.initialize_data_structures()

    if hasattr(model, 'inat_taxon_id_to_class_label'):
        inat_taxon_id_to_class_label = model.inat_taxon_id_to_class_label
    else:
        # Assume that the node keys are the inat taxon ids
        inat_taxon_id_to_class_label = {k : k for k in model.taxonomy.nodes}

        #print("ERROR: inat_taxon_id_to_class_label needs to be present")
        #return

    label_to_inat_taxon_id = {v : k for k, v in inat_taxon_id_to_class_label.items()}
    
    most_skill = []
    for entry in model.taxonomy.inner_nodes():
        label = entry.key
        taxon_id = label_to_inat_taxon_id[str(label)]
        z_integer_id = model.orig_node_key_to_integer_id[label]
        z_node_list = model.root_to_node_path_list[z_integer_id]
        z_parent_node = z_node_list[len(z_node_list)-2]
        skill_vector_index = model.internal_node_integer_id_to_skill_vector_index[z_parent_node]
        res = {}
        for worker_id in list(model.workers.keys()):
            worker = model.workers[worker_id]
            skill = worker.skill_vector[skill_vector_index]
            res[worker_id] = skill
        if only_same_number(list(res.values())):
            max_val = None
            max_key = None
            min_val = None
            min_key = None
        else:
            max_val = max(res.values())
            max_key = [k for k, v in res.items() if v == max_val][0]
            min_val = min(res.values())
            min_key = [k for k, v in res.items() if v == min_val][0]
        if label == '0':
            z_parent_node = None
        if label == 0:
            parent = None
        most_skill.append({'parent': z_parent_node,'label': int(label), 'taxon_id': taxon_id, 'most_skilled': max_key, 'most_skilled_val': max_val, 'least_skilled': min_key, 'least_skilled_val': min_val})

    print_list(most_skill)

if __name__ == '__main__':

    main()