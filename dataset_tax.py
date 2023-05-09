"""
Data Formats:

identifications.csv
    id
    taxon_id
    user_id
    observation_id
    created_at
    current
    taxon_change_id
    category
    label

taxonomy.csv
    parent
    key
    taxon_id
    prob
    leaf

"""


from __future__ import absolute_import, division, print_function
import argparse
from collections import Counter
import datetime
import csv
import json
import os
import random

class iNaturalistDataset():

    def __init__(self, observations=None, observation_photos=None,
                 identifications=None, taxa=None, users=None):

        self.observations = observations
        self.observation_photos = observation_photos
        self.identifications = identifications
        self.taxa = taxa
        self.users = users

        self.taxon_id_to_taxon = {taxon['taxon_id'] : taxon for taxon in self.taxa}
        self.ob_id_to_ob = {ob['id'] : ob for ob in self.observations}
        self.iden_id_to_iden = {iden['id'] : iden for iden in self.identifications}
        
        self._sanity_check_data()

    def _sanity_check_data(self):
        """ Ensure that all identifications have corresponding taxons.
        """
        for iden in self.identifications:
            #assert iden['taxon_id'] in self.taxon_id_to_taxon
            if iden['taxon_id'] not in self.taxon_id_to_taxon:
                self.identifications.remove(iden)
            #assert iden['observation_id'] in self.ob_id_to_ob
            if iden['observation_id'] not in self.ob_id_to_ob:
                self.identifications.remove(iden)

        # Lets make sure that identification ids are unique
        iden_ids = [iden['id'] for iden in self.identifications]
        assert len(iden_ids) == len(set(iden_ids))

        # Lets make sure that the observation ids are unique
        obs_ids = [ob['id'] for ob in self.observations]
        assert len(obs_ids) == len(set(obs_ids))
    
    def keep_specific_observations(self, observation_ids_to_keep):
        """ Keep only the specified observations.
        """
        observation_ids_to_keep = set(observation_ids_to_keep)
        obs_to_keep = [ob for ob in self.observations if ob['id'] in observation_ids_to_keep]
        self.observations = obs_to_keep
        self.ob_id_to_ob = {ob['id'] : ob for ob in self.observations}

        idens_to_keep = [iden for iden in self.identifications if iden['observation_id'] in self.ob_id_to_ob]
        self.identifications = idens_to_keep
        self.iden_id_to_iden = {iden['id'] : iden for iden in self.identifications}

    def keep_one_identification_per_user_per_observation(self, keep_index=0):
        """ Select one identification per user per observation.
        keep_index = 0 for the first identification, -1 for the last identification.
        """

        # group the identifications by observation ids
        ob_id_to_idens = {ob['id'] : [] for ob in self.observations}
        for iden in self.identifications:
            ob_id_to_idens[iden['observation_id']].append(iden)

        iden_ids_to_keep = set()
        for ob_id, idens in ob_id_to_idens.items():
            # group the identifications by user and process them individually
            user_id_to_idens = {}
            for identification in idens:
                user_id = identification['user_id']
                user_id_to_idens.setdefault(user_id, [])
                user_id_to_idens[user_id].append(identification)

            for user_id in user_id_to_idens:
                user_idens = user_id_to_idens[user_id]

                # Sort the identifications by time
                for identification in user_idens:
                    created_at = identification['created_at']
                    try:
                        cat = datetime.datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S %Z')
                    except:
                        cat = datetime.datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
                    identification['time'] = cat
                user_idens.sort(key=lambda x: x['time'])

                iden_ids_to_keep.add(user_idens[keep_index]['id'])

        idens_to_keep = [iden for iden in self.identifications if iden['id'] in iden_ids_to_keep]
        self.identifications = idens_to_keep
        self.iden_id_to_iden = {iden['id'] : iden for iden in self.identifications}
    
    def keep_current_identifications(self):
        """ Keep only the current identifications.
        """

        idens_to_keep = [iden for iden in self.identifications if iden['current'] == True]
        self.identifications = idens_to_keep
        self.iden_id_to_iden = {iden['id'] : iden for iden in self.identifications}

        # Lets make sure that each identification for each observation is
        # coming from a unique worker.
        ob_id_to_idens = {ob['id'] : [] for ob in self.observations}
        for iden in self.identifications:
            ob_id_to_idens[iden['observation_id']].append(iden)
        for ob_id, idens in ob_id_to_idens.items():
            if len(set([iden['user_id'] for iden in idens])) != len(idens):
                print("ERROR: observation %s has multiple identifications from the same user" % (ob_id,))
                assert False

    def enforce_min_identifications(self, min_identifications=1):
        """ Remove observations that have less than `min_identifications`.
        """

        ob_id_to_idens = {ob['id'] : [] for ob in self.observations}
        for iden in self.identifications:
            ob_id_to_idens[iden['observation_id']].append(iden)

        ob_ids_to_keep = set()
        for ob_id, idens in ob_id_to_idens.items():
            if len(idens) >= min_identifications:
                ob_ids_to_keep.add(ob_id)

        obs_to_keep = [ob for ob in self.observations if ob['id'] in ob_ids_to_keep]
        self.observations = obs_to_keep
        self.ob_id_to_ob = {ob['id'] : ob for ob in self.observations}

        idens_to_keep = [iden for iden in self.identifications if iden['observation_id'] in self.ob_id_to_ob]
        self.identifications = idens_to_keep
        self.iden_id_to_iden = {iden['id'] : iden for iden in self.identifications}

    def enforce_max_observations(self, max_observations=None):
        if max_observations is None:
            return

        if max_observations >= len(self.observations):
            return

        # Randomly pick observations to keep
        ob_ids = [ob['id'] for ob in self.observations]
        ob_ids_to_keep = random.sample(ob_ids, max_observations)
        obs_to_keep = [ob for ob in self.observations if ob['id'] in ob_ids_to_keep]
        self.observations = obs_to_keep
        self.ob_id_to_ob = {ob['id'] : ob for ob in self.observations}

        idens_to_keep = [iden for iden in self.identifications if iden['observation_id'] in self.ob_id_to_ob]
        self.identifications = idens_to_keep
        self.iden_id_to_iden = {iden['id'] : iden for iden in self.identifications}
    
    def create_dataset(self, leaf_taxa_priors):

        taxon_id_to_class_label = {}
        for d in self.taxa:
            taxon_id_to_class_label[int(d['taxon_id'])] = d['key']
            
        taxonomy = []
        for item in self.taxa:
            new_item = {'parent': item['parent'], 'key': item['key'], 'data': {'prob': item['prob']}}
            taxonomy.append(new_item)

        dataset = {
            'num_classes' : len(leaf_taxa_priors),
            'inat_taxon_id_to_class_label' : taxon_id_to_class_label,
            'global_class_priors' : leaf_taxa_priors,
            'taxonomy_data' : taxonomy
        }

        workers = {}
        for iden in self.identifications:
            if iden['user_id'] not in workers:
                workers[iden['user_id']] = {
                    'id' : iden['user_id']
                }
        
        images = {}
        for ob in self.observations:
            images[ob['id']] = {
                'id' : ob['id'],
                'created_at' : None,
                'url' : None,
                'urls' : None
            }

        annos = []
        for iden in self.identifications:

            taxon_id = int(iden['taxon_id'])
            if taxon_id in taxon_id_to_class_label:
                worker_label = taxon_id_to_class_label[taxon_id]
            else:
                assert False

            annos.append({
                'anno' : {
                    'gtype' : 'multiclass',
                    'label' : str(worker_label), #iden['label']
                },
                'image_id' : iden['observation_id'],
                'worker_id' : iden['user_id'],
                'created_at' : iden['created_at'],
                'id' : iden['id']
            })

        dataset = {
            'dataset' : dataset,
            'workers' : workers,
            'images' : images,
            'annos' : annos
        }

        return dataset

def parse_args():

    parser = argparse.ArgumentParser(description='Create a dataset for crowdsourcing.')

    parser.add_argument('--archive_dir', dest='archive_dir',
                        help='Path to the database archive directory.', type=str,
                        required=True)

    parser.add_argument('--output_dir', dest='output_dir',
                        help='Path to an output directory to save the datasets.', type=str,
                        required=True)

    parser.add_argument('--max_obs', dest='max_observations',
                        help='Maximum number of observations to include in the training and testing datasets.', type=int,
                        required=False, default=None)


    args = parser.parse_args()
    return args

def main():

    args = parse_args()

    archive_dir = args.archive_dir
    output_dir = args.output_dir

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(os.path.join(archive_dir, 'taxonomy.csv'), 'r') as f:
        reader = csv.DictReader(f)
        taxonomy = []
        for row in reader:
            for key, value in row.items():
                if key != "parent" and key != "key":
                    if value.isdigit():
                        row[key] = int(value)
                    else:
                        try:
                            row[key] = float(value)
                        except ValueError:
                            pass
            taxonomy.append(row)

    with open(os.path.join(archive_dir, 'identifications.csv'), 'r') as f:
        reader = csv.DictReader(f)
        identifications = []
        for row in reader:
            identifications.append(row)

    for d in identifications:
        for k, v in d.items():
            if v == 'true':
                d[k] = True

    for d in taxonomy:
        for k, v in d.items():
            if v == '':
                d[k] = None
    
    observation_ids = {d['observation_id'] for d in identifications}
    users = {d['user_id'] for d in identifications}
    observations = [{'id': observation_id,
      'user_id': None,
      'community_taxon_id': None,
      'quality_grade': None,
      'created_at': None,
      'latitude': None,
      'longitude': None} for observation_id in observation_ids]
    observation_photos = [{'id': observation_id, 'native_original_image_url': None} for observation_id in observation_ids]
    
    # Build the observation label prediction dataset
    ob_inat = iNaturalistDataset(observations, observation_photos, identifications, taxonomy, users)
    ob_inat.keep_current_identifications()
    ob_inat.enforce_min_identifications(min_identifications=2)
    ob_inat.enforce_max_observations(args.max_observations)
    ob_ids = [ob['id'] for ob in ob_inat.observations] # Make sure the worker dataset has the same obs
    
    # Build the worker skill prediction dataset
    worker_inat = iNaturalistDataset(observations, observation_photos, identifications, taxonomy, users)
    worker_inat.keep_specific_observations(ob_ids)
    worker_inat.keep_one_identification_per_user_per_observation(keep_index=0)
    worker_inat.enforce_min_identifications(min_identifications=2)
    worker_inat.enforce_max_observations(args.max_observations)
    
    taxa_priors = {}
    for d in ob_inat.taxa:
        if d['leaf'] == 1:
            taxa_priors[int(d['key'])] = float(d['prob'])
    ob_label_pred_dataset = ob_inat.create_dataset(taxa_priors)
    
    label_pred_output_path = os.path.join(output_dir, 'observation_label_pred_dataset.json')
    with open(label_pred_output_path, 'w') as f:
        json.dump(ob_label_pred_dataset, f)

    worker_skill_pred_dataset = worker_inat.create_dataset(taxa_priors)

    worker_skill_pred_output_path = os.path.join(output_dir, 'worker_skill_pred_dataset.json')
    with open(worker_skill_pred_output_path, 'w') as f:
        json.dump(worker_skill_pred_dataset, f, indent=4, ensure_ascii=False)
        
    # Build a "testing" dataset.
    inat = iNaturalistDataset(observations, observation_photos, identifications, taxonomy, users)
    inat.keep_current_identifications()
    inat.enforce_min_identifications(min_identifications=1)
    inat.enforce_max_observations(args.max_observations)
    test_dataset = inat.create_dataset(taxa_priors)

    test_output_path = os.path.join(output_dir, 'test_dataset.json')
    with open(test_output_path, 'w') as f:
        json.dump(test_dataset, f)

if __name__ == '__main__':
    main()
