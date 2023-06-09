"""
Data Formats:

identification{
  u'category': u'improving',
  u'created_at': u'2011-12-04 05:08:30.290905',
  u'current': u't',
  u'id': u'53829',
  u'observation_id': u'41053',
  u'taxon_change_id': None,
  u'taxon_id': u'47792',
  u'user_id': u'3891'
}

observation{
  u'community_taxon_id': u'47817', # Can also be None
  u'created_at': u'2011-12-04 05:08:29.229652',
  u'id': u'41053',
  u'latitude': u'38.1850031143',
  u'longitude': u'-122.1861015833',
  u'user_id': u'3891'
}

users = [<user_id>]

observation_photo{
  u'medium_url': u'http://static.inaturalist.org/photos/1596277/medium.jpg?1425545517',
  u'observation_id': u'1272361'
}

taxa{
  u'ancestry': u'48460/1/2/355675/3/7251/559248/10093',
  u'iconic_taxon_id': u'3',
  u'id': u'564898',
  u'is_active': u't',
  u'rank': u'species'
}

"""

from __future__ import absolute_import, division, print_function
import argparse
from collections import Counter
import datetime
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

        self.taxon_id_to_taxon = {taxon['id'] : taxon for taxon in self.taxa}
        self.ob_id_to_ob = {ob['id'] : ob for ob in self.observations}
        self.iden_id_to_iden = {iden['id'] : iden for iden in self.identifications}

        self._sanity_check_data()

    def _sanity_check_data(self):
        """ Ensure that all identifications have corresponding taxons.
        """

        # We could have some funky taxa (like `life`)
        taxa_to_keep = []
        obs_taxon_ids = [ob['community_taxon_id'] for ob in self.observations]
        for taxon in self.taxa:
            try:
                taxon['rank_level'] = float(taxon['rank_level'])
                if taxon['id'] in obs_taxon_ids:
                    taxa_to_keep.append(taxon)
            except:
                print("WARNING: bad taxa? Non numeric rank level?")
                print(taxon)
                print()
                continue
        if len(taxa_to_keep) != len(self.taxa):
            self.taxa = taxa_to_keep
            self.taxon_id_to_taxon = {taxon['id'] : taxon for taxon in self.taxa}

            idens_to_keep = [iden for iden in self.identifications if iden['taxon_id'] in self.taxon_id_to_taxon]
            self.identifications = idens_to_keep
            self.iden_id_to_iden = {iden['id'] : iden for iden in self.identifications}

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


    def set_rank_level_as_leaf_level(self, leaf_rank_level=10):
        """ Set a specific rank level to be the leaf level of the taxonomy.
        Map all lower ranks up to that level. Modify the identifications to
        reflect the mapping.
        """

        taxon_id_to_taxon = {taxon['id'] : taxon for taxon in self.taxa}
        taxon_id_to_rank_level = {taxon['id'] : taxon['rank_level']
                                  for taxon in self.taxa}

        # This will hold taxon ids at lower rank level to their
        # corresponding taxon at the `leaf_rank_level`
        taxon_id_to_remapped_taxon = {}
        taxa_ids_at_lower_rank = set()
        taxa_ids_not_remapped = set()
        for taxon in self.taxa:
            if taxon['rank_level'] < leaf_rank_level:
                taxa_ids_at_lower_rank.add(taxon['id'])
                ancestry = taxon['ancestry']
                try:
                    ancestry_ids = ancestry.split('/')
                except:
                    print("WARNING: bad taxa? No ancestors found, but has a \
                           lower rank level than the one specified?")
                    print(taxon)
                    print()
                    taxa_ids_not_remapped.add(taxon['id'])
                    continue

                ancestry_ids.reverse()
                found_higher_ancestor = False
                for ancestor_id in ancestry_ids:
                    ancestor_id = int(ancestor_id)
                    if ancestor_id in taxon_id_to_rank_level:
                        if taxon_id_to_rank_level[ancestor_id] >= leaf_rank_level:
                            taxon_id_to_remapped_taxon[taxon['id']] = taxon_id_to_taxon[ancestor_id]
                            found_higher_ancestor = True
                            break
                if not found_higher_ancestor:
                    taxa_ids_not_remapped.add(taxon['id'])
            else:
                taxon_id_to_remapped_taxon[taxon['id']] = taxon

        # Some taxa might not have been remapped
        if len(taxa_ids_not_remapped) > 0:
            print("WARNING: Found %d taxa that could not be remapped to the rank level of %d" % (len(taxa_ids_not_remapped), leaf_rank_level))
            for taxon_id in taxa_ids_not_remapped:
                print(self.taxon_id_to_taxon[taxon_id])
            print()

            taxa_to_keep = [taxon for taxon in self.taxa if taxon['id'] not in taxa_ids_not_remapped]
            self.taxa = taxa_to_keep
            self.taxon_id_to_taxon = {taxon['id'] : taxon for taxon in self.taxa}

            idens_to_keep = [iden for iden in self.identifications if iden['taxon_id'] not in taxa_ids_not_remapped]
            self.identifications = idens_to_keep

        # Remap the identifications
        for identification in self.identifications:
            if identification['taxon_id'] in taxa_ids_at_lower_rank:
                identification['taxon_id'] = taxon_id_to_remapped_taxon[identification['taxon_id']]['id']
            else:
                if identification['taxon_id'] not in taxon_id_to_rank_level.keys():
                    self.identifications.remove(identification)
                    #assert taxon_id_to_rank_level[identification['taxon_id']] >= leaf_rank_level

        # Delete the remapped taxons
        taxa_to_keep = []
        for taxa in self.taxa:
            if taxa['id'] not in taxa_ids_at_lower_rank:
                taxa_to_keep.append(taxa)
        self.taxa = taxa_to_keep
        self.taxon_id_to_taxon = {taxon['id'] : taxon for taxon in self.taxa}

    def create_flat_taxonomy(self, leaf_rank_level='10'):
        """ Remove taxa not at this rank level. Remove identifications not
        at this rank level.
        """

        taxa_to_keep = [taxon for taxon in self.taxa if taxon['rank_level'] == leaf_rank_level]
        self.taxa = taxa_to_keep
        self.taxon_id_to_taxon = {taxon['id'] : taxon for taxon in self.taxa}

        # Filter the identifications
        idens_to_keep = [iden for iden in self.identifications if iden['taxon_id'] in self.taxon_id_to_taxon]
        self.identifications = idens_to_keep
        self.iden_id_to_iden = {iden['id'] : iden for iden in self.identifications}


    def remove_non_active_taxa(self):
        """ Remove non active taxa and identifications.
        """

        non_active_taxa_ids = set([taxon['id'] for taxon in self.taxa if taxon['is_active'] != True])

        # Filter the taxa
        taxa_to_keep = [taxon for taxon in self.taxa if taxon['id'] not in non_active_taxa_ids]
        self.taxa = taxa_to_keep
        self.taxon_id_to_taxon = {taxon['id'] : taxon for taxon in self.taxa}

        # Filter the identifications
        idens_to_keep = [iden for iden in self.identifications if iden['taxon_id'] not in non_active_taxa_ids]
        self.identifications = idens_to_keep
        self.iden_id_to_iden = {iden['id'] : iden for iden in self.identifications}

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
                        cat = datetime.datetime.strptime(created_at, '%Y-%m-%dT%H:%M:%S.%fZ')
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

    def remove_obs_with_no_photos(self):
        """ Remove observations that don't have any photos.
        """

        ob_ids_with_urls = set([ob_image['observation_id'] for ob_image in self.observation_photos])
        obs_to_keep = [ob for ob in self.observations if ob['id'] in ob_ids_with_urls]
        self.observations = obs_to_keep
        self.ob_id_to_ob = {ob['id'] : ob for ob in self.observations}

        idens_to_keep = [iden for iden in self.identifications if iden['observation_id'] in self.ob_id_to_ob]
        self.identifications = idens_to_keep
        self.iden_id_to_iden = {iden['id'] : iden for iden in self.identifications}

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


    def estimate_taxa_priors(self, include_taxa_ids=None):
        """ Use the current identifications and the corresponding `community_taxon_id`
        from the observations to estimate taxa priors.
        NOTE: The number of taxa represented in the identifications might be less than the
        total number of taxa.
        """

        taxon_id_working_set = set([iden['taxon_id'] for iden in self.identifications])
        if include_taxa_ids is not None:
            taxon_id_working_set.update(include_taxa_ids)
        community_taxon_ids = []
        for iden in self.identifications:
            ob = self.ob_id_to_ob[iden['observation_id']]
            cid = ob['community_taxon_id']
            if cid is not None:
                if cid in taxon_id_working_set:
                    community_taxon_ids.append(cid)
        taxon_counts = Counter(community_taxon_ids)

        # fill in any missing taxa
        for taxon_id in taxon_id_working_set:
            if taxon_id not in taxon_counts:
                taxon_counts[taxon_id] = 1


        total = float(sum(taxon_counts.values()))
        return {taxon_id : c / total for taxon_id, c in taxon_counts.items()}

    def create_dataset(self, leaf_taxa_priors):

        taxon_id_to_class_label = {taxon_id : label for label, taxon_id in enumerate(leaf_taxa_priors)}

        dataset = {
            'num_classes' : len(taxon_id_to_class_label),
            'inat_taxon_id_to_class_label' : taxon_id_to_class_label,
            'global_class_priors' : list(leaf_taxa_priors.values())
        }

        workers = {}
        for iden in self.identifications:
            if iden['user_id'] not in workers:
                workers[iden['user_id']] = {
                    'id' : iden['user_id']
                }

        ob_id_to_urls = {ob['id'] : [] for ob in self.observations}
        for ob_image in self.observation_photos:
            if ob_image['observation_id'] in ob_id_to_urls:
                ob_id_to_urls[ob_image['observation_id']].append(ob_image['native_original_image_url'])

        images = {}
        for ob in self.observations:
            images[ob['id']] = {
                'id' : ob['id'],
                'created_at' : ob['created_at'],
                'url' : ob_id_to_urls[ob['id']][0],
                'urls' : ob_id_to_urls[ob['id']]
            }

        annos = []
        for iden in self.identifications:

            taxon_id = iden['taxon_id']
            if taxon_id in taxon_id_to_class_label:
                worker_label = taxon_id_to_class_label[taxon_id]
            else:
                assert False

            annos.append({
                'anno' : {
                    'gtype' : 'multiclass',
                    'label' : worker_label
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

    with open(os.path.join(archive_dir, 'observations.json')) as f:
        observations = json.load(f)

    with open(os.path.join(archive_dir, 'observation_photos.json')) as f:
        observation_photos = json.load(f)

    with open(os.path.join(archive_dir, 'identifications.json')) as f:
        identifications = json.load(f)

    with open(os.path.join(archive_dir, 'taxa.json')) as f:
        taxa = json.load(f)

    with open(os.path.join(archive_dir, 'users.json')) as f:
        users = json.load(f)
    
    for d in identifications:
        for k, v in d.items():
            if v == 't':
                d[k] = True
    
    taxa = [d for d in taxa if (d['is_active'] == True and d['ancestry'] != None) ]
    
    # Build the observation label prediction dataset
    ob_inat = iNaturalistDataset(observations, observation_photos, identifications, taxa, users)
    ob_inat.set_rank_level_as_leaf_level(leaf_rank_level=10)
    ob_inat.create_flat_taxonomy(leaf_rank_level=10)
    ob_inat.remove_non_active_taxa()
    ob_inat.keep_current_identifications()
    ob_inat.remove_obs_with_no_photos()
    ob_inat.enforce_min_identifications(min_identifications=2)
    ob_inat.enforce_max_observations(args.max_observations)
    ob_ids = [ob['id'] for ob in ob_inat.observations] # Make sure the worker dataset has the same obs

    # Build the worker skill prediction dataset
    worker_inat = iNaturalistDataset(observations, observation_photos, identifications, taxa, users)
    worker_inat.set_rank_level_as_leaf_level(leaf_rank_level=10)
    worker_inat.create_flat_taxonomy(leaf_rank_level=10)
    worker_inat.remove_non_active_taxa()
    worker_inat.keep_specific_observations(ob_ids)
    worker_inat.keep_one_identification_per_user_per_observation(keep_index=0)
    worker_inat.remove_obs_with_no_photos()
    worker_inat.enforce_min_identifications(min_identifications=2)
    worker_inat.enforce_max_observations(args.max_observations)

    # We want to reconcile the taxa priors between the image label and worker skill dataset
    # This way the labels will match up
    taxon_id_working_set = set([iden['taxon_id'] for iden in ob_inat.identifications + worker_inat.identifications])

    taxa_priors = ob_inat.estimate_taxa_priors(include_taxa_ids=taxon_id_working_set)
    ob_label_pred_dataset = ob_inat.create_dataset(taxa_priors)

    label_pred_output_path = os.path.join(output_dir, 'observation_label_pred_dataset.json')
    with open(label_pred_output_path, 'w') as f:
        json.dump(ob_label_pred_dataset, f)

    #worker_skill_taxa_priors = worker_inat.estimate_taxa_priors(include_taxa_ids=taxon_id_working_set)
    worker_skill_pred_dataset = worker_inat.create_dataset(taxa_priors)

    worker_skill_pred_output_path = os.path.join(output_dir, 'worker_skill_pred_dataset.json')
    with open(worker_skill_pred_output_path, 'w') as f:
        json.dump(worker_skill_pred_dataset, f)

    # Build a "testing" dataset.
    inat = iNaturalistDataset(observations, observation_photos, identifications, taxa, users)
    inat.set_rank_level_as_leaf_level(leaf_rank_level=10)
    inat.create_flat_taxonomy(leaf_rank_level=10)
    inat.remove_non_active_taxa()
    inat.keep_current_identifications()
    inat.remove_obs_with_no_photos()
    inat.enforce_min_identifications(min_identifications=1)
    inat.enforce_max_observations(args.max_observations)
    taxa_priors = inat.estimate_taxa_priors()
    test_dataset = inat.create_dataset(taxa_priors)

    test_output_path = os.path.join(output_dir, 'test_dataset.json')
    with open(test_output_path, 'w') as f:
        json.dump(test_dataset, f)

if __name__ == '__main__':
    main()
