#!/usr/bin/env python
# -*- coding: utf-8 -*-

import glob     # Import glob to easily loop over files
import pympi    # Import pympi to work with elan files
import string   # Import string to get the punctuation data
import subprocess

def writeEaf(filename, filepath):
    eafob = pympi.Elan.Eaf()
    #print filepath + filename + '.mp4'
    eafob.add_linked_file(filepath + filename + '.mp4', mimetype = "mp4")
    eafob.remove_tier('default')
    eafob.add_tier('punching')
    eafob.add_tier('punching prep')
    eafob.add_tier('kicking')
    eafob.add_tier('kicking prep')
    eafob.add_tier('hitting')
    eafob.add_tier('hitting prep')
    eafob.add_tier('shoving')
    eafob.add_tier('shoving prep')
    eafob.add_tier('fist threat')
    eafob.add_tier('pointing')
    eafob.add_tier('pointing and laughing')
    eafob.add_tier('tongue out')
    eafob.add_tier('tongue out and hand at ears')
    pympi.Elan.to_eaf(filepath + filename + '.eaf', eafob, pretty=True)

def getTierNames(eafFile):
    res = list()
    for file_path in glob.glob(str(eafFile)):
        # Initialize the elan file
        eafob = pympi.Elan.Eaf(file_path)
        res.extend(eafob.get_tier_names())
    return res

def getActionList(eafFile):
    res = dict()
    for file_path in glob.glob(str(eafFile)):
        # Initialize the elan file
        eafob = pympi.Elan.Eaf(file_path)
        #print eafob.get_full_time_interval()

        for ort_tier in eafob.get_tier_names():
            res[ort_tier] = list()
            for annotation in eafob.get_annotation_data_for_tier(ort_tier):
                  # We are only interested in the utterance
                start = annotation[0]
                end = annotation[1]

                #print ort_tier + ' ' + repr(start) + ' ' + repr(end)
                res[ort_tier].append((repr(start), repr(end)))

    return res

if __name__ == "__main__":
    # #ort_tier_names = ['Verbal']
    ort_tier_names = ['hitting','kicking']

    # Loop over all elan files the corpusroot subdirectory called eaf
    for file_path in glob.glob('*.eaf'):
        #print corpus_root
        #print file_path

        # Initialize the elan file
        eafob = pympi.Elan.Eaf(file_path)
        #print eafob.get_full_time_interval()

        # Loop over all the defined tiers that contain orthography
        for ort_tier in ort_tier_names:
    #         # If the tier is not present in the elan file spew an error and
    #         # continue. This is done to avoid possible KeyErrors
            if ort_tier not in eafob.get_tier_names():
                print 'WARNING!!!'
                print 'One of the ortography tiers is not present in the elan file'
                print 'namely: {}. skipping this one...'.format(ort_tier)
    #         # If the tier is present we can loop through the annotation data
            else:
                for annotation in eafob.get_annotation_data_for_tier(ort_tier):
                      # We are only interested in the utterance
                    start = annotation[0]
                    end = annotation[1]

                    print ort_tier + ' ' + repr(start) + ' ' + repr(end)