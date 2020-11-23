#!/usr/bin/env python3
import luigi
import sys



from tasks.readCleaning.rawReadQC import readqc
from tasks.readCleaning.rawReadQC import rawReadsQC
from tasks.readCleaning.preProcessReads import cleanFastq
from tasks.readCleaning.preProcessReads import cleanReads
from tasks.readCleaning.preProcessReads import filtlong


from tasks.utility.luigiconfig import configureProject

from tasks.metagenome import metaAssembly
from tasks.metagenome import genome_binning
from tasks.metagenome import bin_refinement
from tasks.metagenome import genome_dereplicate
from tasks.metagenome import genome_annotation
from tasks.metagenome import genome_enrichment
from tasks.metagenome import metagenome_profiling


#from tasks.metagenome import meta_profile
#from tasks.metagenome import metagenome_analysis

#from tasks.annotation import prokaryotic_annotation
#from tasks.annotation.make_tx_to_gene import makeTx2Gene


if __name__ == '__main__':

    luigi.run()
