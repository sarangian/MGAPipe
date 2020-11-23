
'''import os
import luigi
def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' + directory)

class GlobalParameter(luigi.Config):
	threads = luigi.Parameter()
	maxMemory = luigi.Parameter()
	projectName = luigi.Parameter()
	domain=luigi.Parameter()
	pe_read_dir=luigi.Parameter()
	adapter=luigi.Parameter()
	seq_platforms=luigi.Parameter()

	dRep_bins=os.path.join(os.getcwd(),GlobalParameter().projectName ,"dRep_bins")
	createFolder(dRep_bins)
	genome_list_by_cond=os.path.join(os.getcwd(),GlobalParameter().projectName ,"dRep_bins", "genomes_dRep_by_condition.lst")
	touch(genome_list_by_cond)
	genome_list_cond_free=os.path.join(os.getcwd(),GlobalParameter().projectName ,"dRep_bins", "genomes_dRep_regardless_condition.lst")
	touch(genome_list_cond_free)

'''

#__init__.py

#Version

__version__="1.0.0"

from configparser import ConfigParser
import os
import sys
current_folder=os.path.join(os.getcwd())
luigi_config=os.path.join(os.getcwd(),"luigi.cfg")

if not os.path.isfile(luigi_config):
	print(f'Please prepare the luigi configuration file first')
	sys.exit(0)

'''
configure = ConfigParser()
print(configure.read('luigi.cfg'))
print("Sections: ", configure.sections())

projectDir=configure.get('GlobalParameter','projectDir')

dRep_bins=os.path.join(os.getcwd(),projectDir,"dRep_bins")

if len(dRep_bins)==0:
	print(f'Please de-relicate genome using conditionBasedDeRep or conditionFreeDeRep and then proceed')
	sys.exit(0)
'''


