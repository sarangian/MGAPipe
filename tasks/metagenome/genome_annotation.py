
import os
import luigi
import os
import time
import subprocess
import pandas as pd
from luigi import Parameter

from tasks.metagenome.metaAssembly import metagenomeAssembly
from tasks.metagenome.genome_binning import genomeBinning
from tasks.metagenome.bin_refinement import binRefinement
from tasks.metagenome.genome_dereplicate import dRepBins
from tasks.metagenome.genome_dereplicate import conditionBasedDeRep
from tasks.metagenome.genome_dereplicate import conditionFreeDeRep




class GlobalParameter(luigi.Config):
	threads = luigi.Parameter()
	maxMemory = luigi.Parameter()
	projectName = luigi.Parameter()
	domain=luigi.Parameter()
	pe_read_dir=luigi.Parameter()
	adapter=luigi.Parameter()
	seq_platforms=luigi.Parameter()

def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' + directory)

def run_cmd(cmd):
	p = subprocess.Popen(cmd, bufsize=-1,
						 shell=True,
						 universal_newlines=True,
						 stdout=subprocess.PIPE,
						 executable='/bin/bash')
	output = p.communicate()[0]
	return output



class prokka(luigi.Task):
	project_name=GlobalParameter().projectName
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().seq_platforms
	min_contig_length=luigi.IntParameter(default="1500")
	min_genome_length=luigi.IntParameter(default="50000")
	completeness=luigi.IntParameter(default="75")
	contamination=luigi.IntParameter(default="25")
	checkM_method=luigi.ChoiceParameter(default="taxonomy_wf",choices=["taxonomy_wf", "lineage_wf"], var_type=str)
	dRep_method=luigi.ChoiceParameter(choices=["condition_based", "condition_free"], var_type=str)
	genomeName=luigi.Parameter()
	reference_condition=luigi.Parameter()
	contrast_condition=luigi.Parameter()
	'''
	def requires(self):
		if self.dRep_method=="condition_based":
			return [conditionBasedDeRep(pre_process_reads=self.pre_process_reads,
										checkM_method=self.checkM_method,
					   			  		min_contig_length=self.min_contig_length,
										reference_condition=self.reference_condition,
									 	completeness=self.completeness,
									 	contrast_condition=self.contrast_condition,
										contamination=self.contamination)]
		if self.dRep_method=="condition_free":
			return [conditionFreeDeRep(pre_process_reads=self.pre_process_reads,
										contrast_condition=self.contrast_condition,
										checkM_method=self.checkM_method,
										reference_condition=self.reference_condition,
					   			  		min_contig_length=self.min_contig_length,
									 	completeness=self.completeness,
										contamination=self.contamination)]
	'''		
	def requires(self):
		return[]
		'''
		return [dRepBins(dRep_method=self.dRep_method,
						pre_process_reads=self.pre_process_reads,
						checkM_method=self.checkM_method,
					   	min_contig_length=self.min_contig_length,
						reference_condition=self.reference_condition,
						completeness=self.completeness,
						contrast_condition=self.contrast_condition,
						contamination=self.contamination)]
		'''
	def output(self):
		bin_name=self.genomeName.split(".filtered.fa")[0]
		outDir = os.path.join(os.getcwd(), GlobalParameter().projectName, "prokka_"+self.dRep_method,bin_name + "/")
		return {'out': luigi.LocalTarget(outDir , bin_name+".fna" )}
		
						
	def run(self):
		bin_name=self.genomeName.split(".filtered.fa")[0]
		outDir = os.path.join(os.getcwd(), GlobalParameter().projectName, "prokka_"+self.dRep_method,bin_name + "/")

		if self.dRep_method=="condition_based":
			binDir=os.path.join(os.getcwd(),GlobalParameter().projectName,"dRep_bins","dReplicated_bins_condition_based"+"/")
			if not os.path.join(os.getcwd(),GlobalParameter().projectName ,"dRep_bins", "genomes_dRep_by_condition.lst"):
				sys.exit("Run dRepBins with dRep_method --condition_based") 
				#touch(os.path.join(os.getcwd(),GlobalParameter().projectName ,"dRep_bins", "genomes_dRep_by_condition.lst"))


		if self.dRep_method=="condition_free":
			binDir=os.path.join(os.getcwd(), GlobalParameter().projectName,"dRep_bins","dReplicated_bins_condition_free","dereplicated_genomes"+"/")
			if not os.path.join(os.getcwd(),GlobalParameter().projectName ,"dRep_bins", "genomes_dRep_regardless_condition.lst"):
				sys.exit("Run dRepBins with dRep_method --condition_free") 
				#touch(os.path.join(os.getcwd(),GlobalParameter().projectName ,"dRep_bins", "genomes_dRep_regardless_condition.lst"))


		cmd_run_prokka="[ -d  {outDir} ] || mkdir -p {outDir}; " \
				   "prokka {binDir}/{genomeName} " \
				   "--quiet " \
				   "--outdir {outDir} "  \
				   "--prefix {bin_name} " \
				   "--metagenome " \
				   "--kingdom Bacteria " \
				   "--locustag PROKKA " \
				   "--cpus {threads}  --force ".format(binDir=binDir,
				   	genomeName=self.genomeName, 
				   	outDir=outDir, 
				   	bin_name=bin_name,
				   	threads=GlobalParameter().threads)
		print(' ')
		print(' ')
		print("************************************")
		print('Now Annotating:', bin_name)
		print('Annotation OUTPUT:', outDir)
		print('Annotation FILE:', outDir+bin_name+".gff")
		print("*************************************")
		print(' ')
		print("****** NOW RUNNING COMMAND ******: " + cmd_run_prokka)
		print (run_cmd(cmd_run_prokka))
		time.sleep(2)


#####################################################################################################################	



class annotateGenomeBins(luigi.Task):
	project_name=GlobalParameter().projectName
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().seq_platforms
	min_contig_length=luigi.IntParameter(default="1500")
	min_genome_length=luigi.IntParameter(default="50000")
	completeness=luigi.IntParameter(default="75")
	contamination=luigi.IntParameter(default="25")
	checkM_method=luigi.ChoiceParameter(default="taxonomy_wf",choices=["taxonomy_wf", "lineage_wf"], var_type=str)
	dRep_method=luigi.ChoiceParameter(choices=["condition_based", "condition_free"], var_type=str)
	reference_condition=luigi.Parameter()
	contrast_condition=luigi.Parameter()




	def requires(self):
		
		'''
		if self.dRep_method=="condition_based":
			dRep_bins=os.path.join(os.getcwd(),GlobalParameter().projectName ,"dRep_bins")
			createFolder(dRep_bins)
			genome_list=os.path.join(os.getcwd(),GlobalParameter().projectName ,"dRep_bins", "genomes_dRep_by_condition.lst")
			create_list="touch {genome_list}".format(genome_list=genome_list)
			print(run_cmd(create_list))

		if self.dRep_method=="condition_free":
			dRep_bins=os.path.join(os.getcwd(),GlobalParameter().projectName ,"dRep_bins")
			createFolder(dRep_bins)
			genome_list=os.path.join(os.getcwd(),GlobalParameter().projectName ,"dRep_bins", "genomes_dRep_regardless_condition.lst")
			create_list="touch {genome_list}".format(genome_list=genome_list)
			print(run_cmd(create_list))

		'''
		if self.dRep_method=="condition_based":
			return [

					[prokka(checkM_method=self.checkM_method,
						dRep_method=self.dRep_method,
					   pre_process_reads=self.pre_process_reads,
					   reference_condition=self.reference_condition,
					   completeness=self.completeness,
					   contrast_condition=self.contrast_condition,
					   contamination=self.contamination,
					   min_contig_length=self.min_contig_length,
						genomeName=i)
						for i in [line.strip()
							  for line in
							  open((os.path.join(os.getcwd(),GlobalParameter().projectName ,"dRep_bins", "genomes_dRep_by_condition.lst")))]]]

		if self.dRep_method=="condition_free":

			return [
										
					[prokka(checkM_method=self.checkM_method,dRep_method=self.dRep_method,
					   pre_process_reads=self.pre_process_reads,contrast_condition=self.contrast_condition,
					   completeness=self.completeness,reference_condition=self.reference_condition,
					   contamination=self.contamination,
					   min_contig_length=self.min_contig_length,
						genomeName=i)
					for i in [line.strip()
							  for line in
							  open((os.path.join(os.getcwd(),GlobalParameter().projectName,"dRep_bins", "genomes_dRep_regardless_condition.lst")))]]]
		
	def output(self):
		timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
		return luigi.LocalTarget(os.path.join(os.getcwd(),"task_logs",'task.genome.dereplication.complete.{t}'.format(
			t=timestamp)))

	def run(self):
		timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
		with self.output().open('w') as outfile:
			outfile.write('Genome Dereplication finished at {t}'.format(t=timestamp))
