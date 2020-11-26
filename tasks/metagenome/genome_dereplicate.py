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

def condition_based_derep(input_file,ref_cond,cont_cond):
	df = pd.read_table(input_file, sep='\t+', engine='python',header=None)
	df[1:]
	df.columns = ['samples', 'conditions']
	df1 = df[["samples", "conditions"]]
	df2 = df1.set_index("samples", drop = False)
	df_groups = df1.groupby('conditions')


	reference_condition=ref_cond
	contrast_condition=cont_cond


	dRep_bin_path=os.path.join(os.getcwd(), GlobalParameter().projectName, "dRep_bins"+ "/")

	destDir_reference_condition=os.path.join(os.getcwd(), GlobalParameter().projectName, "dRep_bins", reference_condition +"/")
	destDir_contrast_condition=os.path.join(os.getcwd(), GlobalParameter().projectName, "dRep_bins", contrast_condition +"/")


	reference_condition_file=os.path.join(destDir_reference_condition, reference_condition +".txt")
	contrast_condition_file=os.path.join(destDir_contrast_condition, contrast_condition +".txt")

   
	sourceDir=os.path.join(os.getcwd(),GlobalParameter().projectName, "bin_refinement"+ "/")


	ref_cond_folder=dRep_bin_path+"/"+reference_condition
	con_cond_folder=dRep_bin_path+"/"+contrast_condition

	createFolder(ref_cond_folder)
	createFolder(con_cond_folder)


	split_group_a=df_groups.get_group(reference_condition)
	group = split_group_a[["samples"]]
	samples=group.to_csv(reference_condition_file, header=False, index=False)


	split_group_b=df_groups.get_group(contrast_condition)
	group = split_group_b[["samples"]]
	samples=group.to_csv(contrast_condition_file, header=False, index=False)

	base_dir=os.getcwd()       
 
	fh_reference_condition=open(reference_condition_file)
	for sample in fh_reference_condition:
		sample=sample.rstrip()
		head, tail = os.path.split(os.path.split(reference_condition_file)[0])
		destPath=head+"/" + tail
		destPath=destPath.rstrip()

		refSourcePath=os.path.join(base_dir,GlobalParameter().projectName,"bin_refinement",sample, "filtered_bins")
		refSourcePath=refSourcePath.rstrip()

		cmd_copy= " cd {refSourcePath} ; cp *.fa {destPath} &>/dev/null".format(refSourcePath=refSourcePath,destPath=destPath)

				
		print("****** NOW RUNNING COMMAND ******: " + cmd_copy)
		print(run_cmd(cmd_copy))


	fh_contrast_condition=open(contrast_condition_file)
	for sample in fh_contrast_condition:
		sample=sample.rstrip()
		print (sample)
		head, tail = os.path.split(os.path.split(contrast_condition_file)[0])
		destPath=head+"/" + tail
		destPath=destPath.rstrip()

		conSourcePath=os.path.join(base_dir,GlobalParameter().projectName,"bin_refinement",sample, "filtered_bins")
		conSourcePath=conSourcePath.rstrip()

		print("copying",sample)
		cmd_copy= "cd  {conSourcePath} ; cp *.fa {destPath} &>/dev/null".format(conSourcePath=conSourcePath,destPath=destPath)		  
		#cmd_copy= " find . -name '*.fa' -exec cp {curly} {destPath} {end}".format(curly=curly,destPath=destPath,end=end)
		print("****** NOW RUNNING COMMAND ******: " + cmd_copy)
		print(run_cmd(cmd_copy))



def condition_free_derep(input_file,ref_cond,cont_cond):

	reference_condition=ref_cond
	contrast_condition=cont_cond


	destPath=os.path.join(os.getcwd(), GlobalParameter().projectName,"dRep_bins", "input_bins_cond_free")
	destPath=destPath.rstrip()
	createFolder(destPath)

	sample_file=os.path.join(os.getcwd(),"config","pe_samples.lst")

	fh_samples=open(sample_file)
	for sample in fh_samples:
		sample=sample.rstrip()
		head, tail = os.path.split(os.path.split(sample_file)[0])
		sourceDir=os.path.join(os.getcwd(),GlobalParameter().projectName,"bin_refinement",sample,"filtered_bins")
		sourceDir=sourceDir.rstrip()		

		print("copying",sample)
		cmd_copy= " cp {sourceDir}/*.fa {destPath} &>/dev/null".format(sourceDir=sourceDir,destPath=destPath)

		print("****** NOW RUNNING COMMAND ******: " + cmd_copy)
		print(run_cmd(cmd_copy))


class conditionBasedDeRep(luigi.Task):
	projectName=GlobalParameter().projectName
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
	reference_condition=luigi.Parameter()
	contrast_condition=luigi.Parameter()



	def requires(self):
			return [binRefinement(min_contig_length=self.min_contig_length,
								  pre_process_reads=self.pre_process_reads)]
	def output(self):


		#reference_condition_out=os.path.join(os.getcwd(), GlobalParameter().projectName, "dRep_bins", self.reference_condition +"_out"+"/")
		#contrast_condition_out=os.path.join(os.getcwd(), GlobalParameter().projectName, "dRep_bins", self.contrast_condition +"_out"+"/")
		dRep_ref_cond_out_final_folder=os.path.join(os.getcwd(),GlobalParameter().projectName,"dRep_bins", self.reference_condition +"_dReplicated"+"/")
		dRep_con_cond_out_final_folder=os.path.join(os.getcwd(),GlobalParameter().projectName,"dRep_bins", self.contrast_condition +"_dReplicated"+"/")

		'''
		return {'out1': luigi.LocalTarget(reference_condition_out, "data_tables","genomeInfo.csv" ),
			    'out2': luigi.LocalTarget(contrast_condition_out ,"data_tables","genomeInfo.csv" )}
		'''
		return {'out1': luigi.LocalTarget(dRep_ref_cond_out_final_folder, "data_tables","genomeInfo.csv" ),
			    'out2': luigi.LocalTarget(dRep_con_cond_out_final_folder ,"data_tables","genomeInfo.csv" )}

		#dRepBin_folder=os.path.join(os.getcwd(), GlobalParameter().projectName,"dRep_bins"+"/")
		#return {'out1': luigi.LocalTarget(dRepBin_folder + "genomes_dRep_by_condition.lst.lst" )}


	def run(self):

		mapping_file=os.path.join(os.getcwd(), "config","metagenome_group.tsv")
		condition_based_derep(mapping_file,self.reference_condition,self.contrast_condition)

		reference_dRep_path=os.path.join(os.getcwd(),GlobalParameter().projectName,"dRep_bins",self.reference_condition)
		contrast_dRep_path=os.path.join(os.getcwd(),GlobalParameter().projectName,"dRep_bins",self.contrast_condition)

		dRep_ref_cond_out_folder=os.path.join(os.getcwd(),GlobalParameter().projectName,"dRep_bins", self.reference_condition +"_dReplicated"+"/")
		dRep_con_cond_out_folder=os.path.join(os.getcwd(),GlobalParameter().projectName,"dRep_bins", self.contrast_condition +"_dReplicated"+"/")


		cmd_dRep_reference="[ -d  {dRep_ref_cond_out_folder} ] || mkdir -p {dRep_ref_cond_out_folder}; " \
					   "dRep dereplicate {dRep_ref_cond_out_folder} " \
					   "-g {reference_dRep_path}/*.fa " \
					   "--checkM_method {checkM_method} " \
					   "-p {threads}".format(checkM_method=self.checkM_method,threads=self.threads,
											 dRep_ref_cond_out_folder=dRep_ref_cond_out_folder,
											 reference_dRep_path=reference_dRep_path)


		cmd_dRep_contrast="[ -d  {dRep_con_cond_out_folder} ] || mkdir -p {dRep_con_cond_out_folder}; " \
					   "dRep dereplicate {dRep_con_cond_out_folder} " \
					   "-g {contrast_dRep_path}/*.fa " \
					   "--checkM_method {checkM_method} " \
					   "-p {threads}".format(dRep_con_cond_out_folder=dRep_con_cond_out_folder, 
											checkM_method=self.checkM_method,threads=self.threads,
											contrast_dRep_path=contrast_dRep_path)	

		
		merged_bin_folder=os.path.join(os.getcwd(), GlobalParameter().projectName ,"dRep_bins","dReplicated_bins_condition_based"+"/")
		dRep_ref_genomes=os.path.join(dRep_ref_cond_out_folder,"dereplicated_genomes")
		dRep_con_genomes=os.path.join(dRep_con_cond_out_folder,"dereplicated_genomes")

		cmd_merge_bins_by_condition="[ -d  {merged_bin_folder} ] || mkdir -p {merged_bin_folder} ; cd {merged_bin_folder} ; " \
									   "cp {dRep_ref_genomes}/*.fa {merged_bin_folder} ; " \
									   "cp {dRep_con_genomes}/*.fa {merged_bin_folder} ;".format(merged_bin_folder=merged_bin_folder,
										dRep_ref_genomes=dRep_ref_genomes,dRep_con_genomes=dRep_con_genomes)
		

		print("****** NOW RUNNING COMMAND ******: " + cmd_dRep_reference)
		print(run_cmd(cmd_dRep_reference))

		print("****** NOW RUNNING COMMAND ******: " + cmd_dRep_contrast)
		print(run_cmd(cmd_dRep_contrast))

		print("****** NOW RUNNING COMMAND ******: " + cmd_merge_bins_by_condition)
		print(run_cmd(cmd_merge_bins_by_condition))


		merged_bin_folder=os.path.join(os.getcwd(), GlobalParameter().projectName ,"dRep_bins","dReplicated_bins_condition_based"+"/")

		genomes_dRep_by_condition_list=os.listdir(merged_bin_folder)
		#print(genomes_dRep_by_condition_list)
		genome_list_path=os.path.join(os.getcwd(), GlobalParameter().projectName,"dRep_bins","genomes_dRep_by_condition.lst")
		with open(genome_list_path, 'w') as output:
			for genome in genomes_dRep_by_condition_list:
				output.write('%s\n' % genome)

class conditionFreeDeRep(luigi.Task):
	projectName=GlobalParameter().projectName
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
	reference_condition=luigi.Parameter()
	contrast_condition=luigi.Parameter()
	

	def requires(self):
			return [binRefinement(min_contig_length=self.min_contig_length,
								  pre_process_reads=self.pre_process_reads)]
	def output(self):


		dReplicated_bins_condition_free=os.path.join(os.getcwd(), GlobalParameter().projectName,"dRep_bins", "dReplicated_bins_condition_free"+"/")
		#dRepBin_folder=os.path.join(os.getcwd(), GlobalParameter().projectName,"dRep_bins"+"/")
		
		return {'out1': luigi.LocalTarget(dReplicated_bins_condition_free + "data_tables/" + "genomeInfo.csv" )}
		#return {'out1': luigi.LocalTarget(dRepBin_folder + "genomes_dRep_regardless_condition.lst" )}


	def run(self):

		sample_file=os.path.join(os.getcwd(), "config","pe_samples.lst")
		condition_free_derep(sample_file,self.reference_condition,self.contrast_condition)


		dReplicated_bins_condition_free=os.path.join(os.getcwd(),GlobalParameter().projectName,"dRep_bins","dReplicated_bins_condition_free"+"/")
		dReplicated_bins_input=os.path.join(os.getcwd(), GlobalParameter().projectName,"dRep_bins", "input_bins_cond_free"+"/")



		cmd_dRep="[ -d  {dReplicated_bins_condition_free} ] || mkdir -p {dReplicated_bins_condition_free}; " \
					   "dRep dereplicate {dReplicated_bins_condition_free} " \
					   "-g {dReplicated_bins_input}/*.fa " \
					   "--checkM_method {checkM_method} " \
					   "-p {threads}".format(checkM_method=self.checkM_method,
											 threads=self.threads,
											 dReplicated_bins_condition_free=dReplicated_bins_condition_free,
											 dReplicated_bins_input=dReplicated_bins_input)


		print("****** NOW RUNNING COMMAND ******: " + cmd_dRep)
		print(run_cmd(cmd_dRep))



		merged_bin_folder=os.path.join(os.getcwd(), GlobalParameter().projectName,"dRep_bins", "dReplicated_bins_condition_free","dereplicated_genomes" +"/")
		genomes_dRep_regardless_condition_list=os.listdir(merged_bin_folder)
		print(genomes_dRep_regardless_condition_list)
		genome_list_path=os.path.join(os.getcwd(), GlobalParameter().projectName,"dRep_bins", "genomes_dRep_regardless_condition.lst")
		with open(genome_list_path, 'w') as output:
			for genome in genomes_dRep_regardless_condition_list:
				output.write('%s\n' % genome)
#######################################################################################################
class dRepBins(luigi.Task):
	projectName=GlobalParameter().projectName
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
	dRep_method=luigi.ChoiceParameter(default="condition_based",choices=["condition_based", "condition_free"], var_type=str)
	reference_condition=luigi.Parameter()
	contrast_condition=luigi.Parameter()


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


	def output(self):
		timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
		return luigi.LocalTarget(os.path.join(os.getcwd(),"task_logs",'task.bin.de-replication.complete.{t}'.format(
			t=timestamp)))

	def run(self):
		timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
		with self.output().open('w') as outfile:
			outfile.write('Bin de-replication finished at {t}'.format(t=timestamp))
