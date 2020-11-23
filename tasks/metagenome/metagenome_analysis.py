import os
import luigi
import os
import time
import subprocess
import pandas as pd
from luigi import Parameter


from tasks.readCleaning.preProcessReads import bbduk
from tasks.readCleaning.reFormatReads import reformat


suffix='.fastq'
old_left_read_name_suffix = '_R1.fastq'
old_rigt_read_name_suffix = '_R2.fastq'
new_left_read_name_suffix = '_1.fastq'
new_rigt_read_name_suffix = '_2.fastq'

cleaned_read_folder = os.path.join(os.getcwd(), "CleanedReads", "Cleaned_PE_Reads" + "/")
verified_read_folder = os.path.join(os.getcwd(), "VerifiedReads", "Verified_PE_Reads" + "/")
symlink_read_folder = os.path.join(os.getcwd(), "MetagenomeAnalysis", "input_reads" + "/")
genome_bin_folder =  os.path.join(os.getcwd(), "MetagenomeAnalysis", "binning" + "/")

task_log_folder =  os.path.join(os.getcwd(), "MetagenomeAnalysis", "task_logs" + "/")


def createFolder(directory):
	try:
		if not os.path.exists(directory):
			os.makedirs(directory)
	except OSError:
		print ('Error: Creating directory. ' + directory)
createFolder(symlink_read_folder)
createFolder(task_log_folder)

def run_cmd(cmd):
	p = subprocess.Popen(cmd, bufsize=-1,
						 shell=True,
						 universal_newlines=True,
						 stdout=subprocess.PIPE,
						 executable='/bin/bash')
	output = p.communicate()[0]
	return output
def clean_read_symlink(input_file):
	with open(input_file) as ifh:
		sample_name_list = ifh.read().splitlines()
		dir = os.listdir(symlink_read_folder)
		if len(dir) != 0: 
				print("symolic link directory not empty" )
				cmd_rm_symlink_dir="rm -rf {symlink_read_folder}".format(symlink_read_folder=symlink_read_folder) 
				print("****** NOW RUNNING COMMAND ******: " + cmd_rm_symlink_dir)
				print (run_cmd(cmd_rm_symlink_dir))
				createFolder(symlink_read_folder)		

		for sample in sample_name_list:	
			if not (len(dir) ==  len(sample_name_list)): 
				os.symlink(os.path.join(cleaned_read_folder,sample + old_left_read_name_suffix),os.path.join(symlink_read_folder,sample+new_left_read_name_suffix))
				os.symlink(os.path.join(cleaned_read_folder,sample + old_rigt_read_name_suffix),os.path.join(symlink_read_folder,sample+new_rigt_read_name_suffix))	
		print('symlinks created successfully')
		cmd_tree="tree {symlink_read_folder}".format(symlink_read_folder=symlink_read_folder)
		print (run_cmd(cmd_tree))

def verified_read_symlink(input_file):
	with open(input_file) as ifh:
		sample_name_list = ifh.read().splitlines()
		dir = os.listdir(symlink_read_folder)
		if len(dir) != 0: 
				print("symolic link directory not empty" )
				cmd_rm_symlink_dir="rm -rf {symlink_read_folder}".format(symlink_read_folder=symlink_read_folder) 
				print("****** NOW RUNNING COMMAND ******: " + cmd_rm_symlink_dir)
				print (run_cmd(cmd_rm_symlink_dir))
				createFolder(symlink_read_folder)		

		for sample in sample_name_list:	
			if not (len(dir) ==  len(sample_name_list)): 
				os.symlink(os.path.join(verified_read_folder,sample + old_left_read_name_suffix),os.path.join(symlink_read_folder,sample+new_left_read_name_suffix))
				os.symlink(os.path.join(verified_read_folder,sample + old_rigt_read_name_suffix),os.path.join(symlink_read_folder,sample+new_rigt_read_name_suffix))	
		print('symlinks created successfully')
		cmd_tree="tree {symlink_read_folder}".format(symlink_read_folder=symlink_read_folder)
		print (run_cmd(cmd_tree))

def prepare_binning_input(pefile):
	with open(pefile) as fh:
		sample_name_list = fh.read().splitlines()
		left_read_name_suffix = '_1.fastq'
		right_read_name_suffix = '_2.fastq'
		read_folder = os.path.join(os.getcwd(), "MetagenomeAnalysis", "input_reads" + "/")
		left_read_name_list = [read_folder + x + left_read_name_suffix for x in sample_name_list]
		right_read_name_list = [read_folder + x + right_read_name_suffix for x in sample_name_list]
		result = [item for sublist in zip(left_read_name_list, right_read_name_list) for item in sublist]
		binning_input = " ".join(result)
		return binning_input


###########################################

def condition_based_derep(input_file,ref_cond,cont_cond):
	df = pd.read_table(input_file, sep='\t+', engine='python')
	df[1:]
	df.columns = ['samples', 'conditions']
	df1 = df[["samples", "conditions"]]
	df2 = df1.set_index("samples", drop = False)
	df_groups = df1.groupby('conditions')
	

	reference_condition=ref_cond
	contrast_condition=cont_cond

	path=os.path.join(os.getcwd() +"/"+ "MetagenomeAnalysis" +"/"+"dRep_bins"+ "/")

	destDir_reference_condition=os.path.join(os.getcwd() +"/"+ "MetagenomeAnalysis" +"/"+"dRep_bins"+ "/"+reference_condition+"/")
	destDir_contrast_condition=os.path.join(os.getcwd() +"/"+ "MetagenomeAnalysis" +"/"+"dRep_bins"+ "/"+contrast_condition+"/")


	reference_condition_file=os.path.join(os.getcwd() +"/"+ "MetagenomeAnalysis" +"/"+"dRep_bins"+ "/"+reference_condition+"/"+reference_condition+".txt")
	contrast_condition_file=os.path.join(os.getcwd() +"/"+ "MetagenomeAnalysis" +"/"+"dRep_bins"+ "/"+contrast_condition+"/"+contrast_condition+".txt")

   
	sourceDir=os.path.join(os.getcwd() +"/"+ "MetagenomeAnalysis" +"/"+"bin_refinement"+ "/")


	ref_cond_folder=path+"/"+reference_condition
	con_cond_folder=path+"/"+contrast_condition

	createFolder(ref_cond_folder)
	createFolder(con_cond_folder)


	split_group_a=df_groups.get_group(reference_condition)
	group = split_group_a[["samples"]]
	path=os.path.join(os.getcwd() +"/"+ "MetagenomeAnalysis" +"/"+"dRep_bins"+ "/")
	samples=group.to_csv(path+"/"+reference_condition+ "/"+ reference_condition+".txt", header=False, index=False)

	split_group_b=df_groups.get_group(contrast_condition)
	group = split_group_b[["samples"]]
	samples=group.to_csv(path+"/"+contrast_condition+ "/"+ contrast_condition+".txt", header=False, index=False)

	base_dir=os.getcwd()       
 
	fh_reference_condition=open(reference_condition_file)
	for sample in fh_reference_condition:
		sample=sample.rstrip()
		head, tail = os.path.split(os.path.split(reference_condition_file)[0])
		destPath=head+"/" + tail
		destPath=destPath.rstrip()

		refSourcePath=os.path.join(base_dir,"MetagenomeAnalysis","bin_refinement",sample, "filtered_bins")
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

		conSourcePath=os.path.join(base_dir,"MetagenomeAnalysis","bin_refinement",sample, "filtered_bins")
		conSourcePath=conSourcePath.rstrip()

		print("copying",sample)
		cmd_copy= "cd  {conSourcePath} ; cp *.fa {destPath} &>/dev/null".format(conSourcePath=conSourcePath,destPath=destPath)		  
		#cmd_copy= " find . -name '*.fa' -exec cp {curly} {destPath} {end}".format(curly=curly,destPath=destPath,end=end)
		print("****** NOW RUNNING COMMAND ******: " + cmd_copy)
		print(run_cmd(cmd_copy))



def condition_free_derep(input_file,ref_cond,cont_cond):

	reference_condition=ref_cond
	contrast_condition=cont_cond


	destPath=os.path.join(os.getcwd(), "MetagenomeAnalysis","dRep_bins", "dReplicated_bins")
	destPath=destPath.rstrip()
	createFolder(destPath)

	sample_file=os.path.join(os.getcwd() +"/"+ "sample_list" +"/"+"pe_samples.lst")

	fh_samples=open(sample_file)
	for sample in fh_samples:
		sample=sample.rstrip()
		head, tail = os.path.split(os.path.split(sample_file)[0])
		
		sourceDir=os.path.join(os.getcwd(),"MetagenomeAnalysis","bin_refinement",sample,"filtered_bins")
		sourceDir=sourceDir.rstrip()		

		print("copying",sample)
		cmd_copy= " cp {sourceDir}/*.fa {destPath} &>/dev/null".format(sourceDir=sourceDir,destPath=destPath)

		print("****** NOW RUNNING COMMAND ******: " + cmd_copy)
		print(run_cmd(cmd_copy))
#################################################################################################################
############################################
class GlobalParameter(luigi.Config):
	read_library_type=luigi.Parameter()
	threads = luigi.Parameter()
	maxMemory = luigi.Parameter()
	adapter=luigi.Parameter()

class singleSampleAssembly(luigi.Task):
	project_name=luigi.Parameter(default="MetagenomeAnalysis")
	#assembly_type=luigi.ChoiceParameter(default="single",choices=["single", "co"], var_type=str)
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().read_library_type
	min_contig_length=luigi.IntParameter(default="1500")
	sampleName = luigi.Parameter(description="name of the sample to be analyzed. (string)")


	def requires(self):
		if self.pre_process_reads=="yes":
			return [bbduk(read_library_type="pe",
					  sampleName=i)
				for i in [line.strip()
						  for line in
						  open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]]
		
		if self.pre_process_reads=="no":
			return [reformat(read_library_type="pe",
					  sampleName=i)
				for i in [line.strip()
						  for line in
						  open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]]
		

	def output(self):

		assembled_metagenome_folder = os.path.join(os.getcwd(), "MetagenomeAnalysis", "assembly" )
		return {'out': luigi.LocalTarget(assembled_metagenome_folder + "/" + self.sampleName + "/" + self.sampleName+".contigs.fa")}
		
	def run(self):

		assembled_metagenome_folder = os.path.join(os.getcwd(), "MetagenomeAnalysis", "assembly" + "/")		

		input_sample_list = os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")
		symlink_read_folder = os.path.join(os.getcwd(), "MetagenomeAnalysis", "input_reads" + "/")

		if self.pre_process_reads == "yes":
			clean_read_symlink(input_sample_list)

		if self.pre_process_reads == "no":
			verified_read_symlink(input_sample_list)

		cmd_run_assembler = "[ -d  {assembled_metagenome_folder} ] || mkdir -p {assembled_metagenome_folder}; cd {assembled_metagenome_folder}; " \
							 "megahit " \
							 "-1 {symlink_read_folder}{sample}_1.fastq " \
							 "-2 {symlink_read_folder}{sample}_2.fastq " \
							 "-m {max_memory} " \
							 "-t {threads} " \
							 "--out-prefix {sample} " \
							 "-o {output_dir} " \
			.format(assembled_metagenome_folder=assembled_metagenome_folder,
					sample=self.sampleName,
					symlink_read_folder=symlink_read_folder,
					output_dir=self.sampleName,
					max_memory=self.max_memory,
					threads=self.threads)

		print("****** NOW RUNNING COMMAND ******: " + cmd_run_assembler)
		print (run_cmd(cmd_run_assembler))



class metagenomeAssembly(luigi.Task):
	project_name=luigi.Parameter(default="MetagenomeAnalysis")
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().read_library_type
	min_contig_length=luigi.IntParameter(default="1500")

	def requires(self):

		return [singleSampleAssembly(pre_process_reads=self.pre_process_reads,
									 min_contig_length=self.min_contig_length,
						sampleName=i)
					for i in [line.strip()
							  for line in
							  open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]]


	def output(self):
		timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
		return luigi.LocalTarget(os.path.join(os.getcwd(),"MetagenomeAnalysis","task_logs",'task.metagenome.assembly.complete.{t}'.format(
			t=timestamp)))

	def run(self):
		timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
		with self.output().open('w') as outfile:
			outfile.write('Metagenome Assembly finished at {t}'.format(t=timestamp))

######################################################
class estimateCoverage(luigi.Task):
	priority =100
	project_name=luigi.Parameter(default="MetagenomeAnalysis")
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().read_library_type

	def requires(self):
		return [metagenomeAssembly(pre_process_reads=self.pre_process_reads)]

	def output(self):

		inDir = os.path.join(os.getcwd(), "MetagenomeAnalysis", "input_reads" + "/")
		readlist=os.listdir(inDir)

		for read in readlist:	
			if read.endswith("_1.fastq"):
				filename,file_extn=read.split(".",1)[0],read.split('.',1)[1]
				forward_read=read
				reverse_read=read.replace("_1.fastq", "_2.fastq")
				sample=read.split("_1",1)[0]


				coverage_folder=os.path.join(os.getcwd(), "MetagenomeAnalysis", "binning","coverage" )
				return {'out1': luigi.LocalTarget(coverage_folder +"/"+ sample +".bam"),
						'out2': luigi.LocalTarget(coverage_folder +"/"+ sample +".bam.bai"),
						'out3': luigi.LocalTarget(coverage_folder +"/"+ sample +".cov")}


	def run(self):
		
		coverage_folder=os.path.join(os.getcwd(), "MetagenomeAnalysis", "binning","coverage")
		inDir = os.path.join(os.getcwd(), "MetagenomeAnalysis", "input_reads" + "/")

		readlist=os.listdir(inDir)

		for read in readlist:	
			if read.endswith("_1.fastq"):
				filename,file_extn=read.split(".",1)[0],read.split('.',1)[1]
				forward_read=read
				reverse_read=read.replace("_1.fastq", "_2.fastq")

				sample=read.split("_1",1)[0]
				print("Computing Coverage for Sample:",sample)
		  
				assembled_metagenome = os.path.join(os.getcwd(), "MetagenomeAnalysis", "assembly", sample, sample+".contigs.fa" )
				print (assembled_metagenome)


				rum_cmd_coverm = "[ -d  {coverage_folder} ] || mkdir -p {coverage_folder}; " \
						 "coverm contig " \
						 "-c {symlink_read_folder}/{forward_read} {symlink_read_folder}/{reverse_read} "    \
						 "-r {assembled_metagenome} " \
						 "-p minimap2-sr -t {threads} " \
						 "--output-format sparse " \
						 "--bam-file-cache-directory {coverage_folder} "    \
						 "-m metabat > {coverage_folder}/{sample}.cov ".format(
							symlink_read_folder=symlink_read_folder,
							coverage_folder=coverage_folder,threads=self.threads,
							assembled_metagenome=assembled_metagenome,
							sample=sample,forward_read=forward_read,reverse_read=reverse_read)

				cmd_rename_bam = "cd {coverage_folder} ;" \
						 "mv {sample}.contigs.fa.{sample}_1.fastq.bam {sample}.bam ".format(
							coverage_folder=coverage_folder,
							assembled_metagenome=assembled_metagenome,
							sample=sample)

				cmd_index_bam = "cd {coverage_folder} ;" \
						"samtools index {sample}.bam ".format(coverage_folder=coverage_folder,sample=sample)

				print("****** NOW RUNNING COMMAND ******: " + rum_cmd_coverm)
				print (run_cmd(rum_cmd_coverm))

				print("****** NOW RUNNING COMMAND ******: " + cmd_rename_bam)
				print (run_cmd(cmd_rename_bam))

				print("****** NOW RUNNING COMMAND ******: " + cmd_index_bam)
				print (run_cmd(cmd_index_bam))  
		

######################################################

class metabat2(luigi.Task):
	project_name=luigi.Parameter(default="MetagenomeAnalysis")
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().read_library_type
	min_contig_length=luigi.IntParameter(default="1500")



	def requires(self):
			return [estimateCoverage(pre_process_reads=self.pre_process_reads)]
		

	def output(self):
		inDir = os.path.join(os.getcwd(), "MetagenomeAnalysis", "input_reads" + "/")
		readlist=os.listdir(inDir)

		for read in readlist:	
			if read.endswith("_1.fastq"):
				filename,file_extn=read.split(".",1)[0],read.split('.',1)[1]
				forward_read=read
				reverse_read=read.replace("_1.fastq", "_2.fastq")

				sample=read.split("_1",1)[0]

				genome_bin_folder = os.path.join(os.getcwd(), "MetagenomeAnalysis", "binning", sample + "/")
				return {'out': luigi.LocalTarget(genome_bin_folder+"/" + ".finished.txt" )}
		
		
		
	def run(self):


		inDir = os.path.join(os.getcwd(), "MetagenomeAnalysis", "input_reads" + "/")
		readlist=os.listdir(inDir)

		for read in readlist:	
			if read.endswith("_1.fastq"):
				filename,file_extn=read.split(".",1)[0],read.split('.',1)[1]
				forward_read=read
				reverse_read=read.replace("_1.fastq", "_2.fastq")

				sample=read.split("_1",1)[0]
				print("Computing Coverage for Sample:",sample)
		  
				assembled_metagenome = os.path.join(os.getcwd(), "MetagenomeAnalysis", "assembly", sample, sample+".contigs.fa" )		
				genome_bin_folder = os.path.join(os.getcwd(), "MetagenomeAnalysis", "binning", sample + "/")
				coverage_folder=os.path.join(os.getcwd(), "MetagenomeAnalysis", "binning","coverage" )
				symlink_read_folder = os.path.join(os.getcwd(), "MetagenomeAnalysis", "input_reads" + "/")
		
	
				rum_cmd_metabat2 = "[ -d  {genome_bin_folder} ] || mkdir -p {genome_bin_folder}; cd {genome_bin_folder}; " \
							"metabat2 -i {assembled_metagenome} " \
							"-o {sample}_bin " \
							"-m {min_contig_length} " \
							"-a {coverage_folder}/{sample}.cov " \
							"-t {threads} ".format(
							genome_bin_folder=genome_bin_folder,
							coverage_folder=coverage_folder,
							min_contig_length=self.min_contig_length,
							threads=self.threads,
							sample=sample,
							assembled_metagenome=assembled_metagenome)


				print("****** NOW RUNNING COMMAND ******: " + rum_cmd_metabat2)
				print (run_cmd(rum_cmd_metabat2))

				cmd_signal="cd {genome_bin_folder}; " \
						   "touch .finished.txt".format(genome_bin_folder=genome_bin_folder)

				print (run_cmd(cmd_signal))



###########################################################################################################################


###########################################################################################################################
class genomeBinning(luigi.Task):
	project_name=luigi.Parameter(default="MetagenomeAnalysis")
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().read_library_type
	min_contig_length=luigi.IntParameter(default="1500")

	def requires(self):

		return [metabat2(pre_process_reads=self.pre_process_reads,
							 min_contig_length=self.min_contig_length)]


	def output(self):
		timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
		return luigi.LocalTarget(os.path.join(os.getcwd(),"task_logs",'task.genome.binning.complete.{t}'.format(
			t=timestamp)))

	def run(self):
		timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
		with self.output().open('w') as outfile:
			outfile.write('Metagenome binning finished at {t}'.format(t=timestamp))


######################################
#Run Refine M on ech bin
##########################################
class refineM(luigi.Task):
	project_name=luigi.Parameter(default="MetagenomeAnalysis")
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().read_library_type
	min_contig_length=luigi.IntParameter(default="1500")

	def requires(self):
		 
		return [estimateCoverage(pre_process_reads=self.pre_process_reads),
				genomeBinning(min_contig_length=self.min_contig_length,
							 pre_process_reads=self.pre_process_reads)]

		
	def output(self):
		inDir = os.path.join(os.getcwd(), "MetagenomeAnalysis", "input_reads" + "/")
		readlist=os.listdir(inDir)

		for read in readlist:	
			if read.endswith("_1.fastq"):
				filename,file_extn=read.split(".",1)[0],read.split('.',1)[1]
				forward_read=read
				reverse_read=read.replace("_1.fastq", "_2.fastq")

				sample=read.split("_1",1)[0]
				refineM_stats_out_dir = os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_refinement", sample, "scaffold_stats" + "/")
				refineM_outlier_out_dir=os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_refinement", sample,  "outliers" + "/")
				refineM_filtered_out_dir = os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_refinement", sample, "filtered_bins"+ "/")
				print("aditya",refineM_stats_out_dir)
				print("aditya",refineM_outlier_out_dir)
				print("aditya",refineM_filtered_out_dir)


				return {'out1': luigi.LocalTarget(refineM_stats_out_dir + "/"  + "scaffold_stats.tsv" ),
						'out2': luigi.LocalTarget(refineM_outlier_out_dir + "/"  + "outliers.tsv" ),
						'out3': luigi.LocalTarget(refineM_filtered_out_dir + "/"  + ".refinem.log" )}



	def run(self):

		inDir = os.path.join(os.getcwd(), "MetagenomeAnalysis", "input_reads" + "/")
		readlist=os.listdir(inDir)

		for read in readlist:	
			if read.endswith("_1.fastq"):
				filename,file_extn=read.split(".",1)[0],read.split('.',1)[1]
				forward_read=read
				reverse_read=read.replace("_1.fastq", "_2.fastq")

				sample=read.split("_1",1)[0]
				print("Computing Coverage for Sample:",sample)

				refineM_bin_folder = os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_refinement", sample + "/")
				refineM_stats_out_dir = os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_refinement", sample, "scaffold_stats" + "/")
				refineM_outlier_out_dir=os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_refinement", sample, "outliers" + "/")
				refineM_filtered_out_dir = os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_refinement", sample,"filtered_bins"+ "/")
				bam_file = os.path.join(os.getcwd(), "MetagenomeAnalysis", "binning","coverage", sample + ".bam")


				print("run",refineM_bin_folder)
				print("run",refineM_outlier_out_dir)
				print("run",refineM_stats_out_dir)
				print("run",bam_file)


				scaffold_file = os.path.join(os.getcwd(), "MetagenomeAnalysis", "assembly", sample, sample+".contigs.fa" )
		
		
				input_bin_folder = os.path.join(os.getcwd(), "MetagenomeAnalysis", "binning", sample + "/")
		
		
				run_cmd_scaffold_stats = "[ -d  {refineM_stats_out_dir} ] || mkdir -p {refineM_stats_out_dir}; " \
								 "refinem scaffold_stats -x fa " \
								 " -c {threads} {scaffold_file} {input_bin_folder} {refineM_stats_out_dir} {bam_file} ".format(
									threads=self.threads,
									scaffold_file=scaffold_file,
									input_bin_folder=input_bin_folder,
									refineM_stats_out_dir=refineM_stats_out_dir,
									bam_file=bam_file)

				run_cmd_refinem_outliers = "refinem outliers {refineM_stats_out_dir}scaffold_stats.tsv {refineM_outlier_out_dir} ".format(
									refineM_stats_out_dir=refineM_stats_out_dir,
									refineM_outlier_out_dir=refineM_outlier_out_dir)

				run_cmd_refinem_filtered = "refinem filter_bins -x fa {input_bin_folder} {refineM_outlier_out_dir}outliers.tsv {refineM_filtered_out_dir} ".format(
									input_bin_folder=input_bin_folder,
									refineM_filtered_out_dir=refineM_filtered_out_dir,
									refineM_outlier_out_dir=refineM_outlier_out_dir)

				run_cmd_mv_log = "cd {refineM_filtered_out_dir}; " \
						 "mv refinem.log .refinem.log".format(refineM_filtered_out_dir=refineM_filtered_out_dir)

		
		#run_cmd_mv_seed = "cd {refineM_filtered_out_dir}; " \
						 #"rm *.seed ".format(refineM_filtered_out_dir=refineM_filtered_out_dir)
		
		

				print("****** NOW RUNNING COMMAND ******: " + run_cmd_scaffold_stats)
				print (run_cmd(run_cmd_scaffold_stats))

				print("****** NOW RUNNING COMMAND ******: " + run_cmd_refinem_outliers)
				print (run_cmd(run_cmd_refinem_outliers))

				print("****** NOW RUNNING COMMAND ******: " + run_cmd_refinem_filtered)
				print (run_cmd(run_cmd_refinem_filtered))

				print (run_cmd(run_cmd_mv_log))
				#print (run_cmd(run_cmd_mv_seed))
######################################################



####################################################
class binRefinement(luigi.Task):
	project_name=luigi.Parameter(default="MetagenomeAnalysis")
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().read_library_type
	min_contig_length=luigi.IntParameter(default="1500")

	def requires(self):
		return [refineM(pre_process_reads=self.pre_process_reads,
				min_contig_length=self.min_contig_length)]

		
		
	def output(self):
		timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
		return luigi.LocalTarget(os.path.join(os.getcwd(),"task_logs",'task.bin.refinement.complete.{t}'.format(
			t=timestamp)))

	def run(self):
		timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
		with self.output().open('w') as outfile:
			outfile.write('Bin Refinement finished at {t}'.format(t=timestamp))


######################################################
#######################################################################################################
class conditionBasedDeRep(luigi.Task):
	project_name=luigi.Parameter(default="MetagenomeAnalysis")
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().read_library_type
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


		reference_condition_out=os.path.join(os.getcwd() +"/"+ "MetagenomeAnalysis" +"/"+ "dRep_bins"+ "/"+ self.reference_condition +"_out"+"/")
		contrast_condition_out=os.path.join(os.getcwd() +"/"+ "MetagenomeAnalysis" +"/"+ "dRep_bins"+ "/"+ self.contrast_condition +"_out"+"/")


		return {'out1': luigi.LocalTarget(reference_condition_out + "data_tables/" + "genomeInfo.csv" ),
				'out2': luigi.LocalTarget(contrast_condition_out + "data_tables/" + "genomeInfo.csv" )}


	def run(self):

		mapping_file=os.path.join(os.getcwd(), "sample_list","metgen_sample_group.tsv")
		condition_based_derep(mapping_file,self.reference_condition,self.contrast_condition)


		
		reference_dRep_path=os.path.join(os.getcwd(),"MetagenomeAnalysis","dRep_bins",self.reference_condition)
		contrast_dRep_path=os.path.join(os.getcwd(),"MetagenomeAnalysis","dRep_bins",self.contrast_condition)


		dRep_ref_cond_out_folder=os.path.join(os.getcwd(),"MetagenomeAnalysis","dRep_bins", self.reference_condition +"_out"+"/")
		dRep_con_cond_out_folder=os.path.join(os.getcwd(),"MetagenomeAnalysis","dRep_bins", self.contrast_condition +"_out"+"/")



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


		dRep_ref_cond_out_folder=os.path.join(os.getcwd(),"MetagenomeAnalysis","dRep_bins", self.reference_condition +"_out"+"/")
		dRep_con_cond_out_folder=os.path.join(os.getcwd(),"MetagenomeAnalysis","dRep_bins", self.contrast_condition +"_out"+"/")
		merged_bin_folder=os.path.join(os.getcwd(), "MetagenomeAnalysis" ,"dRep_bins","dReplicated_bins_condition_based"+"/")
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

		merged_bin_folder=os.path.join(os.getcwd(), "MetagenomeAnalysis" ,"dRep_bins","dReplicated_bins_condition_based"+"/")

		genomes_dRep_by_condition_list=os.listdir(merged_bin_folder)
		#print(genomes_dRep_by_condition_list)
		genome_list_path=os.path.join(os.getcwd(), "MetagenomeAnalysis","dRep_bins","genomes_dRep_by_condition.lst")
		with open(genome_list_path, 'w') as output:
			for genome in genomes_dRep_by_condition_list:
				output.write('%s\n' % genome)

class conditionFreeDeRep(luigi.Task):
	project_name=luigi.Parameter(default="MetagenomeAnalysis")
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().read_library_type
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


		dReplicated_bins_condition_free=os.path.join(os.getcwd() +"/"+ "MetagenomeAnalysis" +"/"+"dRep_bins"+ "/" + "dReplicated_bins_condition_free"+"/")
		

		return {'out1': luigi.LocalTarget(dReplicated_bins_condition_free + "data_tables/" + "genomeInfo.csv" )}


	def run(self):

		sample_file=os.path.join(os.getcwd(), "sample_list","pe_samples.lst")
		condition_free_derep(sample_file,self.reference_condition,self.contrast_condition)


		dReplicated_bins_condition_free=os.path.join(os.getcwd() +"/"+ "MetagenomeAnalysis" +"/"+"dRep_bins"+ "/" + "dReplicated_bins_condition_free"+"/")
		dReplicated_bins_input=os.path.join(os.getcwd() +"/"+ "MetagenomeAnalysis" +"/"+"dRep_bins"+ "/" + "dReplicated_bins"+"/")



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


		merged_bin_folder=os.path.join(os.getcwd(), "MetagenomeAnalysis","dRep_bins", "dReplicated_bins_condition_free","dereplicated_genomes")
		genomes_dRep_regardless_condition_list=os.listdir(merged_bin_folder)
		#print(genomes_dRep_by_condition_list)
		genome_list_path=os.path.join(os.getcwd(), "MetagenomeAnalysis","dRep_bins", "genomes_dRep_regardless_condition.lst")
		with open(genome_list_path, 'w') as output:
			for genome in genomes_dRep_regardless_condition_list:
				output.write('%s\n' % genome)
#######################################################################################################
class dRepBins(luigi.Task):
	project_name=luigi.Parameter(default="MetagenomeAnalysis")
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().read_library_type
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
									 	completeness=self.completeness,contrast_condition=self.contrast_condition,
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



			
#######################################################################################################
class prokka(luigi.Task):
	project_name=luigi.Parameter(default="MetagenomeAnalysis")
	#assembler = luigi.ChoiceParameter(default="megahit",choices=["megahit", "metaspades"], var_type=str)
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().read_library_type
	min_contig_length=luigi.IntParameter(default="1500")
	min_genome_length=luigi.IntParameter(default="50000")
	completeness=luigi.IntParameter(default="75")
	contamination=luigi.IntParameter(default="25")
	checkM_method=luigi.ChoiceParameter(default="taxonomy_wf",choices=["taxonomy_wf", "lineage_wf"], var_type=str)
	dRep_method=luigi.ChoiceParameter(choices=["condition_based", "condition_free"], var_type=str)
	genomeName=luigi.Parameter()
	reference_condition=luigi.Parameter()
	contrast_condition=luigi.Parameter()
	
	def requires(self):
		return [dRepBins(pre_process_reads=self.pre_process_reads,
						checkM_method=self.checkM_method,
						dRep_method=self.dRep_method,
					   	min_contig_length=self.min_contig_length,
						reference_condition=self.reference_condition,
						completeness=self.completeness,
						contrast_condition=self.contrast_condition,
						contamination=self.contamination)]
		

								
	def output(self):
		bin_name=self.genomeName.split(".filtered.fa")[0]
		outDir = os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_annotation_prokka","annotated_bins",bin_name + "/")
		return {'out': luigi.LocalTarget(outDir , bin_name+".log" )}
		
						
	def run(self):
		if self.dRep_method=="condition_free":
			binDir=os.path.join(os.getcwd(),"MetagenomeAnalysis","dRep_bins","dReplicated_bins_condition_free","dereplicated_genomes")

		
		if self.dRep_method=="condition_based":
			binDir=os.path.join(os.getcwd(), "MetagenomeAnalysis" ,"dRep_bins","dReplicated_bins_condition_based"+"/")

		
		outDir = os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_annotation_prokka","annotated_bins"+"/")


		bin_name=self.genomeName.split(".filtered.fa")[0]
		outDir = os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_annotation_prokka","annotated_bins",bin_name)

		cmd_run_prokka="[ -d  {outDir} ] || mkdir -p {outDir}; " \
				   "prokka {binDir}/{genomeName} " \
				   "--quiet " \
				   "--outdir {outDir} "  \
				   "--prefix {bin_name} " \
				   "--metagenome " \
				   "--kingdom Bacteria " \
				   "--locustag PROKKA " \
				   "--cpus {threads}  --force &>/dev/null".format(binDir=binDir,genomeName=self.genomeName, outDir=outDir, bin_name=bin_name,threads=self.threads)
		print("****** NOW RUNNING COMMAND ******: " + cmd_run_prokka)
		print (run_cmd(cmd_run_prokka))

#####################################################################################################################	
class genomeResolvedMetagenomics(luigi.Task):
	project_name=luigi.Parameter(default="MetagenomeAnalysis")
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().read_library_type
	min_contig_length=luigi.IntParameter(default="1500")
	min_genome_length=luigi.IntParameter(default="50000")
	completeness=luigi.IntParameter(default="75")
	contamination=luigi.IntParameter(default="25")
	checkM_method=luigi.ChoiceParameter(default="taxonomy_wf",choices=["taxonomy_wf", "lineage_wf"], var_type=str)
	dRep_method=luigi.ChoiceParameter(choices=["condition_based", "condition_free"], var_type=str)
	reference_condition=luigi.Parameter()
	contrast_condition=luigi.Parameter()

	def requires(self):
		if self.dRep_method=="condition_based":
			return [prokka(checkM_method=self.checkM_method,dRep_method=self.dRep_method,
					   pre_process_reads=self.pre_process_reads,reference_condition=self.reference_condition,
					   completeness=self.completeness,contrast_condition=self.contrast_condition,
					   contamination=self.contamination,
					   min_contig_length=self.min_contig_length,
						genomeName=i)
					for i in [line.strip()
							  for line in
							  open((os.path.join(os.getcwd(),"MetagenomeAnalysis" ,"dRep_bins", "genomes_dRep_by_condition.lst")))]]

		if self.dRep_method=="condition_free":
			return [prokka(checkM_method=self.checkM_method,dRep_method=self.dRep_method,
					   pre_process_reads=self.pre_process_reads,contrast_condition=self.contrast_condition,
					   completeness=self.completeness,reference_condition=self.reference_condition,
					   contamination=self.contamination,
					   min_contig_length=self.min_contig_length,
						genomeName=i)
					for i in [line.strip()
							  for line in
							  open((os.path.join(os.getcwd(),"MetagenomeAnalysis","dRep_bins", "genomes_dRep_regardless_condition.lst")))]]
		
	def output(self):
		timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
		return luigi.LocalTarget(os.path.join(os.getcwd(),"task_logs",'task.genome.dereplication.complete.{t}'.format(
			t=timestamp)))

	def run(self):
		timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
		with self.output().open('w') as outfile:
			outfile.write('Genome Dereplication finished at {t}'.format(t=timestamp))

######################################################################################################################
######################################################################################################################
#enrich bins using enrichM
#Prepare Input for check
class enrichBins(luigi.Task):
	project_name=luigi.Parameter(default="MetagenomeAnalysis")
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	checkM_method=luigi.ChoiceParameter(choices=["taxonomy_wf", "lineage_wf"], var_type=str)
	read_library_type = GlobalParameter().read_library_type
	min_contig_length=luigi.IntParameter(default="1500")
	completeness=luigi.IntParameter(default="50")
	contamination=luigi.IntParameter(default="25")
	reference_condition=luigi.Parameter()
	contrast_condition=luigi.Parameter()
	
	
	def requires(self):

		return [conditionBasedDeRep(checkM_method=self.checkM_method,
					   pre_process_reads=self.pre_process_reads,
					   completeness=self.completeness,
					   contamination=self.contamination,
					   reference_condition=self.reference_condition,
					   contrast_condition=self.contrast_condition,
					   min_contig_length=self.min_contig_length)]

	def output(self):

		enrichm_annotate_output=os.path.join(os.getcwd() +"/"+ "MetagenomeAnalysis" +"/"+"bin_enrichment_analysis"+ "/" + "enrichm_annotate_out"+"/")			
		return {'out1': luigi.LocalTarget(enrichm_annotate_output + "cazy_frequency_table.tsv"),
					'out2': luigi.LocalTarget(enrichm_annotate_output + "ec_frequency_table.tsv"),
					'out3': luigi.LocalTarget(enrichm_annotate_output + "pfam_frequency_table.tsv"),
					'out4': luigi.LocalTarget(enrichm_annotate_output + "ko_hmm_frequency_table.tsv"),
					'out5': luigi.LocalTarget(enrichm_annotate_output + "tigrfam_frequency_table.tsv")}

	def run(self):

		dRep_ref_cond_out_folder=os.path.join(os.getcwd(),"MetagenomeAnalysis","dRep_bins", self.reference_condition +"_out"+"/")
		dRep_con_cond_out_folder=os.path.join(os.getcwd(),"MetagenomeAnalysis","dRep_bins", self.contrast_condition +"_out"+"/")
		enrichm_annotate_input_merge_by_condition=os.path.join(os.getcwd(), "MetagenomeAnalysis" ,"dRep_bins","dReplicated_bins_condition_based"+"/")
		dRep_ref_genomes=os.path.join(dRep_ref_cond_out_folder,"dereplicated_genomes")
		dRep_con_genomes=os.path.join(dRep_con_cond_out_folder,"dereplicated_genomes")

		cmd_prepare_enrichm_annotate_input="[ -d  {enrichm_annotate_input_merge_by_condition} ] || mkdir -p {enrichm_annotate_input_merge_by_condition} ; cd {enrichm_annotate_input_merge_by_condition} ; " \
									   "cp {dRep_ref_genomes}/*.fa {enrichm_annotate_input_merge_by_condition} ; " \
									   "cp {dRep_con_genomes}/*.fa {enrichm_annotate_input_merge_by_condition} ;".format(enrichm_annotate_input_merge_by_condition=enrichm_annotate_input_merge_by_condition,
										dRep_ref_genomes=dRep_ref_genomes,dRep_con_genomes=dRep_con_genomes)
			#print("****** NOW RUNNING COMMAND ******: " + cmd_prepare_enrichm_annotate_input)
		print(run_cmd(cmd_prepare_enrichm_annotate_input))



		bin_enrichment_analysis=os.path.join(os.getcwd(), "MetagenomeAnalysis","bin_enrichment_analysis")
		createFolder(bin_enrichment_analysis)
		enrichm_annotate_out=os.path.join(os.getcwd() +"/"+ "MetagenomeAnalysis" +"/"+"bin_enrichment_analysis"+ "/" + "enrichm_annotate_out"+"/")


		sample_group_file=os.path.join(os.getcwd(), "sample_list","metgen_sample_group.tsv")

		dRep_bins_path=os.path.join(os.getcwd(), "MetagenomeAnalysis" ,"dRep_bins","dReplicated_bins_condition_based"+"/")


		file_names=os.listdir(dRep_bins_path)
		bin_names = []
		for string in file_names:
			new_string = string.replace(".fa", ".")
			bin_names.append(new_string)
		df1 = pd.DataFrame(bin_names) 
		df1.columns = ['bin_names']
		df1['samples'] = df1['bin_names'].str.split('_').str[0]
	
		df2=pd.read_csv(sample_group_file, sep='\t+', engine='python')
		df2.columns=['samples', 'conditions']
	
		outer_join = pd.merge(df1,  df2,  on='samples', how ='outer')
		metadata=outer_join[['bin_names','conditions']]

		nan_value = float("NaN")
		metadata.replace("", nan_value, inplace=True)
		metadata.dropna(subset = ["bin_names"], inplace=True)

		
		metadata.to_csv(bin_enrichment_analysis+"/"+"metadata.tsv",sep='\t',header=False, index=False)	

	

		cmd_run_annotate="enrichm annotate --genome_directory {dRep_bins_path} --suffix fa " \
				   "--output {enrichm_annotate_out} "  \
				   "--ko --ko_hmm --pfam --tigrfam --cazy  --ec " \
				   "--threads {threads} " \
				   "--parallel {threads} ".format(dRep_bins_path=dRep_bins_path,
												  enrichm_annotate_out=enrichm_annotate_out,
												  threads=self.threads)
		print("****** NOW RUNNING COMMAND ******: " + cmd_run_annotate)
		print (run_cmd(cmd_run_annotate))
		

#############################################################################################################################
class differentialEnrichmentAnalysis(luigi.Task):
	project_name=luigi.Parameter(default="MetagenomeAnalysis")
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	checkM_method=luigi.ChoiceParameter(choices=["taxonomy_wf", "lineage_wf"], var_type=str)
	read_library_type = GlobalParameter().read_library_type
	min_contig_length=luigi.IntParameter(default="1500")
	completeness=luigi.IntParameter(default="50")
	contamination=luigi.IntParameter(default="25")
	reference_condition=luigi.Parameter()
	contrast_condition=luigi.Parameter()
	pvalcutoff=luigi.FloatParameter(default="0.05")
	correction = luigi.ChoiceParameter(default="fdr_bh",choices=["fdr_bh", "fdr_by","fdr_tsbh","fdr_tsbky","fdr_gbs","b","s","h","hs","sh","ho"], var_type=str)
	dRep_method=luigi.ChoiceParameter(choices=["condition_based", "condition_free"], var_type=str)
	
	def requires(self):
			return [enrichBins(dRep_method=self.dRep_method,
					   checkM_method=self.checkM_method,
					   pre_process_reads=self.pre_process_reads,
					   completeness=self.completeness,
					   contamination=self.contamination,
					   min_contig_length=self.min_contig_length,
					   reference_condition=self.reference_condition,
					   contrast_condition=self.contrast_condition)]


	def output(self):

		enrichm_ec_output = os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_enrichment_analysis", "enrichm_enrichment_ec_out" + "/")
		enrichm_ko_output = os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_enrichment_analysis", "enrichm_enrichment_ko_out" + "/")
		enrichm_pfam_output = os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_enrichment_analysis", "enrichm_enrichment_pfam_out" + "/")
		enrichm_cazy_output = os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_enrichment_analysis", "enrichm_enrichment_cazy_out" + "/")
		enrichm_kohmm_output = os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_enrichment_analysis", "enrichm_enrichment_kohmm_out" + "/")
		enrichm_tigrfam_output = os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_enrichment_analysis", "enrichm_enrichment_tigrfam_out" + "/")


		return {'out1': luigi.LocalTarget(enrichm_ko_output + "proportions.tsv"),
				'out2': luigi.LocalTarget(enrichm_pfam_output + "proportions.tsv"),
				'out3': luigi.LocalTarget(enrichm_cazy_output + "proportions.tsv"),
				'out4': luigi.LocalTarget(enrichm_kohmm_output + "proportions.tsv"),
				'out5': luigi.LocalTarget(enrichm_tigrfam_output + "proportions.tsv"),
				'out6': luigi.LocalTarget(enrichm_ec_output + "proportions.tsv")}
			   

	def run(self):

		
		metadata_file=os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_enrichment_analysis","metadata.tsv")
		enrichm_annotate_out = os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_enrichment_analysis", "enrichm_annotate_out" + "/")

		enrichm_ec_output = os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_enrichment_analysis", "enrichm_enrichment_ec_out" + "/")
		enrichm_ko_output = os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_enrichment_analysis", "enrichm_enrichment_ko_out" + "/")
		enrichm_pfam_output = os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_enrichment_analysis", "enrichm_enrichment_pfam_out" + "/")
		enrichm_cazy_output = os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_enrichment_analysis", "enrichm_enrichment_cazy_out" + "/")
		enrichm_kohmm_output = os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_enrichment_analysis", "enrichm_enrichment_kohmm_out" + "/")
		enrichm_tigrfam_output = os.path.join(os.getcwd(), "MetagenomeAnalysis", "bin_enrichment_analysis", "enrichm_enrichment_tigrfam_out" + "/")

		
		# enrichm enrichment --annotate_output enrichm_annotate_out/ --metadata metadata.txt --processes 8 --pfam --output enrichm_enrichment_pfam_out


		cmd_run_ko_enrichment="enrichm enrichment --annotate_output {enrichm_annotate_out} " \
				   "--output {enrichm_ko_output} --log {enrichm_ko_output}/ko_enrichment.log --verbosity 5 "  \
				   "--ko --pval_cutoff {pvalcutoff} --multi_test_correction {correction} " \
				   "--processes {threads} " \
				   "--metadata {metadata_file} ".format(enrichm_annotate_out=enrichm_annotate_out,
														enrichm_ko_output=enrichm_ko_output,
														metadata_file=metadata_file,
														pvalcutoff=self.pvalcutoff,
														correction=self.correction,
														threads=self.threads)
		print("****** NOW RUNNING COMMAND ******: " + cmd_run_ko_enrichment)
		print (run_cmd(cmd_run_ko_enrichment))

		cmd_run_pfam_enrichment="enrichm enrichment --annotate_output {enrichm_annotate_out} " \
				   "--output {enrichm_pfam_output} --log {enrichm_pfam_output}/pfam_enrichment.log --verbosity 5 "  \
				   "--pfam  --pval_cutoff {pvalcutoff} --multi_test_correction {correction} " \
				   "--processes {threads} " \
				   "--metadata {metadata_file} ".format(enrichm_annotate_out=enrichm_annotate_out,
														enrichm_pfam_output=enrichm_pfam_output,
														pvalcutoff=self.pvalcutoff,
														correction=self.correction,
														metadata_file=metadata_file,
														threads=self.threads)
		print("****** NOW RUNNING COMMAND ******: " + cmd_run_pfam_enrichment)
		print (run_cmd(cmd_run_pfam_enrichment))

		cmd_run_cazy_enrichment="enrichm enrichment --annotate_output {enrichm_annotate_out} " \
				   "--output {enrichm_cazy_output} --log {enrichm_cazy_output}/cazy_enrichment.log --verbosity 5 "  \
				   "--cazy --pval_cutoff {pvalcutoff} --multi_test_correction {correction} " \
				   "--processes {threads} " \
				   "--metadata {metadata_file} ".format(enrichm_annotate_out=enrichm_annotate_out,
														enrichm_cazy_output=enrichm_cazy_output,
														pvalcutoff=self.pvalcutoff,
														correction=self.correction,
														metadata_file=metadata_file,
														threads=self.threads)
		print("****** NOW RUNNING COMMAND ******: " + cmd_run_cazy_enrichment)
		print (run_cmd(cmd_run_cazy_enrichment))


		cmd_run_kohmm_enrichment="enrichm enrichment --annotate_output {enrichm_annotate_out} " \
				   "--output {enrichm_kohmm_output} --log {enrichm_kohmm_output}/kohmm_enrichment.log --verbosity 5 "  \
				   "--ko_hmm --pval_cutoff {pvalcutoff} --multi_test_correction {correction} " \
				   "--processes {threads} " \
				   "--metadata {metadata_file} ".format(enrichm_annotate_out=enrichm_annotate_out,
														enrichm_kohmm_output=enrichm_kohmm_output,
														pvalcutoff=self.pvalcutoff,
														correction=self.correction,
														metadata_file=metadata_file,
														threads=self.threads)
		print("****** NOW RUNNING COMMAND ******: " + cmd_run_kohmm_enrichment)
		print (run_cmd(cmd_run_kohmm_enrichment))


		cmd_run_tigrfam_enrichment="enrichm enrichment --annotate_output {enrichm_annotate_out} " \
				   "--output {enrichm_tigrfam_output} --log {enrichm_tigrfam_output}/tigrfam_enrichment.log --verbosity 5 "  \
				   "--tigrfam --pval_cutoff {pvalcutoff} --multi_test_correction {correction} " \
				   "--processes {threads} " \
				   "--metadata {metadata_file} ".format(enrichm_annotate_out=enrichm_annotate_out,
														enrichm_tigrfam_output=enrichm_tigrfam_output,
														pvalcutoff=self.pvalcutoff,
														correction=self.correction,
														metadata_file=metadata_file,
														threads=self.threads)
		print("****** NOW RUNNING COMMAND ******: " + cmd_run_tigrfam_enrichment)
		print (run_cmd(cmd_run_tigrfam_enrichment))



		cmd_run_ec_enrichment="enrichm enrichment --annotate_output {enrichm_annotate_out} " \
				   "--output {enrichm_ec_output} --log {enrichm_ec_output}/ec_enrichment.log --verbosity 5 "  \
				   "--ec --pval_cutoff {pvalcutoff} --multi_test_correction {correction} " \
				   "--processes {threads} " \
				   "--metadata {metadata_file} ".format(enrichm_annotate_out=enrichm_annotate_out,
														enrichm_ec_output=enrichm_ec_output,
														pvalcutoff=self.pvalcutoff,
														correction=self.correction,
														metadata_file=metadata_file,
														threads=self.threads)
		print("****** NOW RUNNING COMMAND ******: " + cmd_run_ec_enrichment)
		print (run_cmd(cmd_run_ec_enrichment))

######################################################################################################################
class humann(luigi.Task):
	project_name=luigi.Parameter(default="MetagenomeAnalysis")
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().read_library_type
	sampleName = luigi.Parameter(description="name of the sample to be analyzed. (string)")


	def requires(self):
		if self.pre_process_reads=="yes":
			return [bbduk(read_library_type="pe",
					  sampleName=i)
				for i in [line.strip()
						  for line in
						  open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]]
		
		if self.pre_process_reads=="no":
			return [reformat(read_library_type="pe",
					  sampleName=i)
				for i in [line.strip()
						  for line in
						  open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]]
		

	def output(self):

		humann_folder = os.path.join(os.getcwd(), "MetagenomeAnalysis", "humann_pathway_analysis" )
		return {'out': luigi.LocalTarget(humann_folder,self.sampleName+"_1_pathabundance.tsv")}
		
	def run(self):

		humann_folder = os.path.join(os.getcwd(), "MetagenomeAnalysis", "humann_pathway_analysis" )		

		input_sample_list = os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")
		symlink_read_folder = os.path.join(os.getcwd(), "MetagenomeAnalysis", "input_reads" + "/")

		if self.pre_process_reads == "yes":
			clean_read_symlink(input_sample_list)

		if self.pre_process_reads == "no":
			verified_read_symlink(input_sample_list)

		cmd_run_humann = "[ -d  {humann_folder} ] || mkdir -p {humann_folder};  " \
							 "humann " \
							 "-i {symlink_read_folder}{sample}_1.fastq " \
							 "--protein-database /home/sutripa/scriptome/database/humann/gtdb " \
							 "--nucleotide-database /home/sutripa/scriptome/database/humann/chocophlan " \
							 "--memory-use minimum --remove-temp-output " \
							 "--threads {threads} " \
							 "-o {humann_folder} 2>&1 | tee  {humann_folder}/{sample}.log" \
			.format(humann_folder=humann_folder,
					sample=self.sampleName,
					symlink_read_folder=symlink_read_folder,
					threads=self.threads)

		print("****** NOW RUNNING COMMAND ******: " + cmd_run_humann)
		print (run_cmd(cmd_run_humann))



class profilePathway(luigi.Task):
	project_name=luigi.Parameter(default="MetagenomeAnalysis")
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().read_library_type

	def requires(self):

		return [humann(pre_process_reads=self.pre_process_reads,
						sampleName=i)
					for i in [line.strip()
							  for line in
							  open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]]

	def output(self):
		timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
		return luigi.LocalTarget(os.path.join(os.getcwd(),"MetagenomeAnalysis","task_logs",'task.pathway.profile.complete.{t}'.format(
			t=timestamp)))

	def run(self):
		timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
		with self.output().open('w') as outfile:
			outfile.write('Metagenome pathway profile finished at {t}'.format(t=timestamp))

