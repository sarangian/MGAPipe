import os
import luigi
import os
import time
import subprocess
import pandas as pd
from luigi import Parameter

from tasks.metagenome.metaAssembly import metagenomeAssembly

class GlobalParameter(luigi.Config):
	threads = luigi.Parameter()
	maxMemory = luigi.Parameter()
	projectName = luigi.Parameter()
	domain=luigi.Parameter()
	pe_read_dir=luigi.Parameter()
	adapter=luigi.Parameter()
	seq_platforms=luigi.Parameter()

def run_cmd(cmd):
	p = subprocess.Popen(cmd, bufsize=-1,
						 shell=True,
						 universal_newlines=True,
						 stdout=subprocess.PIPE,
						 executable='/bin/bash')
	output = p.communicate()[0]
	return output


class estimateCoverage(luigi.Task):
	priority =100
	project_name=GlobalParameter().projectName
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().seq_platforms
	min_contig_length=luigi.IntParameter(default="1500")

	def requires(self):
		return [metagenomeAssembly(pre_process_reads=self.pre_process_reads,
								  min_contig_length=self.min_contig_length)]

	def output(self):

		if self.pre_process_reads=="no":
			pe_read_folder = os.path.join(os.getcwd(), self.project_name, "ReadQC", "VerifiedReads", "PE-Reads" + "/")
			
		if self.pre_process_reads=="yes":
			pe_read_folder = os.path.join(os.getcwd(), self.project_name, "ReadQC", "CleanedReads", "PE-Reads" + "/")
		   

		inDir = os.path.join(os.getcwd(), pe_read_folder + "/")
		readlist=os.listdir(inDir)

		for read in readlist:	
			if read.endswith("_1.fastq"):
				filename,file_extn=read.split(".",1)[0],read.split('.',1)[1]
				forward_read=read
				reverse_read=read.replace("_1.fastq", "_2.fastq")
				sample=read.split("_1",1)[0]


				coverage_folder=os.path.join(os.getcwd(), self.project_name, "binning","coverage" )
				return {'out1': luigi.LocalTarget(coverage_folder +"/"+ sample +".bam"),
						'out2': luigi.LocalTarget(coverage_folder +"/"+ sample +".bam.bai"),
						'out3': luigi.LocalTarget(coverage_folder +"/"+ sample +".cov")}


	def run(self):
		
		coverage_folder=os.path.join(os.getcwd(), self.project_name, "binning","coverage" )
		if self.pre_process_reads=="no":
			pe_read_folder = os.path.join(os.getcwd(), self.project_name, "ReadQC", "VerifiedReads", "PE-Reads" + "/")
			
		if self.pre_process_reads=="yes":
			pe_read_folder = os.path.join(os.getcwd(), self.project_name, "ReadQC", "CleanedReads", "PE-Reads" + "/")


		inDir = os.path.join(os.getcwd(), pe_read_folder + "/")

		readlist=os.listdir(inDir)

		for read in readlist:	
			if read.endswith("_1.fastq"):
				filename,file_extn=read.split(".",1)[0],read.split('.',1)[1]
				forward_read=read
				reverse_read=read.replace("_1.fastq", "_2.fastq")

				sample=read.split("_1",1)[0]
				print("Computing Coverage for Sample:",sample)
		  
				assembled_metagenome = os.path.join(os.getcwd(), self.project_name, "MGAssembly", sample, sample+".contigs.fa" )
				print (assembled_metagenome)


				rum_cmd_coverm = "[ -d  {coverage_folder} ] || mkdir -p {coverage_folder}; " \
						 "coverm contig " \
						 "-c {pe_read_folder}/{forward_read} {pe_read_folder}/{reverse_read} "    \
						 "-r {assembled_metagenome} " \
						 "-p minimap2-sr -t {threads} " \
						 "--output-format sparse " \
						 "--bam-file-cache-directory {coverage_folder} "    \
						 "-m metabat > {coverage_folder}/{sample}.cov ".format(
							pe_read_folder=pe_read_folder,
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

class genomeBinning(luigi.Task):
	projectName=GlobalParameter().projectName
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().seq_platforms
	min_contig_length=luigi.IntParameter(default="1500")



	def requires(self):
			return [estimateCoverage(pre_process_reads=self.pre_process_reads,
									 min_contig_length=self.min_contig_length)]
		

	def output(self):
		if self.pre_process_reads=="no":
			pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "PE-Reads" + "/")
			
		if self.pre_process_reads=="yes":
			pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "PE-Reads" + "/")


		inDir = os.path.join(os.getcwd(), pe_read_folder + "/")
		readlist=os.listdir(inDir)

		for read in readlist:	
			if read.endswith("_1.fastq"):
				filename,file_extn=read.split(".",1)[0],read.split('.',1)[1]
				forward_read=read
				reverse_read=read.replace("_1.fastq", "_2.fastq")

				sample=read.split("_1",1)[0]

				genome_bin_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "binning", sample + "/")
				return {'out': luigi.LocalTarget(genome_bin_folder+"/" + ".finished.txt" )}
		
		
		
	def run(self):

		if self.pre_process_reads=="no":
			pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "PE-Reads" + "/")
			
		if self.pre_process_reads=="yes":
			pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "PE-Reads" + "/")


		inDir = os.path.join(os.getcwd(), pe_read_folder + "/")
		readlist=os.listdir(inDir)

		for read in readlist:	
			if read.endswith("_1.fastq"):
				filename,file_extn=read.split(".",1)[0],read.split('.',1)[1]
				forward_read=read
				reverse_read=read.replace("_1.fastq", "_2.fastq")

				sample=read.split("_1",1)[0]
				print("Computing Coverage for Sample:",sample)
		  
				assembled_metagenome = os.path.join(os.getcwd(), GlobalParameter().projectName, "MGAssembly", sample, sample+".contigs.fa")		
				genome_bin_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "binning", sample + "/")
				coverage_folder=os.path.join(os.getcwd(), GlobalParameter().projectName, "binning","coverage" + "/" )
				#symlink_read_folder = os.path.join(os.getcwd(), self.project_name, "input_reads" + "/")
		
	
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
				print("****** NOW RUNNING COMMAND ******: " + cmd_signal)		   	
				print (run_cmd(cmd_signal))



###########################################################################################################################


###########################################################################################################################
'''
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
'''
