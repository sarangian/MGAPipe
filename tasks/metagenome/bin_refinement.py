import os
import luigi
import os
import time
import subprocess
import pandas as pd
from luigi import Parameter

from tasks.metagenome.metaAssembly import metagenomeAssembly
from tasks.metagenome.genome_binning import genomeBinning

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


class refineM(luigi.Task):
	project_name=GlobalParameter().projectName
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().seq_platforms
	min_contig_length=luigi.IntParameter(default="1500")

	def requires(self):
		return [genomeBinning(pre_process_reads=self.pre_process_reads,
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

				refineM_stats_out_dir = os.path.join(os.getcwd(), self.project_name, "bin_refinement", sample, "scaffold_stats" + "/")
				refineM_outlier_out_dir=os.path.join(os.getcwd(), self.project_name, "bin_refinement", sample,  "outliers" + "/")
				refineM_filtered_out_dir = os.path.join(os.getcwd(), self.project_name, "bin_refinement", sample, "filtered_bins"+ "/")
				
				print("STAT DIR",refineM_stats_out_dir)
				print("OUTLIER DIR",refineM_outlier_out_dir)
				print("FILTERED DIR",refineM_filtered_out_dir)


				return {'out1': luigi.LocalTarget(refineM_stats_out_dir + "/"  + "scaffold_stats.tsv" ),
						'out2': luigi.LocalTarget(refineM_outlier_out_dir + "/"  + "outliers.tsv" ),
						'out3': luigi.LocalTarget(refineM_filtered_out_dir + "/"  + ".refinem.log" )}


	def run(self):
		
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

				refineM_bin_folder = os.path.join(os.getcwd(), self.project_name, "bin_refinement", sample + "/")
				refineM_stats_out_dir = os.path.join(os.getcwd(), self.project_name, "bin_refinement", sample, "scaffold_stats" + "/")
				refineM_outlier_out_dir=os.path.join(os.getcwd(), self.project_name, "bin_refinement", sample, "outliers" + "/")
				refineM_filtered_out_dir = os.path.join(os.getcwd(), self.project_name, "bin_refinement", sample,"filtered_bins"+ "/")
				bam_file = os.path.join(os.getcwd(), self.project_name,"binning","coverage", sample + ".bam")


				print("refineM BIN",refineM_bin_folder)
				print("refineM OUTLIER",refineM_outlier_out_dir)
				print("refineM STAT",refineM_stats_out_dir)
				print("refineM BAM",bam_file)


				scaffold_file = os.path.join(os.getcwd(), self.project_name, "MGAssembly", sample, sample+".contigs.fa")	
		
		
				input_bin_folder = os.path.join(os.getcwd(), self.project_name, "binning", sample + "/")
		
		
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
	project_name=GlobalParameter().projectName
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().seq_platforms
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
