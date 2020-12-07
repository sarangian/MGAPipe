import luigi
import os
import time
import subprocess
#from tasks.rnaSeq.index_genome import indexGenome
from tasks.metagenome.readCleaning.preProcessReads import bbduk
from tasks.metagenome.readCleaning.preProcessReads import filtlong
from tasks.metagenome.readCleaning.reFormatReads import reformat


def run_cmd(cmd):
    p = subprocess.Popen(cmd, bufsize=-1,
                         shell=True,
                         universal_newlines=True,
                         stdout=subprocess.PIPE,
                         executable='/bin/bash')
    output = p.communicate()[0]
    return output

def createFolder(directory):
	try:
		if not os.path.exists(directory):
			os.makedirs(directory)
	except OSError:
		print ('Error: Creating directory. ' + directory)

createFolder("task_logs")

class GlobalParameter(luigi.Config):
	genome_suffix=luigi.Parameter()
	read_library_type=luigi.Parameter()
	organism_domain=luigi.Parameter()
	genome_name=luigi.Parameter()
	genome_dir=luigi.Parameter()
	adapter=luigi.Parameter()
	read_library_type=luigi.Parameter()
	threads = luigi.Parameter()
	maxMemory = luigi.Parameter()

class annotateGenome(luigi.Task):
	#Global Parameters
	genome_dir=GlobalParameter().genome_dir
	genome_name = GlobalParameter().genome_name
	adapter = GlobalParameter().adapter
	read_library_type = GlobalParameter().read_library_type
	threads = GlobalParameter().threads
	project_name=luigi.Parameter(default="RNASeqAnalysis")
	genome_suffix = GlobalParameter().genome_suffix

	#Local Parameters

	def requires(self):
		return []

	def output(self):
		transcriptomeFolder = os.path.join(os.getcwd(),self.project_name, "alignment_free_dea", self.genome_name+"_transcriptome" + "/")
		genomeFolder = os.path.join(GlobalParameter().genome_dir+ "/")
		return {'out1': luigi.LocalTarget(transcriptomeFolder +"/"  + self.genome_name + ".ffn"),
				'out2': luigi.LocalTarget(genomeFolder +"/"  + self.genome_name + ".gff"),
				'out3': luigi.LocalTarget(transcriptomeFolder + "/" + "tx2gene.csv")
				}

	def run(self):
		transcriptomeFolder = os.path.join(os.getcwd(),self.project_name, "alignment_free_dea", self.genome_name+"_transcriptome" + "/")
		genomeFolder = os.path.join(os.getcwd(),GlobalParameter().genome_dir + "/")
		annotationFolder = os.path.join(os.getcwd(),self.project_name,"alignment_free_dea", "transcriptome",self.genome_name + "_PROKKA" +"/")


		cmd_run_prokka = "prokka {genomeFolder}{genome_name}.{genome_suffix} " \
						 "--cpu {threads} " \
						 "--prefix {genome_name} " \
						 "--mincontiglen {minContigLength} " \
						 "--outdir {annotationFolder} --force --rfam" \
			.format(genome_name=self.genome_name,
					annotationFolder=annotationFolder,
					genome_suffix=self.genome_suffix,
					minContigLength=self.minContigLength,
					genomeFolder=genomeFolder,
					threads=self.threads)

		print ("****** NOW RUNNING COMMAND ******: " + cmd_run_prokka)
		print (run_cmd(cmd_run_prokka))

		print("Generating tx2gene")
		awk_cmd = 'BEGIN{FS="\\t"}{print ""$1"," ""$1"*" $7}'
		cmd_run_tx2gene = "[ -d  {transcriptomeFolder} ] || mkdir -p {transcriptomeFolder}; " \
						   "cd {annotationFolder}; " \
						  "awk '{awk}' {genome_name}.tsv > {transcriptomeFolder}tx2gene.csv" \
			.format(annotationFolder=annotationFolder,
					transcriptomeFolder=transcriptomeFolder,
					genome_name=self.genome_name,
					awk=awk_cmd)

		print ("****** NOW RUNNING COMMAND ******: " + cmd_run_tx2gene)
		print (run_cmd(cmd_run_tx2gene))

		
		print("Generating GTF from GFF")
		sed_cmd2 = ''' sed 's/ID//g' '''
		awk_cmd2 = 'BEGIN{OFS="\\t"}{print $1,"PROKKA","CDS",$2,$3,".",$4,".","gene_id " $5}'
		cmd_run_gff2gtf = "cd {annotationFolder}; " \
						  "grep -v '#' {genome_name}.gff | grep 'ID=' | {sed_cmd2} | " \
						  "cut -f1,4,5,7,9 | awk '{awk_cmd2}' > {genome_name}.gtf " \
			.format(genomeFolder=genomeFolder,
					annotationFolder=annotationFolder,
					genome_name=self.genome_name,
					sed_cmd2=sed_cmd2,
					awk_cmd2=awk_cmd2)

		print ("****** NOW RUNNING COMMAND ******: " + cmd_run_gff2gtf)
		print (run_cmd(cmd_run_gff2gtf))


		cmd_copy_files = "cd {annotationFolder}; " \
						 "cp {genome_name}.ffn {transcriptomeFolder}{genome_name}.ffn && " \
						 "cp {genome_name}.gff {genomeFolder}{genome_name}.gff && " \
						 "cp {genome_name}.gtf {transcriptomeFolder}{genome_name}.gtf " \
						 .format(annotationFolder=annotationFolder,
							  genomeFolder=genomeFolder,
							  transcriptomeFolder=transcriptomeFolder,
							  genome_name=self.genome_name)
					
		print ("****** NOW RUNNING COMMAND ******: " + cmd_copy_files)
		print (run_cmd(cmd_copy_files))
