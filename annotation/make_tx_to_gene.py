import luigi
import os
import time
import subprocess
from tasks.metagenome.annotation.prokaryotic_annotation import annotateGenome


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
	threads = luigi.Parameter()
	maxMemory = luigi.Parameter()

class makeTx2Gene(luigi.Task):

	#Global Parameters
	project_name=luigi.Parameter(default="RNASeqAnalysis")
	genome_name=GlobalParameter().genome_name
	genome_suffix = GlobalParameter().genome_suffix
	organism_domain=GlobalParameter().organism_domain
	annotation_file_type = luigi.ChoiceParameter(choices=["NA","GFF","GTF"],var_type=str)
	#Local Parameters

	def requires(self):
		if (self.organism_domain == "prokaryote") and (self.annotation_file_type == "NA"):
			return [annotateGenome()]

	def output(self):

		transcriptomeFolder = os.path.join(os.getcwd(), self.project_name,"alignment_free_dea", "transcript_index", self.genome_name + "_transcriptome" + "/")

		return {'out1': luigi.LocalTarget(transcriptomeFolder + "tx2gene.csv"),
			   'out2': luigi.LocalTarget(transcriptomeFolder + self.genome_name + ".gtf"),
			   'out3': luigi.LocalTarget(transcriptomeFolder + self.genome_name + ".ffn")
				}

	def run(self):
		transcriptomeFolder = os.path.join(os.getcwd(), self.project_name,"alignment_free_dea", "transcript_index", self.genome_name + "_transcriptome" + "/")

		genomeFolder = os.path.join(os.getcwd(), GlobalParameter().genome_dir + "/")

		cmd_extract_transcript_from_genome = "[ -d  {transcriptomeFolder} ] || mkdir -p {transcriptomeFolder}; " \
							  "gffread -w {transcriptomeFolder}{genome_name}.ffn " \
							   "-g {genomeFolder}{genome_name}.{genome_suffix} {genomeFolder}{genome_name}.gtf -F " \
							   .format(genomeFolder=genomeFolder,
									   transcriptomeFolder=transcriptomeFolder,
									   genome_name=self.genome_name,
									   genome_suffix=self.genome_suffix)

		cmd_copy_gtf = "[ -d  {transcriptomeFolder} ] || mkdir -p {transcriptomeFolder}; " \
								  "cp {genomeFolder}{genome_name}.gtf {transcriptomeFolder}{genome_name}.gtf " \
								  .format(genomeFolder=genomeFolder,
								   genome_name=self.genome_name,
								   transcriptomeFolder=transcriptomeFolder)

		cmd_gff2gtf =   "gffread -E  {genomeFolder}{genome_name}.gff -T -o {genomeFolder}{genome_name}.gtf " \
								  .format(genome_name=self.genome_name,
								   genomeFolder=genomeFolder)

		cmd_tx2gene_for_eukaryote = "cd {transcriptomeFolder}; tx2gene.R " \
							   "-a gtf " \
							   "-p {transcriptomeFolder}{genome_name}.gtf " \
							   "-o tx2gene.csv" \
							   .format(transcriptomeFolder=transcriptomeFolder,
									   genome_name=self.genome_name)


		tx2g_cmd1 = ''' awk '$3=="transcript"' | cut -f9 | tr -s ";" " " |awk '{print$4","$2}' ''' 
		tx2g_cmd2 = ''' sed 's/\"//g' | sed -e '1i\TRANSCRIPT,GENE' '''

		
		cmd_tx2gene_for_prokaryote = "[ -d  {transcriptomeFolder} ] || mkdir -p {transcriptomeFolder}; cd {transcriptomeFolder};" \
								  "cat {transcriptomeFolder}{genome_name}.gtf |{tx2g_cmd1} | {tx2g_cmd2} > tx2gene.csv" \
								  .format(transcriptomeFolder=transcriptomeFolder,
									  genome_name=self.genome_name,
									  tx2g_cmd1=tx2g_cmd1,
										  tx2g_cmd2=tx2g_cmd2)
		

		if (self.annotation_file_type == "GTF") and (self.organism_domain == "eukaryote"):

			print ("****** NOW RUNNING COMMAND ******: " + cmd_copy_gtf)
			print (run_cmd(cmd_copy_gtf))
			
			print ("****** NOW RUNNING COMMAND ******: " + cmd_extract_transcript_from_genome)
			print (run_cmd(cmd_extract_transcript_from_genome))

			print ("****** NOW RUNNING COMMAND ******: " + cmd_tx2gene_for_eukaryote)
			print (run_cmd(cmd_tx2gene_for_eukaryote))
			
		
		if (self.annotation_file_type == "GFF") and (self.organism_domain == "eukaryote"):
			
			print ("****** NOW RUNNING COMMAND ******: " + cmd_gff2gtf)
			print (run_cmd(cmd_gff2gtf))

			print ("****** NOW RUNNING COMMAND ******: " + cmd_copy_gtf)
			print (run_cmd(cmd_copy_gtf))
			
			print ("****** NOW RUNNING COMMAND ******: " + cmd_extract_transcript_from_genome)
			print (run_cmd(cmd_extract_transcript_from_genome))

			print ("****** NOW RUNNING COMMAND ******: " + cmd_tx2gene_for_eukaryote)
			print (run_cmd(cmd_tx2gene_for_eukaryote))
			
		
		if (self.annotation_file_type == "GTF") and (self.organism_domain == "prokaryote"):
			
			print ("****** NOW RUNNING COMMAND ******: " + cmd_copy_gtf)
			print (run_cmd(cmd_copy_gtf))
			
			print ("****** NOW RUNNING COMMAND ******: " + cmd_extract_transcript_from_genome)
			print (run_cmd(cmd_extract_transcript_from_genome))

			print ("****** NOW RUNNING COMMAND ******: " + cmd_tx2gene_for_prokaryote)
			print (run_cmd(cmd_tx2gene_for_prokaryote))


		if (self.annotation_file_type == "GFF") and (self.organism_domain == "prokaryote"):
			print ("****** NOW RUNNING COMMAND ******: " + cmd_gff2gtf)
			print (run_cmd(cmd_gff2gtf))
			
			print ("****** NOW RUNNING COMMAND ******: " + cmd_copy_gtf)
			print (run_cmd(cmd_copy_gtf))

			
			print ("****** NOW RUNNING COMMAND ******: " + cmd_extract_transcript_from_genome)
			print (run_cmd(cmd_extract_transcript_from_genome))

			print ("****** NOW RUNNING COMMAND ******: " + cmd_tx2gene_for_prokaryote)
			print (run_cmd(cmd_tx2gene_for_prokaryote))

