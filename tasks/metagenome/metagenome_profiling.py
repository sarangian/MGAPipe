import os
import luigi
import os
import time
import subprocess
import pandas as pd
from luigi import Parameter


from tasks.metagenome.readCleaning.preProcessReads import cleanFastq
from tasks.metagenome.readCleaning.preProcessReads import filtlong
from tasks.metagenome.readCleaning.reFormatReads import reformat



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




############################################
class GlobalParameter(luigi.Config):
	threads = luigi.Parameter()
	maxMemory = luigi.Parameter()
	projectName = luigi.Parameter()
	domain=luigi.Parameter()
	pe_read_dir=luigi.Parameter()
	adapter=luigi.Parameter()
	seq_platforms=luigi.Parameter()

class metaphlan(luigi.Task):
	project_name=luigi.Parameter(default="MetagenomeAnalysis")
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().seq_platforms
	sampleName = luigi.Parameter(description="name of the sample to be analyzed. (string)")


	def requires(self):
		if self.read_library_type == "pe" and self.pre_process_reads=="yes":
			return [cleanFastq(sampleName=i)
				for i in [line.strip()
						  for line in
						  open((os.path.join(os.getcwd(), "config", "pe_samples.lst")))]]

		if self.read_library_type == "pe" and self.pre_process_reads=="no":
			return [reformat(sampleName=i)
				for i in [line.strip()
						  for line in
						  open((os.path.join(os.getcwd(), "config", "pe_samples.lst")))]]

		

	def output(self):
		profiled_samples = os.path.join(os.getcwd(), GlobalParameter().projectName, "metaphlan_analysis","profiled_samples" )

			
		return {'out1': luigi.LocalTarget(profiled_samples, self.sampleName +".txt"),
				'out2': luigi.LocalTarget(profiled_samples, self.sampleName +".out")}
		
	def run(self):
		#metaphlan_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "metaphlan_analysis" )
		profiled_samples= os.path.join(os.getcwd(), GlobalParameter().projectName, "metaphlan_analysis", "profiled_samples" + "/")

		#createFolder(profiled_samples)

		if self.pre_process_reads=="no":
			pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "VerifiedReads", "PE-Reads" + "/")
			
		if self.pre_process_reads=="yes":
			pe_read_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "ReadQC", "CleanedReads", "PE-Reads" + "/")

	

		run_metaphlan="[ -d {profiled_samples} ] || mkdir -p {profiled_samples} ; " \
					  "metaphlan --input_type fastq " \
					  "{pe_read_folder}{sample}_1.fastq,{pe_read_folder}{sample}_2.fastq " \
					  "--nproc {threads} " \
					  "--bt2_ps very-sensitive " \
					  "--bowtie2out {profiled_samples}{sample}.out 2>&1 | tee {profiled_samples}{sample}.txt ".format(
						pe_read_folder=pe_read_folder,profiled_samples=profiled_samples,sample=self.sampleName,threads=self.threads)

		print("****NOW RUNNING COMMAND****:" + run_metaphlan)
		print(run_cmd(run_metaphlan))


class compositionProfiling(luigi.Task):
	project_name=GlobalParameter().projectName
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().seq_platforms

	def requires(self):

		return [metaphlan(pre_process_reads=self.pre_process_reads, 
				sampleName=i)
                	for i in [line.strip()
                          for line in
                          	open((os.path.join(os.getcwd(), "config", "pe_samples.lst")))]]	


	def output(self):
		timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
		return luigi.LocalTarget(os.path.join(os.getcwd(),"task_logs",'task.genome.binning.complete.{t}'.format(
			t=timestamp)))

	def run(self):
		timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
		with self.output().open('w') as outfile:
			outfile.write('Metagenome binning finished at {t}'.format(t=timestamp))


class graphlan(luigi.Task):
	project_name=GlobalParameter().projectName
	#assembly_type=luigi.ChoiceParameter(default="single",choices=["single", "co"], var_type=str)
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().seq_platforms


	def requires(self):
		return [metaphlan(pre_process_reads=self.pre_process_reads, 
					sampleName=i)
                	for i in [line.strip()
                          for line in
                          	open((os.path.join(os.getcwd(), "config", "pe_samples.lst")))]]	



	def output(self):
		metaphlan_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "metaphlan_analysis" ,"profiled_samples" + "/")
		outDir=os.path.join(os.getcwd(), GlobalParameter().projectName,"metaphlan_analysis" ,"data_for_images" + "/")
		graphlan_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "metaphlan_analysis" ,"figures" + "/")

		return {'out1': luigi.LocalTarget(outDir + "/" + "merged_abundance_table.txt"),
				'out2': luigi.LocalTarget(outDir + "/" + "otu_table_ampvis2.txt")
				}
				
				#'out6': luigi.LocalTarget(graphlan_folder + "/" + "graphlan_merged_abundance.pdf")}

	def run(self):
		metaphlan_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "metaphlan_analysis" ,"profiled_samples" + "/")
		outDir=os.path.join(os.getcwd(), GlobalParameter().projectName, "metaphlan_analysis" ,"data_for_images" + "/")
		graphlan_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "metaphlan_analysis" ,"figures" + "/")


		run_merge_metaphln_tables="[ -d {outDir} ] || mkdir -p {outDir} ; " \
								  "merge_metaphlan_tables.py {metaphlan_folder}*.txt > {outDir}merged_abundance_table.txt".format(outDir=outDir,metaphlan_folder=metaphlan_folder)
		
		print("****NOW RUNNING COMMAND****:" + run_merge_metaphln_tables)
		print(run_cmd(run_merge_metaphln_tables))


		cmd_prepare_file="grep -E '(s__)' {outDir}/merged_abundance_table.txt |cut -f2 --complement | grep -v 't__' | sed 's/^.*s__//g' " \
						 "> {outDir}/merged_abundance_table_species.txt".format(outDir=outDir)

		cmd_generate_header="cat {outDir}/merged_abundance_table.txt | head -n 2 | tail -n 1 | cut -f1 --complement " \
							"> {outDir}/header.txt".format(outDir=outDir)

		cmd_merge_header="cat {outDir}/header.txt {outDir}/merged_abundance_table_species.txt " \
						 "> {outDir}/abundance_table_species.txt".format(outDir=outDir)

		cmd_taxonomy_phyloseq="cat {outDir}/merged_abundance_table.txt | grep -E 's__|clade' " \
							"| grep -v '#'| grep -v 'MetaPhlAn'| grep -v 'WARNING'| " \
							"sed -e 's/clade_name/taxonomy/'|sed -e 's/|/;/g'|cut -f1 " \
							 "> {outDir}/taxonomy_phyloseq.txt".format(outDir=outDir)

		cmd_sample_abundance="cat {outDir}/merged_abundance_table.txt | grep -E 's__|clade' " \
							 "| grep -v '#'| grep -v 'MetaPhlAn'| grep -v 'WARNING'| " \
							 "sed -e 's/clade_name/taxonomy/'|sed -e 's/|/;/g'|cut -f1,2 --complement " \
							 "> {outDir}/sample_abundance.txt".format(outDir=outDir)

		cmd_otu_id_phyloseq="cat {outDir}/taxonomy_phyloseq.txt |sed 's/^.*s__//g' | sed -e 's/taxonomy/#OTU ID/' " \
							"> {outDir}/otu_id_phyloseq.txt".format(outDir=outDir)

		cmd_otu_table_pgyloseq="paste {outDir}/otu_id_phyloseq.txt {outDir}/sample_abundance.txt {outDir}/taxonomy_phyloseq.txt " \
							   "> {outDir}otu_table_phyloseq.txt".format(outDir=outDir)

		cmd_phyloseq_biom="biom convert -i  {outDir}/otu_table_phyloseq.txt " \
						  "-o  {outDir}/otu_table_phyloseq.biom --table-type='OTU table' " \
						  "--process-obs-metadata taxonomy --to-json".format(outDir=outDir)

		cmd_taxonomy_ampvis2="cat {outDir}/taxonomy_phyloseq.txt |sed -e 's/;/;\t/g' | " \
							 "sed -e '1s/taxonomy/Kingdom\tPhylum\tClass\tOrder\tFamily\tGenus\tSpecies/g' " \
							"> {outDir}/taxonomy_ampvis2.txt".format(outDir=outDir)

		cmd_otu_phyloseq="cat {outDir}/taxonomy_phyloseq.txt |sed 's/^.*s__//g' " \
						 "| sed -e 's/taxonomy/OTU/' > {outDir}/otu_id_ampvis2.txt".format(outDir=outDir)

		cmd_otu_ampvis2="paste {outDir}/otu_id_ampvis2.txt {outDir}/sample_abundance.txt {outDir}/taxonomy_ampvis2.txt" \
						" > {outDir}/otu_table_ampvis2.txt".format(outDir=outDir)

		
		print("****NOW RUNNING COMMAND****:" + cmd_taxonomy_phyloseq)
		print(run_cmd(cmd_taxonomy_phyloseq))		
		print("****NOW RUNNING COMMAND****:" + cmd_sample_abundance)
		print(run_cmd(cmd_sample_abundance))
		print("****NOW RUNNING COMMAND****:" + cmd_otu_id_phyloseq)
		print(run_cmd(cmd_otu_id_phyloseq))	
		print("****NOW RUNNING COMMAND****:" + cmd_otu_table_pgyloseq)
		print(run_cmd(cmd_otu_table_pgyloseq))	
		print("****NOW RUNNING COMMAND****:" + cmd_phyloseq_biom)
		print(run_cmd(cmd_phyloseq_biom))	
		print("****NOW RUNNING COMMAND****:" + cmd_taxonomy_ampvis2)
		print(run_cmd(cmd_taxonomy_ampvis2))	
		print("****NOW RUNNING COMMAND****:" + cmd_otu_phyloseq)
		print(run_cmd(cmd_otu_phyloseq))	
		print("****NOW RUNNING COMMAND****:" + cmd_otu_ampvis2)
		print(run_cmd(cmd_otu_ampvis2))	

		print("****NOW RUNNING COMMAND****:" + cmd_prepare_file)
		print(run_cmd(cmd_prepare_file))

		print("****NOW RUNNING COMMAND****:" + cmd_generate_header)
		print(run_cmd(cmd_generate_header))


		print("****NOW RUNNING COMMAND****:" + cmd_merge_header)
		print(run_cmd(cmd_merge_header))



		cmd_run_hclust2="[ -d {graphlan_folder} ] || mkdir -p {graphlan_folder} ; " \
					"hclust2.py -i {outDir}/abundance_table_species.txt " \
					"-o {graphlan_folder}/hclust_abundance_heatmap_species.png " \
					"--ftop 25 " \
					"--f_dist_f braycurtis " \
					"--s_dist_f braycurtis " \
					"--cell_aspect_ratio 0.5 -l " \
					"--flabel_size 6 " \
					"--slabel_size 6 " \
					"--max_flabel_len 100 " \
					"--max_slabel_len 100 --minv 0.1 --dpi 300 ".format(outDir=outDir,graphlan_folder=graphlan_folder)

	
		
		cmd_abundance_reformat="tail -n +5 {outDir}/merged_abundance_table.txt | cut -f1,3- " \
							   "> {outDir}/merged_abundance_table_reformatted.txt".format(outDir=outDir)
		
		print("****NOW RUNNING COMMAND****:" + cmd_abundance_reformat)
		print(run_cmd(cmd_abundance_reformat))

		cmd_run_graphlan_prep="export2graphlan.py --skip_rows 1 " \
					"-i {outDir}/merged_abundance_table_reformatted.txt " \
					"--tree {outDir}/merged_abundance.tree.txt " \
					"--annotation {outDir}/merged_abundance.annot.txt " \
					"--most_abundant 100 " \
					"--abundance_threshold 1 " \
					"--least_biomarkers 10 " \
					"--annotations 5,6 " \
					"--external_annotations 7 "  \
					"--min_clade_size 1 ".format(outDir=outDir)


		cmd_run_graphlan_annotate="[ -d {outDir} ] || mkdir -p {outDir} ; " \
					"graphlan_annotate.py " \
					"--annot {outDir}/merged_abundance.annot.txt " \
					"{outDir}/merged_abundance.tree.txt " \
					"{outDir}/merged_abundance.xml ".format(outDir=outDir)


		cmd_run_graphlan="[ -d {graphlan_folder} ] || mkdir -p {graphlan_folder} ; " \
					"graphlan.py " \
					"--dpi 300 " \
					"--size 15 " \
					"--format pdf " \
					"{outDir}/merged_abundance.xml " \
					"{graphlan_folder}/graphlan_merged_abundance.pdf ".format(outDir=outDir,graphlan_folder=graphlan_folder)

		
		
		print("****NOW RUNNING COMMAND****:" + cmd_run_hclust2)
		print(run_cmd(cmd_run_hclust2))

		#subprocess.run('conda activate graphlan', shell=True)
		
		print("****NOW RUNNING COMMAND****:" + cmd_run_graphlan_prep)
		print(run_cmd(cmd_run_graphlan_prep))

		print("****NOW RUNNING COMMAND****:" + cmd_run_graphlan_annotate)
		print(run_cmd(cmd_run_graphlan_annotate))

		print("****NOW RUNNING COMMAND****:" + cmd_run_graphlan)
		print(run_cmd(cmd_run_graphlan))
		#subprocess.run('conda deactivate graphlan', shell=True)
		

class profileTaxonomy(luigi.Task):
	project_name=GlobalParameter().projectName
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	read_library_type = GlobalParameter().seq_platforms
	condition_column=luigi.Parameter(default="conditions")
	

	def requires(self):
		return [graphlan(pre_process_reads=self.pre_process_reads)]

	def output(self):
		ampvis_image_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "metaphlan_analysis" ,"figures" + "/")
		return {'out1': luigi.LocalTarget(ampvis_image_folder + "/" + "family_heatmap.tiff")}


	def run(self):
		ampvis_folder = os.path.join(os.getcwd(), GlobalParameter().projectName, "metaphlan_analysis" + "/")
		inDir=os.path.join(os.getcwd(), GlobalParameter().projectName, "metaphlan_analysis" ,"data_for_images" + "/")
		map_file=os.path.join(os.getcwd(), "config","metagenome_condition.tsv")

		cmd_run_ampvis="[ -d {ampvis_folder} ] || mkdir -p {ampvis_folder} ;  cd {ampvis_folder} ;" \
						"ampvis.r -t {map_file} " \
						"-v {condition_column} " \
						"-a {inDir}/otu_table_ampvis2.txt".format(map_file=map_file,
																 inDir=inDir,
																 ampvis_folder=ampvis_folder,
																 condition_column=self.condition_column)
		print("****NOW RUNNING COMMAND****:" + cmd_run_ampvis)
		print(run_cmd(cmd_run_ampvis))
