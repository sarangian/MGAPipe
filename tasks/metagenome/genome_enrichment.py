
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
from tasks.metagenome.genome_dereplicate import conditionBasedDeRep

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

class enrichBins(luigi.Task):
	project_name=GlobalParameter().projectName
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	checkM_method=luigi.ChoiceParameter(default="taxonomy_wf",choices=["taxonomy_wf", "lineage_wf"], var_type=str)
	read_library_type = GlobalParameter().seq_platforms
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

		enrichm_annotate_output=os.path.join(os.getcwd(), GlobalParameter().projectName,"bin_enrichment_analysis","enrichm_annotate_out"+"/")			
		return {'out1': luigi.LocalTarget(enrichm_annotate_output + "cazy_frequency_table.tsv"),
				'out2': luigi.LocalTarget(enrichm_annotate_output + "pfam_frequency_table.tsv"),
				'out3': luigi.LocalTarget(enrichm_annotate_output + "tigrfam_frequency_table.tsv")}

	def run(self):
		'''
		dRep_ref_cond_out_folder=os.path.join(os.getcwd(),GlobalParameter().projectName,"dRep_bins", self.reference_condition +"_out"+"/")
		dRep_con_cond_out_folder=os.path.join(os.getcwd(),GlobalParameter().projectName,"dRep_bins", self.contrast_condition +"_out"+"/")
		enrichm_annotate_input_merge_by_condition=os.path.join(os.getcwd(), GlobalParameter().projectName,"dRep_bins","dReplicated_bins_condition_based"+"/")
		dRep_ref_genomes=os.path.join(dRep_ref_cond_out_folder,"dereplicated_genomes")
		dRep_con_genomes=os.path.join(dRep_con_cond_out_folder,"dereplicated_genomes")

		cmd_prepare_enrichm_annotate_input="[ -d  {enrichm_annotate_input_merge_by_condition} ] || mkdir -p {enrichm_annotate_input_merge_by_condition} ; cd {enrichm_annotate_input_merge_by_condition} ; " \
									   "cp {dRep_ref_genomes}/*.fa {enrichm_annotate_input_merge_by_condition} ; " \
									   "cp {dRep_con_genomes}/*.fa {enrichm_annotate_input_merge_by_condition} ;".format(enrichm_annotate_input_merge_by_condition=enrichm_annotate_input_merge_by_condition,
										dRep_ref_genomes=dRep_ref_genomes,dRep_con_genomes=dRep_con_genomes)
		print("****** NOW RUNNING COMMAND ******: " + cmd_prepare_enrichm_annotate_input)
		print(run_cmd(cmd_prepare_enrichm_annotate_input))
		

		bin_enrichment_analysis=os.path.join(os.getcwd(), GlobalParameter().projectName,"bin_enrichment_analysis")
		createFolder(bin_enrichment_analysis)
		enrichm_annotate_out=os.path.join(os.getcwd(), GlobalParameter().projectName,"bin_enrichment_analysis", "enrichm_annotate_out"+"/")

		'''
		bin_enrichment_analysis=os.path.join(os.getcwd(), GlobalParameter().projectName,"bin_enrichment_analysis")
		createFolder(bin_enrichment_analysis)
		enrichm_annotate_out=os.path.join(os.getcwd(), GlobalParameter().projectName,"bin_enrichment_analysis", "enrichm_annotate_out"+"/")
	
		sample_group_file=os.path.join(os.getcwd(), "config","metagenome_group.tsv")

		dRep_bins_path=os.path.join(os.getcwd(), GlobalParameter().projectName ,"dRep_bins","dReplicated_bins_condition_based"+"/")


		file_names=os.listdir(dRep_bins_path)
		bin_names = []
		for string in file_names:
			new_string = string.replace(".fa", ".")
			bin_names.append(new_string)
		df1 = pd.DataFrame(bin_names) 
		df1.columns = ['bin_names']
		df1['samples'] = df1['bin_names'].str.split('_').str[0]
	
		df2=pd.read_csv(sample_group_file, sep='\t+', engine='python',header=None)
		df2.columns=['samples', 'conditions']
	
		outer_join = pd.merge(df1,  df2,  on='samples', how ='outer')
		metadata=outer_join[['bin_names','conditions']]

		nan_value = float("NaN")
		metadata.replace("", nan_value, inplace=True)
		metadata.dropna(subset = ["bin_names"], inplace=True)

		
		metadata.to_csv(bin_enrichment_analysis+"/"+"metadata.tsv",sep='\t',header=False, index=False)	

		cmd_run_annotate="enrichm annotate --genome_directory {dRep_bins_path} --suffix fa " \
				   "--output {enrichm_annotate_out} "  \
				   "--ko --pfam --tigrfam --cazy " \
				   "--threads {threads} " \
				   "--parallel {threads} ".format(dRep_bins_path=dRep_bins_path,
												  enrichm_annotate_out=enrichm_annotate_out,
												  threads=self.threads)
		print("****** NOW RUNNING COMMAND ******: " + cmd_run_annotate)
		print (run_cmd(cmd_run_annotate))
		

#############################################################################################################################
class differentialEnrichmentAnalysis(luigi.Task):
	project_name=GlobalParameter().projectName
	adapter = GlobalParameter().adapter
	threads = GlobalParameter().threads
	max_memory = GlobalParameter().maxMemory
	pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
	checkM_method=luigi.ChoiceParameter(default="taxonomy_wf",choices=["taxonomy_wf", "lineage_wf"], var_type=str)
	read_library_type = GlobalParameter().seq_platforms
	min_contig_length=luigi.IntParameter(default="1500")
	completeness=luigi.IntParameter(default="50")
	contamination=luigi.IntParameter(default="25")
	reference_condition=luigi.Parameter()
	contrast_condition=luigi.Parameter()
	pvalcutoff=luigi.FloatParameter(default="0.05")
	correction = luigi.ChoiceParameter(default="fdr_bh",choices=["fdr_bh", "fdr_by","fdr_tsbh","fdr_tsbky","fdr_gbs","b","s","h","hs","sh","ho"], var_type=str)
	#dRep_method=luigi.ChoiceParameter(choices=["condition_based", "condition_free"], var_type=str)
	
	def requires(self):
			return [enrichBins(
					   checkM_method=self.checkM_method,
					   pre_process_reads=self.pre_process_reads,
					   completeness=self.completeness,
					   contamination=self.contamination,
					   min_contig_length=self.min_contig_length,
					   reference_condition=self.reference_condition,
					   contrast_condition=self.contrast_condition)]


	def output(self):

		#enrichm_ec_output = os.path.join(os.getcwd(), GlobalParameter().projectName, "bin_enrichment_analysis", "enrichm_enrichment_ec_out" + "/")
		enrichm_ko_output = os.path.join(os.getcwd(), GlobalParameter().projectName, "bin_enrichment_analysis", "enrichm_enrichment_ko_out" + "/")
		enrichm_pfam_output = os.path.join(os.getcwd(), GlobalParameter().projectName, "bin_enrichment_analysis", "enrichm_enrichment_pfam_out" + "/")
		enrichm_cazy_output = os.path.join(os.getcwd(), GlobalParameter().projectName, "bin_enrichment_analysis", "enrichm_enrichment_cazy_out" + "/")
		#enrichm_kohmm_output = os.path.join(os.getcwd(), GlobalParameter().projectName, "bin_enrichment_analysis", "enrichm_enrichment_kohmm_out" + "/")
		enrichm_tigrfam_output = os.path.join(os.getcwd(), GlobalParameter().projectName, "bin_enrichment_analysis", "enrichm_enrichment_tigrfam_out" + "/")


		return {'out1': luigi.LocalTarget(enrichm_ko_output + "proportions.tsv"),
				'out2': luigi.LocalTarget(enrichm_pfam_output + "proportions.tsv"),
				'out3': luigi.LocalTarget(enrichm_cazy_output + "proportions.tsv"),
				'out5': luigi.LocalTarget(enrichm_tigrfam_output + "proportions.tsv")}
			   

	def run(self):

		
		metadata_file=os.path.join(os.getcwd(), GlobalParameter().projectName, "bin_enrichment_analysis","metadata.tsv")
		enrichm_annotate_out = os.path.join(os.getcwd(), GlobalParameter().projectName, "bin_enrichment_analysis", "enrichm_annotate_out" + "/")

		enrichm_ko_output = os.path.join(os.getcwd(), GlobalParameter().projectName, "bin_enrichment_analysis", "enrichm_enrichment_ko_out" + "/")
		enrichm_pfam_output = os.path.join(os.getcwd(), GlobalParameter().projectName, "bin_enrichment_analysis", "enrichm_enrichment_pfam_out" + "/")
		enrichm_cazy_output = os.path.join(os.getcwd(), GlobalParameter().projectName, "bin_enrichment_analysis", "enrichm_enrichment_cazy_out" + "/")
		enrichm_tigrfam_output = os.path.join(os.getcwd(), GlobalParameter().projectName, "bin_enrichment_analysis", "enrichm_enrichment_tigrfam_out" + "/")

		
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

		'''
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
		'''

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


		'''
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
		'''
