#! /usr/bin/env python3
import os
import re
import shutil
from collections import OrderedDict
import optparse
from sys import exit
import psutil
import subprocess
import pandas as pd
import luigi

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


totalcpus = psutil.cpu_count()
threads = str(totalcpus -1)

mem = psutil.virtual_memory()
maxMemory= int((mem.available/1073741824) -1)

class configureProject(luigi.Task):
	
	cpus = luigi.Parameter(default=f'{threads}')
	maxMemory=luigi.Parameter(default=f'{maxMemory}')
	domain=luigi.Parameter(description="domain of the organism")
	dataDir=luigi.Parameter(description="Path to FASTQ Data DIrectory")
	projectName=luigi.Parameter()
	schedulerPort=luigi.Parameter(default="8082")
	userEmail=luigi.Parameter(description="Your email")
	symLinkDir=luigi.Parameter(default="symLinkDir",description="Symbolic Link Directory Name")
	
	def require(self):
		return[]

	def output(self):
		current_folder=os.path.join(os.getcwd())
		return {'out': luigi.LocalTarget(current_folder + ".luigi.cfg.tmp")}

	def run (self):
		current_folder=os.path.join(os.getcwd())
		adapter=os.path.abspath(os.path.join(os.getcwd(),"tasks","utility",'adapters.fasta.gz'))
		paired_end_read_dir=os.path.abspath(os.path.join(os.path.join(os.getcwd()),self.symLinkDir, "pe" ))
		pac_read_dir=os.path.abspath(os.path.join(os.path.join(os.getcwd()),self.symLinkDir,"pac"))
		ont_read_dir=os.path.abspath(os.path.join(os.path.join(os.getcwd()),self.symLinkDir,"ont"))
		projectDir=os.path.abspath(os.path.join(os.path.join(os.getcwd()),self.projectName))

		dataDir=os.path.abspath(os.path.join(os.path.join(os.getcwd()),self.dataDir))

		symLinkDir=os.path.abspath(os.path.join(os.path.join(os.getcwd()),self.symLinkDir))

		createFolder(paired_end_read_dir)
		createFolder(pac_read_dir)
		createFolder(ont_read_dir)
		createFolder(projectDir)
		createFolder("config")

		if os.path.isdir(dataDir):
			files = [f for f in os.listdir(dataDir) if os.path.isfile(os.path.join(dataDir, f))]
			keys = []
			fileList = re.compile(r'^(.+?).(fastq|fq|fastq\.gz|fq\.gz)?$')
			for file in files:
				if fileList.search(file):
					keys.append(file)

		dicts = OrderedDict ()
		#keys = [f for f in os.listdir(".") if os.path.isfile(os.path.join(".", f))]

		for i in keys:
			
			accepted_values_genome="pe ont pac".split()
			
			val = str(input("Enter Data Type of {data}: \tchoose from [pe:paired-end, ont:nanopore, pac:pacbio]: ".format(data=i)))
			if val in accepted_values_genome:
				dicts[i] = val
			else:
				print(f'{val} is not a valid option. \tchoose from [pe, mp, ont, pac]: ')
				val = str(input("Enter Data Type of {data}: \tchoose from [pe, mp, ont, pac]: ".format(data=i)))

		#print(dicts)

		for key, val in dicts.items():
			if not os.path.exists(os.path.join(symLinkDir, val)):
				os.mkdir(os.path.join(symLinkDir, val))

		##ln -nsf method
		for key, val in dicts.items():
			dest = (os.path.join(symLinkDir,val,key))
			src = (os.path.join(dataDir,key))
			source = os.path.abspath(src)
			destination = os.path.abspath(dest)
			escape="\'"
			print("Source:\t {skip}{src}{skip}".format(skip=escape,src=source))
			print("Destination:\t {skip}{dest}{skip}".format(skip=escape,dest=destination))
			#print("Desitnation:", '\'destination\'')

			link_cmd = "ln -nsf "
			create_symlink = "{link_cmd} {source} {destination}".format(link_cmd=link_cmd,source=source,destination=destination)
			print("****** NOW RUNNING COMMAND ******: " + create_symlink)
			print (run_cmd(create_symlink))

		###########################################
		def paired_end_samples(pe_dir):
			pe_read_list=os.listdir(paired_end_read_dir)

			sample_list=[]
			for read in pe_read_list:
				pe_allowed_extn=["_R1.fq","_R1.fastq","_R1.fq.gz","_R1.fastq.gz"]
				if any (read.endswith(ext) for ext in pe_allowed_extn):
					
					sample_name=read.split("_R1.f",1)[0]
					sample_list.append(sample_name)

					file_extn=read.split('.',1)[1]

			with open ((os.path.join(os.getcwd(),"config",'pe_samples.lst')),'w') as file:
				for sample in sample_list:
					file.write("%s\n" % sample)
			file.close()

			return file_extn


		def paired_end_group(pe_dir):
			if ((len(os.listdir(paired_end_read_dir))!=0)):
				pe_read_list=os.listdir(paired_end_read_dir)

				sample_list=[]
				for read in pe_read_list:
					pe_allowed_extn=["_R1.fq","_R1.fastq","_R1.fq.gz","_R1.fastq.gz"]
					if any (read.endswith(ext) for ext in pe_allowed_extn):
						
						sample_name=read.split("_R1.f",1)[0]
						sample_list.append(sample_name)

				print ('''
					Now you have to enter the biological conditions associated with each sample
				
					Example:
				
					samples         conditions
					sample_s1_rep1  control
					sample_s1_rep2  control
					sample_s2_rep1  treated
					sample_s2_rep2  treated

					Note: (1) condition name is case sensitive.
					  	  (2) each sample must have atleast one replicate

					 ''')
						
				dictionary=OrderedDict()
				for sample in range(len(sample_list)):
					condition=input('Enter Condition for %s: ' %(sample_list[sample]))
					dictionary.update({sample_list[sample]:condition})
					#print (dictionary)

					df = pd.DataFrame()
					df['lable'] = dictionary.keys()
					df['samples'] = dictionary.keys()
					df['conditions'] = dictionary.values()
					sampledf = df[["samples"]]
					groupdf = df[["samples","conditions"]]


				if (len(df.conditions.unique()) >2) or ((df.groupby('conditions').size().min()) == 1):
					print ('\nError in generating target file\n.................NOTE.................... \n1. Name of the condition is case sensitive. \n2. Each sample must have atleast one replicate.\n3. There must be two different conditions\n\nPlease Check The Values you entered\n')
					
					
				else:
					sampledf.to_csv("config" + "/" + "samples.txt", index=False, header=False)
					groupdf.to_csv("config" + "/" + "metagenome_group.tsv", sep='\t', index=False, header=False)
					groupdf.to_csv("config" + "/" + "metagenome_condition.tsv", sep='\t', index=False)
			
		paired_end_group(paired_end_read_dir)
		
		#################################################################################

		def pacbio_samples(pb_dir):
			raw_pb_read_list=os.listdir(pac_read_dir)
			sample_list=[]
			for read in raw_pb_read_list:
				raw_pb_allowed_extn=[".fq",".fastq",".fq.gz",".fastq.gz"]
				
				if any (read.endswith(ext) for ext in raw_pb_allowed_extn):
					
					sample_name=read.split(".",1)[0]
					sample_list.append(sample_name)

					file_extn=read.split('.',1)[1]


			with open ((os.path.join(os.getcwd(),"config",'pac_samples.lst')),'w') as file:
				for sample in sample_list:
					file.write("%s\n" % sample)
			file.close()

			return file_extn

		################################################################################
		def ont_samples(ont_raw_dir):
			corr_ont_read_list=os.listdir(ont_read_dir)
			sample_list=[]
			for read in corr_ont_read_list:
				corr_ont_allowed_extn=[".fq",".fastq",".fq.gz",".fastq.gz"]
				
				if any (read.endswith(ext) for ext in corr_ont_allowed_extn):
					
					sample_name=read.split(".",1)[0]
					sample_list.append(sample_name)
					file_extn=read.split('.',1)[1]


			with open ((os.path.join(os.getcwd(),"config",'ont_samples.lst')),'w') as file:
				for sample in sample_list:
					file.write("%s\n" % sample)
			file.close()

			return file_extn

		#Get Read Extension

		if ((len(os.listdir(paired_end_read_dir))!=0)):
			paired_end_read_suffix=paired_end_samples(paired_end_read_dir)


		if ((len(os.listdir(ont_read_dir))!=0)):
			ont_read_suffix=ont_samples(ont_read_dir)

		if ((len(os.listdir(pac_read_dir))!=0)):
			pac_read_suffix=pacbio_samples(pac_read_dir)


		with open('.luigi.cfg.tmp', 'w') as config:
			config.write('[core]\n')
			config.write('default-scheduler-port:{port}\n'.format(port=self.schedulerPort))
			config.write('error-email={email}\n\n'.format(email=self.userEmail))
			config.write('[GlobalParameter]\n')
			config.write('projectName={projectName}\n'.format(projectName=self.projectName))
			config.write('projectDir={projectDir}/\n'.format(projectDir=projectDir))
			config.write('domain={domain}\n'.format(domain=self.domain))
			config.write('adapter={adapter}\n'.format(adapter=adapter))
			

			#PE READ
			if ((len(os.listdir(paired_end_read_dir))!=0) and (len(os.listdir(ont_read_dir))==0) and (len(os.listdir(pac_read_dir))==0)):
				config.write('pe_read_dir={paired_end_read_dir}/\n'.format(paired_end_read_dir=paired_end_read_dir))
				config.write('pe_read_suffix={paired_end_read_suffix}\n'.format(paired_end_read_suffix=paired_end_read_suffix))
				config.write('seq_platforms=pe\n')
				config.write('pac_read_dir=NA\n')		
				config.write('pac_read_suffix=NA\n')
				config.write('ont_read_dir=NA\n')
				config.write('ont_read_suffix=NA\n')


			if ((len(os.listdir(ont_read_dir))!=0)  and (len(os.listdir(paired_end_read_dir))==0) and (len(os.listdir(pac_read_dir))==0)):
				config.write('ont_read_dir={ont_read_dir}/\n'.format(ont_read_dir=ont_read_dir))
				config.write('ont_read_suffix={ont_read_suffix}\n'.format(ont_read_suffix=ont_read_suffix))
				config.write('seq_platforms=ont\n')
				config.write('pac_read_dir=NA\n')		
				config.write('pac_read_suffix=NA\n')
				config.write('pe_read_dir=NA\n')
				config.write('pe_read_suffix=NA\n')



			if ((len(os.listdir(pac_read_dir))!=0) and (len(os.listdir(ont_read_dir))==0) and (len(os.listdir(paired_end_read_dir))==0)):
				config.write('pac_read_dir={pac_read_dir}/\n'.format(pac_read_dir=pac_read_dir))
				config.write('pac_read_suffix={pac_read_suffix}\n'.format(pac_read_suffix=pac_read_suffix))
				config.write('seq_platforms=pac\n')

				config.write('pe_read_dir=NA\n')		
				config.write('pe_read_suffix=NA\n')
				config.write('ont_read_dir=NA\n')
				config.write('ont_read_suffix=NA\n')
		

			if ((len(os.listdir(paired_end_read_dir))!=0) and (len(os.listdir(pac_read_dir))!=0) and (len(os.listdir(ont_read_dir))==0)):
				config.write('pe_read_dir={paired_end_read_dir}/\n'.format(paired_end_read_dir=paired_end_read_dir))
				config.write('pac_read_dir={pacbio_read_dir}/\n'.format(pacbio_read_dir=pacbio_read_dir))
				config.write('pe_read_suffix={paired_end_read_suffix}\n'.format(paired_end_read_suffix=paired_end_read_suffix))
				config.write('pac_read_suffix={pac_read_suffix}\n'.format(pac_read_suffix=pac_read_suffix))
				config.write('seq_platforms=pe-pac\n')

				config.write('ont_read_dir=NA\n')
				config.write('ont_read_suffix=NA\n')


			if ((len(os.listdir(paired_end_read_dir))!=0) and (len(os.listdir(ont_read_dir))!=0) and (len(os.listdir(pac_read_dir))==0)):
				config.write('pe_read_dir={paired_end_read_dir}/\n'.format(paired_end_read_dir=paired_end_read_dir))
				config.write('ont_read_dir={ont_read_dir}/\n'.format(ont_read_dir=ont_read_dir))
				config.write('pe_read_suffix={paired_end_read_suffix}\n'.format(paired_end_read_suffix=paired_end_read_suffix))
				config.write('ont_read_suffix={ont_read_suffix}\n'.format(ont_read_suffix=ont_read_suffix))
				config.write('seq_platforms=pe-ont\n')

				config.write('pac_read_dir=NA\n')		
				config.write('pac_read_suffix=NA\n')

		
			config.write('threads={cpus}\n'.format(cpus=self.cpus)) 
			config.write('maxMemory={memory}\n'.format(memory=self.maxMemory))
			config.close()

			print("the luigi config file generated")

			rename_config_cmd="mv .luigi.cfg.tmp luigi.cfg"
			print(run_cmd(rename_config_cmd))



if __name__ == "__main__":
		luigi.run()

