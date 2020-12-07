import luigi
import os
import subprocess
import time

from tasks.metagenome.readCleaning.preProcessReads import cleanFastq
from tasks.metagenome.readCleaning.preProcessReads import filtlong
from tasks.metagenome.readCleaning.reFormatReads import reformat

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

class singleSampleAssembly(luigi.Task):
   
    project_name=GlobalParameter().projectName
    #assembly_type=luigi.ChoiceParameter(default="single",choices=["single", "co"], var_type=str)
    adapter = GlobalParameter().adapter
    threads = GlobalParameter().threads
    max_memory = GlobalParameter().maxMemory
    pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
    read_library_type = GlobalParameter().seq_platforms
    min_contig_length=luigi.IntParameter(default="1500",description="Minimum Contig lenghth. (int[Default: 1500])")
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

        #Paired-end with Pacbio
        if self.read_library_type == "pe-pac" and self.pre_process_reads == "yes":
            return [
                        [cleanFastq(sampleName=i)
                            for i in [line.strip() for line in
                                open((os.path.join(os.getcwd(), "config", "pe_samples.lst")))]],

                        [filtlong(sampleName=i)
                            for i in [line.strip() for line in
                                open((os.path.join(os.getcwd(), "config", "pac_samples.lst")))]]
                    ]


        if self.read_library_type == "pe-pac" and self.pre_process_reads == "no":
            return [
                        [reformat(sampleName=i)
                            for i in [line.strip() for line in
                                open((os.path.join(os.getcwd(), "config", "pe_samples.lst")))]],

                        [reformat(sampleName=i)
                            for i in [line.strip()for line in
                                open((os.path.join(os.getcwd(), "config", "pac_samples.lst")))]]
                    ]

       
    def output(self):
        assembled_metagenome_folder = os.path.join(os.getcwd(), self.project_name, "MGAssembly" )
        return {'out': luigi.LocalTarget(assembled_metagenome_folder + "/" + self.sampleName + "/" + self.sampleName+".contigs.fa")}
        
    def run(self):

        assembled_metagenome_folder = os.path.join(os.getcwd(), self.project_name, "MGAssembly" )
            
        if self.read_library_type=="pe":
            pe_sample_list = os.path.join(os.getcwd(), "congig", "pe_samples.lst")
        if self.read_library_type=="ont":
            ont_sample_list = os.path.join(os.getcwd(), "config", "ont_samples.lst")

        if self.read_library_type=="pac":
            pac_sample_list = os.path.join(os.getcwd(), "config", "pac_samples.lst")

        if self.read_library_type=="pe-pac":
            pac_sample_list = os.path.join(os.getcwd(), "config", "pac_samples.lst")
            pe_sample_list = os.path.join(os.getcwd(), "congig", "pe_samples.lst")

        if self.read_library_type=="pe-ont":
            pe_sample_list = os.path.join(os.getcwd(), "congig", "pe_samples.lst")
            ont_sample_list = os.path.join(os.getcwd(), "config", "ont_samples.lst")


        if self.pre_process_reads=="no":
            pe_read_folder = os.path.join(os.getcwd(), self.project_name, "ReadQC", "VerifiedReads", "PE-Reads" + "/")
            ont_read_folder = os.path.join(os.getcwd(),self.project_name, "ReadQC", "VerifiedReads", "ONT-Reads" + "/")
            pac_read_folder = os.path.join(os.getcwd(), self.project_name, "ReadQC", "VerifiedReads", "PAC-Reads" + "/")

        if self.pre_process_reads=="yes":
            pe_read_folder = os.path.join(os.getcwd(), self.project_name, "ReadQC", "CleanedReads", "PE-Reads" + "/")
            ont_read_folder = os.path.join(os.getcwd(), self.project_name, "ReadQC", "CleanedReads", "ONT-Reads" + "/")
            pac_read_folder = os.path.join(os.getcwd(), self.project_name, "ReadQC", "CleanedReads", "PAC-Reads" + "/")

        

        cmd_run_assembler = "[ -d  {assembled_metagenome_folder} ] || mkdir -p {assembled_metagenome_folder}; cd {assembled_metagenome_folder}; " \
                             "megahit " \
                             "-1 {pe_read_folder}{sample}_1.fastq " \
                             "-2 {pe_read_folder}{sample}_2.fastq " \
                             "-m {max_memory} " \
                             "-t {threads} " \
                             "--out-prefix {sample} " \
                             "-o {output_dir} " \
            .format(assembled_metagenome_folder=assembled_metagenome_folder,
                    sample=self.sampleName,
                    pe_read_folder=pe_read_folder,
                    output_dir=self.sampleName,
                    max_memory=self.max_memory,
                    threads=self.threads)

        print("****** NOW RUNNING COMMAND ******: " + cmd_run_assembler)
        print (run_cmd(cmd_run_assembler))



class metagenomeAssembly(luigi.Task):
    project_name=GlobalParameter().projectName
    adapter = GlobalParameter().adapter
    threads = GlobalParameter().threads
    max_memory = GlobalParameter().maxMemory
    pre_process_reads = luigi.ChoiceParameter(choices=["yes", "no"], var_type=str)
    read_library_type = GlobalParameter().seq_platforms
    min_contig_length=luigi.IntParameter(default="1500",description="Minimum Contig lenghth. (int[Default: 1500])")

    def requires(self):

        return [singleSampleAssembly(pre_process_reads=self.pre_process_reads,
                                     min_contig_length=self.min_contig_length,
                                    sampleName=i)
                        for i in [line.strip()
                            for line in
                                open((os.path.join(os.getcwd(), "config", "pe_samples.lst")))]]


    def output(self):
        timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
        return luigi.LocalTarget(os.path.join(os.getcwd(),self.project_name,"task_logs",'task.metagenome.assembly.complete.{t}'.format(
            t=timestamp)))

    def run(self):
        timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
        with self.output().open('w') as outfile:
            outfile.write('Metagenome Assembly finished at {t}'.format(t=timestamp))
