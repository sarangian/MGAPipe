# MGAPipe
Installation Instructions
================================
  
To install Metagenome Analysis Pipeline, you must have a minimum of 8 GiB free disk space and minimum of 16 GiB free RAM to test run. 

To provide an easier way to install, we provide a miniconda based installer.
Installation also requires **pre-instaled** ``git``, ``gcc``, ``cpp`` and ``zlib1g-dev``.
  
    git clone https://github.com/sarangian/MGAPipe.git
    cd MGAPipe
    chmod 755 INSTALL.sh
    ./INSTALL.sh

    
**Post Installation Instructions**
After successful installation, close the current terminal. 
In a new terminal. source the bashrc file:  ``source ~/.bashrc``

All the third party tools installed using conda are available at $HOME/MGAPipe/ [default location]
or the user specified location during the installation process.

The script to run Metagenome Analysis Pipeline is metagenome.py is available inside the MGAPipe folder, that you cloned from github.

Input Files
===========

Raw RNASeq Reads
----------------

  The raw ilumina reads in FASTQ format must be placed inside a folder with read permission
  Allowed ``extension`` for FASTQ reads: ``fq`` , ``fq.gz`` , ``fastq``, ``fastq.gz``
 
   For **paired-end** RNAseq reads, sample name must be suffixed with _R1. ``extension`` and _R2. ``extension`` for forward and reverse reads respectively

          *Example*

           sample1_R1.fastq 
           sample1_R2.fastq

           sample2_R1.fastq.gz 
           sample2_R2.fastq.gz

           sample3_R1.fq
           sample3_R2.fq

           sample4_R1.fq.gz 
           sample4_R2.fq.gz
          
   where  ``sample1`` , ``sample2`` , ``sample3`` , ``sample4`` are the sample names
           sample_name must be suffixed with _R1.{extension} and _R2.{extension}

   

.. _commands:

Commands
========

.. toctree::
   :hidden:



Commands to run Metagenome Analysis Workflow
--------------------------------------------
 

metagenome.py <command> - -help

.. code-block:: none

    Command                      Description   

    1. configureProject             Configure a Metagenome Analysis Project

    2. rawReadsQC                   Raw Reads Quality Assessment using FASTQC tool 

    3. cleanReads                   Process Raw Reads using BBDUK

    4. metagenomeAssembly           Assemble Metagenome using MegaHIT

    5. genomeBinning                Binning individual assembled genome using metabat2
    
    6. binRefinement                Refine Bins using refineM

    7. dRepBins                     de-replicate genome bins using dRep

    8. annotateGenomeBins           Annotation of de-replicated bins using PROKKA

    9. deaGenomeBins                Differential Enrichment Analysis of genome bins (2 different conditions) using enrichM



1. Prepare Project
-------------------

A. Single (or) multiple samples with out any conditions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: none
    
    STEP 1: copy the luigi.cfg template file to the present working directory

    luigi.cfg Template File
    ---------------------------------------------------------
    [core]
    default-scheduler-port:
    error-email=

    [GlobalParameter]
    projectName=
    projectDir=
    domain=
    adapter=
    pe_read_dir=
    pe_read_suffix=
    seq_platforms=
    pac_read_dir=
    pac_read_suffix=
    ont_read_dir=
    ont_read_suffix=
    threads=
    maxMemory=
    -----------------------------------------------------------

    STEP2: Fill the parameters as instructed

    NOTE:  1. User has to provide the path for the adapter file (tasks/utility/adapters.fasta.gz)
           2. For illumina paired end reads, seq_platforms must be ``pe``
                  Nanopore reads, seq_platforms must be ``ont`` 
                  PacBio reads, seq_platforms must be ``pac`` 
    
    Example: luigi.cfg
    ---------------------------------------------------------
    [core]
    default-scheduler-port:8082
    error-email=xxx@yyy.com

    [GlobalParameter]
    projectName=metagenome_demo_analysis
    projectDir=/home/sutripa/Documents/metagenome_demo_analysis/
    domain=prokaryote
    adapter=/home/sutripa/scriptome/tasks/utility/adapters.fasta.gz
    pe_read_dir=/home/sutripa/Documents/metagenome_symlink/pe/
    pe_read_suffix=fastq.gz
    seq_platforms=pe
    pac_read_dir=NA
    pac_read_suffix=NA
    ont_read_dir=NA
    ont_read_suffix=NA
    threads=15
    maxMemory=28
    ---------------------------------------------------------

    STEP 3: a. User has to create a project folder with exact name provided against ``projectName`` parameter
            b. User must create a folder in the name of ``config`` in the current working directory
            c. The config folder must contain a file in the name of ``pe_samples.lst`` containing the name of the samples

    Example of ``pe_samples.lst``

    ---------------
    SRS017713
    SRS019327
    SRS019221
    SRS013506
    ---------------




B. Single (or) Multiple Samples with conditions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To design and perform a Metagenome Analysis experiment, a Project need to be prepaired using the ``configureProject`` command.
The current working directory must contain a template of luigi.cfg file.

Conda environment must be activated before running the script.


Usage:  projectConfig.py -h


.. code-block:: none
    
    luigi.cfg Template File
    ---------------------------------------------------------
    [core]
    default-scheduler-port:
    error-email=

    [GlobalParameter]
    projectName=
    projectDir=
    domain
    adapter=
    pe_read_dir=
    pe_read_suffix=
    seq_platforms=
    pac_read_dir=
    pac_read_suffix=
    ont_read_dir=
    ont_read_suffix=
    threads=
    maxMemory=
    -----------------------------------------------------------

    metagenome.py configureProject --help <arguments>
    --help             Show this help message and exit

    mandatory arguments         Description
    
    --inputDir                  Path to Directory containing raw illumina paired-end reads
                                type: string
                                Example: /home/sutripa/Documents/scriptome_metaphlan/data



    --domain            Organism Domain
                                type: string
                                allowed values: [prokaryote, eukaryote]

    
    Optional arguments
    ------------------
    --projectName               Name of the Project Directory to be created
                                Must not contain blank spaces and (or) special characters
                                type: string
                                Default: metagenome_project

    --schedulerPort             Scheduler Port Number for luigi
                                type: int
                                Default: 8082

    --userEmail                 Provide your email address
                                type: string
                                Default: Null

    --cpus                      Number of threads to be used
                                type: int
                                Default = (total threads -1)

    --maxMemory                 Maximum allowed memory in GB. 
                                type: int
                                Default = [(available memory in GB) -1)
    

**Run Example**

.. code-block:: none
   
   mkdir MetagenomeAnalysis
   cd MetagenomeAnalysis
   Note: MetagenomeAnalysis folder must contain the luigi.cfg template file
   
   [MetagenomeAnalysis]$ metagenome.py configureProject \
                                      --domain prokaryote \
                                      --inputDir /home/sutripa/Documents/scriptome_metaphlan/data \
                                      --projectName metagenome_demo_analysis \
                                      --symLinkDir metagenome_symlink \
                                      --userEmail xxx@yyy.com \
                                      --local-scheduler
 
   Running the prepareProject.py script with the above parameters asks for the individual file types present inside the inputData folder.

   User has to choose  [pe:   paired end
                        ont:  nanopore
                        pac:  pacbio
                        ]


**Output**

|   Successful run of the projectConfig.py script with appropriate parameters will generate 
|
|   1. Luigi Configuration file ``luigi.cfg`` in the parent folder
|       
|      Edit the luigi.cfg file if required.
|      
|      Note:
|      It is mandatory to provide the path of the adapter file (default location: /tasks/utility/adapter.fastq.gz)
|
|   2. a project folder in the name of ``metagenome_demo_analysis`` will be generated
|
|   3. a configuration folder in the name of config containing 3 files   
|
|      a. metagenome_condition.tsv
|      b. metagenome_group.tsv
|      b. pe_samples.lst
|      c. samples.txt
|   4. A folder named ``metagenome_symlink`` (provided as parameter to --symLinkDir) will be created which contains the symbolic links to the read files present in the inputData (/home/sutripa/Documents/scriptome_metaphlan/data) folder
|
|   The ``metagenome_condition.tsv`` file contains the sample names with their associated environmental conditions, which will be used for condition based genome binning and differential enrichment analysis. Kindly check the generated files and modify if required
|  
|



2. Raw reads quality assessment
--------------------------------

|  **Note**
|    Before running any of the Metagenome Analysis Workflow commands, a project must be configured using ``configureProject`` command.
|    The parent forlder must have the luigi.cfg file, in which the globalparameters are defined.
|    Running any of the  Metagenome Analysis Workflow commands without generating the project folder and luigi.cfg file will give rise to ``luigi.parameter.MissingParameterException``
|

.. code-block:: none

    **Steps**
    1. Run Prepare Projcet with project name ``metagenome_demo_analysis`` as discussed before 
       and inspect the files generated inside config folder

    2. Run rawReadsQC
       [MetagenomeAnalysis]$ metagenome.py rawReadsQC --local-scheduler

      Successful execution of rawReadsQC will generate a folder $PWD/metagenome_demo_analysis/ReadQC/PreQC
      which contains the FASTQC reports of the raw paired-end fastq files




3. Raw samples quality control
------------------------------
Quality control analysis of the raw samples can be done using command ``preProcessSamples``
|
|  **Requirements**
|  1. Execution of prepareProject.py command 
|  2. Availability of ``luigi.cfg`` file in ``parent folder`` and ``pe_samples.lst`` inside the ``config``.
|

.. code-block:: none                                      

   [MetagenomeAnalysis]$ metagenome.py cleanReads <arguments> --local-scheduler
    
    arguments               type      Description
      

     --cleanFastq-kmer-length        int   Kmer length used for finding contaminants
                                      Examle: 13  
                                      Default: 11 

    --cleanFastq-k-trim              str   Trimming protocol to remove bases matching reference
                                      kmers from reads. Choose From['f: dont trim','r: trim to right','l: trim to left] 
                                      Choices: {f, r, l} 

    --cleanFastq-quality-trim       int   Trim read ends to remove bases with quality below trimq.
                                      Performed AFTER looking for kmers.  Values: 
                                          rl  (trim both ends), 
                                          f   (neither end), 
                                          r   (right end only), 
                                          l   (left end only),
                                          w   (sliding window).

                                          Default: f


    --cleanFastq-trim-quality        float  Regions with average quality BELOW this will be trimmed,
                                     if qtrim is set to something other than f.  Can be a 
                                     floating-point number like 7.3  
                                     Default: 6                 

    --cleanFastq-min-length        int   Minimum read length after trimming
                                      Example: 50
                                      Default:50

    --cleanFastq-trim-front          int   Number of bases to be trimmed from the front of the read
                                      Example: 5
                                      Default: 0

    --cleanFastq-trim-tail           int   Number of bases to be trimmed from the end of the read
                                      Example: 5
                                      Default: 0

    --cleanFastq-min-average-quality int   Minimum average quality of reads.
                                      Reads with average quality (after trimming) below 
                                      this will be discarded
                                      Example: 15
                                      Default: 10

    --cleanFastq-mingc              float Minimum GC content threshold
                                      Discard reads with GC content below minGC
                                      Example: 0.1 
                                      Default: 0.0

    --cleanFastq-maxgc              float Maximum GC content  threshold
                                      Discard reads with GC content below minGC
                                      Example: 0.99 
                                      Default: 1.0
    --local-scheduler


**Example Run**

.. code-block:: none

   [MetagenomeAnalysis]$ metagenome.py cleanReads \
                            --cleanFastq-min-average-quality 15 \
                            --cleanFastq-mingc 0.20 \
                            --cleanFastq-maxgc 0.70 \
                            --cleanFastq-quality-trim w  \
                            --local-scheduler

      **Output**
      /path/to/ProjectFolder/metagenome_demo_analysis/ReadQC/CleanedReads/PE-Reads --contains the processed FastQ-reads

A. Genome Resolved Metagenimics
-------------------------------------

1. Metagenome Assembly using megaHIT
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


Assembly of individual samples can be done using command ``metagenomeAssembly``

|
|  **Requirements**
|  1. Pre execution of ``configureProject`` command 
|  2. Availability of ``luigi.cfg`` file in ``parent folder`` and ``pe_samples.lst`` inside the ``config`` folder.
|  
|  

.. code-block:: none   

    [MetagenomeAnalysis]$ metagenome.py  <arguments> --local-scheduler

    argument               type      Description

    --pre-process-reads    str       Run Quality Control Analysis of the raw illumina reads required or not
                                     [yes / no]

                                     If yes, cleanReads command will be run with default parameters.
                                     If no, quality control analysis will not be done, instead re-pair.sh or reformat.sh 
                                     script of bbmap will be run based on paired-end or single-end reads.

   --local-scheduler




|  **Example Run 1**
|  **Metagenome assembly with read quality control analysis** 
|
|   [MetagenomeAnalysis]$ metagenome.py  metagenomeAssembly  \
|                                       --pre-process-reads  ``yes``                                    
|                                       --local-scheduler
|


|  **Example Run 2**
|  **Metagenome assembly with out read quality control analysis** 
|
|
|  [MetagenomeAnalysis]$ metagenome.py  metagenomeAssembly  \
|                                       --pre-process-reads  ``yes``                                    
|                                       --local-scheduler
|
|  Note: Output folder in the name of MGAssembly will be created at $PWD/metagenome_demo_analysis/MGAssembly
|  MGAsembly folder contains the sub-folders by the name of the samples and each sub-folder contains the resultant assembly
|

2. Binning of assembled contigs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Binning of individual assembled samples can be done using command ``genomeBinning``

|
|  **Requirements**
|  1. Pre execution of ``configureProject`` command 
|  2. Availability of ``luigi.cfg`` file in ``parent folder`` and ``pe_samples.lst`` inside the ``config`` folder.
|  
|  

.. code-block:: none   

    [MetagenomeAnalysis]$ metagenome.py  genomeBinning <arguments> --local-scheduler

    argument               type      Description

    --pre-process-reads    str       Run Quality Control Analysis of the ilumina reads required or Not
                                     [yes / no]

                                     If yes, cleanReads command will be run with default parameters.
                                     If no, quality control analysis will not be done, instead re-pair.sh or reformat.sh 
                                     script of bbmap will be run based on paired-end or single-end reads.

   --local-scheduler


|  **Example Run 2**
|  **Metagenome assembly with read quality control analysis followed by genome binning** 
|
|
|  [MetagenomeAnalysis]$ metagenome.py  genomeBinning  \
|                                       --pre-process-reads  ``yes``                                    
|                                       --local-scheduler
|
|  Note: Output folder in the name of binning will be created at ``$PWD/metagenome_demo_analysis/binning``
|  ``binning`` folder contains the sub-folders by the name of the samples and each sub-folder contains the resultant genome bins
|


3. Refinement of genome bins
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Refinement of individual genome bins can be done using command ``binRefinement``

|
|  **Requirements**
|  1. Pre execution of ``configureProject`` command 
|  2. Availability of ``luigi.cfg`` file in ``parent folder`` and ``pe_samples.lst`` inside the ``config`` folder.
|  
|  

.. code-block:: none   

    [MetagenomeAnalysis]$ metagenome.py  binRefinement <arguments> --local-scheduler

    argument               type      Description

    --pre-process-reads    str       Run Quality Control Analysis of the ilumina reads required or Not
                                     [yes / no]

                                     If yes, cleanReads command will be run with default parameters.
                                     If no, quality control analysis will not be done, instead re-pair.sh or reformat.sh 
                                     script of bbmap will be run based on paired-end or single-end reads.

   --local-scheduler


|  **Example Run**
|  **Metagenome assembly with read quality control analysis followed by genome binning and bin refinement** 
|
|
|  [MetagenomeAnalysis]$ metagenome.py  binRefinement  \
|                                       --pre-process-reads  ``yes``                                    
|                                       --local-scheduler
|
|  Note: Output folder in the name of binning will be created at ``$PWD/metagenome_demo_analysis/bin_refinement``
|  ``bin_refinement`` folder contains the sub-folders by the name of the samples and each sub-folder contains the resultant refined genome bins. refineM tool is used for refinement of genome bins
|


4. de-replication of genome bins
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Condition-based or condition-free de-replication of individual genome bins can be done using command ``dRepBins``

|
|  **Requirements**
|  1. Pre execution of ``configureProject`` command 
|  2. Availability of ``luigi.cfg`` file in ``parent folder`` and ``pe_samples.lst`` inside the ``config`` folder.
|  3. Availability of ``metagenome_group.tsv`` file inside the ``config`` folder if user opts for condition_based de-replication
|  

.. code-block:: none   

    [MetagenomeAnalysis]$ metagenome.py  dRepBins <arguments> --local-scheduler

    argument               type      Description

    --pre-process-reads    str       Run Quality Control Analysis of the ilumina reads required or Not
                                     [yes / no]

                                     If yes, cleanReads command will be run with default parameters.
                                     If no, quality control analysis will not be done, instead re-pair.sh or reformat.sh 
                                     script of bbmap will be run based on paired-end or single-end reads.

    --checkM-method        str       choice of workflow to run checkm. choice [lineage_wf, taxonomy_wf]
                                     default: taxonomy_wf

    --dRep-method          str       Mandatory Parameter. choice of de-replication method. choice [condition_based, condition_free]
                    
    --contamination        int       accepted percentage of contamination [default:25] 

    --completeness         int       accepted percentage of genome completeness [default:75]

    --min-genome-length    int       minimum accepted genome length [default: 50000] 

    --local-scheduler


|  
|  **4.a. Metagenome assembly with read quality control analysis followed by genome binning, bin refinement and condition_free de-replication** 
|
|  [MetagenomeAnalysis]$ metagenome.py  dRepBins  \
|                                       --pre-process-reads  ``yes``   \
|                                       --dRep-method ``condition_free`` \
|                                       --checkM-method ``taxonomy_wf``  \
|                                       --contamination ``10`` \
|                                       --completeness ``80`` \
|                                       --local-scheduler
|
|  Note: 1. Output folder in the name of ``dReplicated_bins_condition_free will`` be created at ``$PWD/metagenome_demo_analysis/dRep_bins/dReplicated_bins_condition_free/dereplicated_genomes`` which contains the de-replicated genomes
| 
 
|  
|  **4.b. Metagenome assembly with read quality control analysis followed by genome binning, bin refinement and condition_based de-replication** 
|
|  [MetagenomeAnalysis]$ metagenome.py  dRepBins  \
|                                       --pre-process-reads  ``yes``   \
|                                       --dRep-method ``condition_based`` \
|                                       --reference-condition ``buccal_mucosa`` \
|                                       --contrast-condition  ``tongue_dorsum`` \
|                                       --checkM-method ``taxonomy_wf``  \
|                                       --contamination ``10`` \
|                                       --completeness ``80`` \
|                                       --local-scheduler
|
|  Note: 1. Output folder in the name of ``dReplicated_bins_condition_free will`` be created at ``$PWD/metagenome_demo_analysis/dRep_bins/dReplicated_bins_condition_based/`` which contains the de-replicated genomes
| 



5. annotate de-replicated genome bins
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Annotation of condition-based or condition-free de-replicated genome bins can be done using command ``annotateGenomeBins``

|
|  **Requirements**
|  1. ``annotateGenomeBins`` must be run after condition_based or condition_free de-replication of genome bins 
|  1. Pre execution of ``configureProject`` command 
|  2. Availability of ``luigi.cfg`` file in ``parent folder`` and ``pe_samples.lst`` inside the ``config`` folder.
|  3. If user opted for condition_based de-replication, availability of ``genomes_dRep_by_condition.lst`` file containing the list of genome bins, inside the ``$PWD/metagenome_demo_analysis/dRep_bins`` folder
|  4. If user opted for condition_based de-replication, availability of ``genomes_dRep_regardless_condition.lst`` file containing the list of genome bins inside the ``$PWD/metagenome_demo_analysis/dRep_bins`` folder
|

.. code-block:: none   

    [MetagenomeAnalysis]$ metagenome.py  dRepBins <arguments> --local-scheduler

    argument               type      Description

    --pre-process-reads    str       Run Quality Control Analysis of the ilumina reads required or Not
                                     [yes / no]

                                     If yes, cleanReads command will be run with default parameters.
                                     If no, quality control analysis will not be done, instead re-pair.sh or reformat.sh 
                                     script of bbmap will be run based on paired-end or single-end reads.

    --min-contig-length    int       minimum contig length [default 200]

    --dRep-method          str       Mandatory Parameter. choice of de-replication method. choice [condition_based, condition_free]
                    
    --local-scheduler


|  **Example Run**
|  **Metagenome annotation after condition_free de-replication of genome bins** 
|
|
|  [MetagenomeAnalysis]$ metagenome.py  annotateGenomeBins  \
|                                       --pre-process-reads  ``yes``   \
|                                       --dRep-method condition_free \                                 
|                                       --local-scheduler
|
|  Note: 1. Output folder in the name of ``prokka_condition_free`` will be created at ``$PWD/metagenome_demo_analysis/prokka_condition_free``
|

|  **Example Run**
|  **Metagenome annotation after condition_based de-replication of genome bins** 
|
|
|  [MetagenomeAnalysis]$ metagenome.py  annotateGenomeBins  \
|                                       --pre-process-reads  ``yes``   \
|                                       --dRep-method condition_based \                                 
|                                       --local-scheduler
|
|  Note: 1. Output folder in the name of ``prokka_condition_based`` will be created at ``$PWD/metagenome_demo_analysis/prokka_condition_based``
|
|      



6. Differential Enrichment Analysis of de-replicated genome bins
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Differential Enrichment Analysis of de-replicated genome bins can be done using command ``deaGenomeBins``

|
|  **Requirements**
|  1. Pre execution of ``configureProject`` command 
|  2. Availability of ``luigi.cfg`` file in ``parent folder`` and ``pe_samples.lst`` inside the ``config`` folder.
|  3. Availability of ``metagenome_group.tsv`` file inside the ``config`` folder 
|

.. code-block:: none   

    [MetagenomeAnalysis]$ metagenome.py  deaGenomeBins <arguments> --local-scheduler

    argument               type      Description

    --pre-process-reads    str       Run Quality Control Analysis of the ilumina reads required or Not
                                     [yes / no]

                                     If yes, cleanReads command will be run with default parameters.
                                     If no, quality control analysis will not be done, instead re-pair.sh or reformat.sh 
                                     script of bbmap will be run based on paired-end or single-end reads.

    --checkM-method        str       choice of workflow to run checkm. choice [lineage_wf, taxonomy_wf]
                                     default: taxonomy_wf

    --contamination        int       accepted percentage of contamination [default:25] 

    --completeness         int       accepted percentage of genome completeness [default:75]

    --min-genome-length    int       minimum accepted genome length [default: 50000] 

    --reference-condition  str       Mandatory Parameter: reference condition name

    --contrast-condition   str       Mandatory Parameter: contrast condition name

    --pvalcutoff           float     Default: 0.05

    --correction           str       correction method. Choose from [Bonferroni: b, 
                                                                     FDR 2-stage Benjamini-Hochberg: fdr_tsbh
                                                                     Holm: h
                                                                     Hommel: ho  
                                                                     Holm-Sidak: hs 
                                                                     Simes-Hochberg: sh
                                                                     FDR Benjamini-Yekutieli: fdr_by
                                                                     FDR 2-stage Benjamini-Krieger-Yekutieli: fdr_tsbky]
    --local-scheduler




.. code-block:: none  

    [RNASeq-Analysis]$ rnaseq.py alignmentFreeDEA <arguments> --local-scheduler

    argument               type      Description
    
   [required arguments]  

    --pre-process-reads     str       Run Quality Control Analysis of the RNASeq reads or Not
                                      [yes / no]

                                      If yes, cleanReads command will be run with default parameters.
                                      If no, quality control analysis will not be done, instead re-pair.sh or reformat.sh 
                                      script of bbmap will be run based on paired-end or single-end reads.

   --quant-method           str       Read quantification method
                                      [salmon / kallisto]

   --dea-method             str       Method to be used for differential expression analysis. 
                                      [deseq2 / edger]

   --reference-condition    str       Reference biological condition. 
                                      example: control

   --local-scheduler

    [optional arguments] 

   --attribute-type        str       Atrribute type in GTF annotation
                                     choose from {gene_id, transcript_id}

   --strand-type           int       perform strand-specific read counting.
                                     choose from [0: unstranded,
                                                  1: stranded,
                                                  2: reversely-stranded]

   --report-name           str       Name of the differential expression analysis report
                                     Default: DEA_Report

   --factor-of-intrest     str       Factor of intrest column of the target file
                                     Default: conditions

   --fit-type              str       mean-variance relationship. 
                                     Choices: {local, parametric, mean}

   --size-factor           str       method to estimate the size factors. Choices: {shorth, median} 

   --result-tag            str       Tag need to be apended to result file
                                     Default: treated_vs_control
    
    

B. Marker Gene based Taxonomy Profiling
----------------------------------------

1. Taxonomy profiling from metagenomic shotgun sequencing data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


Taxonomy profiling of individual samples can be done using command ``compositionProfiling``

|
|  **Requirements**
|  1. Pre execution of ``configureProject`` command 
|  2. Availability of ``luigi.cfg`` file in ``parent folder`` and ``pe_samples.lst`` inside the ``config`` folder.
|  
|  

.. code-block:: none   

    [MetagenomeAnalysis]$ metagenome.py  compositionProfiling <arguments> --local-scheduler

    argument               type      Description

    --pre-process-reads    str       Run Quality Control Analysis of the raw illumina reads required or not
                                     [yes / no]

                                     If yes, cleanReads command will be run with default parameters.
                                     If no, quality control analysis will not be done, instead re-pair.sh or reformat.sh 
                                     script of bbmap will be run based on paired-end or single-end reads.

   --local-scheduler



|  **Example**
|  **Composition profiling with read quality control analysis** 
|
|   [MetagenomeAnalysis]$ metagenome.py  compositionProfiling  \
|                                       --pre-process-reads  ``yes``                                    
|                                       --local-scheduler
|
|
|  Note: 1. Output folder in the name of ``profiled_samples`` will be created at ``$PWD/metagenome_demo_analysis/``, which contains the taxonomy profile of individual samples
|  
|      

2. Generate plots using hclust, graphlan and ampvis
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
|
|  **Requirements**
|  1. Pre execution of ``configureProject`` command 
|  2. Availability of ``luigi.cfg`` file in ``parent folder``,``metagenome_group.tsv`` and ``pe_samples.lst`` files inside the ``config`` folder.
|  
|  

.. code-block:: none   

    [MetagenomeAnalysis]$ metagenome.py  compositionProfiling <arguments> --local-scheduler

    argument               type      Description

    [mandatory]
    --pre-process-reads    str       Run Quality Control Analysis of the raw illumina reads required or not
                                     [yes / no]

                                     If yes, cleanReads command will be run with default parameters.
                                     If no, quality control analysis will not be done, instead re-pair.sh or reformat.sh 
                                     script of bbmap will be run based on paired-end or single-end reads.
    
    [optional]

    --condition-column     str       Name of the condition column in metagenome_group.tsv file. [Default: conditions]
    --local-scheduler



|  **Example**
|  **Composition profiling with read quality control analysis** 
|
|   [MetagenomeAnalysis]$ metagenome.py  profileTaxonomy  \
|                                       --pre-process-reads  ``yes``  
|                                       --condition-column ``conditions`` \                                  
|                                       --local-scheduler
|
|
|  Note: 1. Output folder in the name of ``figures`` will be created at ``$PWD/metagenome_demo_analysis/``, which contains the images
|  
|      

