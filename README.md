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

   
