
.. _inputfiles:

Input Files
===========

Raw Illumina Paired-end Reads
-----------------------------

|   The raw-RNASeq reads in FASTQ format must be placed inside a folder with read permission
|   Allowed ``extension`` for FASTQ reads: ``fq`` , ``fq.gz`` , ``fastq``, ``fastq.gz``
| 
|   For **paired-end** RNAseq reads, sample name must be suffixed with _R1. ``extension`` and _R2. ``extension`` for forward and reverse reads respectively
|
|          *Example*
|
|           sample1_R1.fastq 
|           sample1_R2.fastq
|
|           sample2_R1.fastq.gz 
|           sample2_R2.fastq.gz
|
|           sample3_R1.fq
|           sample3_R2.fq
|
|           sample4_R1.fq.gz 
|           sample4_R2.fq.gz
|          
|   where  ``sample1`` , ``sample2`` , ``sample3`` , ``sample4`` are the sample names
|           sample_name must be suffixed with _R1.{extension} and _R2.{extension}
|
|  