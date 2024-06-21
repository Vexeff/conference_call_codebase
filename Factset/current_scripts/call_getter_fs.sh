#!/bin/bash

#---------------------------------------------------------------------------------
# Account information

#SBATCH --account=            # basic (default), staff, phd, faculty

#---------------------------------------------------------------------------------
# Resources requested

#SBATCH --partition=standard     # standard (default), long, gpu, mpi, highmem
#SBATCH --cpus-per-task=1        # number of CPUs requested (for parallel tasks)
#SBATCH --mem=32G         # requested memory
#SBATCH --time=2-00:00:00       # wall clock limit (d-hh:mm:ss)
#SBATCH --output=call_getter_fs.log
#---------------------------------------------------------------------------------
# Job specific name (helps organize and track progress of jobs)

#SBATCH --job-name=fs_transcripts # user-defined job name

#---------------------------------------------------------------------------------
# Print some useful variables

echo "Job ID: $SLURM_JOB_ID"
echo "Job User: $SLURM_JOB_USER"
echo "Num Cores: $SLURM_JOB_CPUS_PER_NODE"

#---------------------------------------------------------------------------------
# Load necessary modules for the job

module load python/booth/3.10

#activating venv that includes needed libraries
source ~/venv/refpull_pckgs/bin/activate

#---------------------------------------------------------------------------------
# Commands to execute below...

srun python3 call_getter_fs.py