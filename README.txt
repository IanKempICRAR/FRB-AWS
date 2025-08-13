README.txt - Ian Kemp - International Centre for Radio Astronomy Research - 12 August 2025 =====================

Guide to setting up a radio-astronomer friendly VM to get started at AWS =======================================
2025_AWS.pdf             The first part is specific to Curtin Uni users


List of observations processed in late 2025 referred to in PhD thesis Chapter 5 ================================
observations.csv


Processing MWA observations to search for FRB ==================================================================

Pre-requisites: Install these first on your VM

giant-squid               Tool to interact with MWA data repository
CASA6                     Containerised CASA tools (see the above get started guide)
WSClean 3.5               Containerised WSClean (see the above get started guide)

Mount the data drive at /home/ec2-user/data and create the following structure:

/data/                    Working folders for observation visibility files will appear here 
/data/results             Outputs from processing will all appear in this folder and can be scp'd from here
/scheduler                Data request and job control scripts (set up a crontab to start these when required)
    register.csv.         Master register of obs and their status. (Make sub-registers for each VM)
    request.py            Script to request a conversion job from MWA ASVO
    request.sh            Controller for the above
    u-and-p.py            Script to start the process observations provisioned by MWA ASVO
    u-and-p.sh            Controller for the above
/scripts-hires            Processing scripts
    FRB11-1.py            Main processing script
    FRB11-get-metadata.py Script of CASA6 calls to extract obs metadata
    EOR_scint_mask_01.csv Example scintillation mask (for 1024x1024 EOR0 images)

Save the VM as an Amazon Machine Image (AMI), then you can spin up as many clones as you want

To run all the above interactively, type:
nohup /home/ec2-user/scheduler/request.sh
nohup /home/ec2-user/scheduler/u-and-p.sh

Some handy scripts to run on your home system after you have downloaded all the results files

image_count.py            Counts images and dumps the 7 sigma detections to the console 
snapshot_count.py.        Counts snapshots (used to calc the total obs hrs searched)

To run the above interactively, type:
cat *detections.csv >all_detections.csv
python3 snapshot_count.py
python3 image_count.py >all7sig_detections.csv