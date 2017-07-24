#!/usr/bin/env python

import os
import yaml
import csv
import json
import sys
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
import glob
import subprocess
import job
import audit
import logging
import utils


def main(argv=None):

    parser = ArgumentParser(description="EGA-file-to-colllab job generator and auditor",
             formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-c", "--setting", dest="conf",
             help="Specify ega setting file", required=False)
    parser.add_argument("-t", "--task", dest="task",
             help="Specify the task", required=True)
    parser.add_argument("-p", "--project", dest="project", nargs="*",
             help="Specify the project", required=False)
    parser.add_argument("-s", "--seq_strategy", dest="seq_strategy", nargs="*",
             help="Specify the sequencing strategy", required=False)

    args = parser.parse_args()
    conf = args.conf if args.conf else 'conf/conf.yaml'
    task = args.task
    project = args.project
    project = list(project) if project else []
    seq_strategy = args.seq_strategy
    seq_strategy = list(seq_strategy) if seq_strategy else []
    
    if not os.path.isfile(conf):
        logger.error('Configuration file does not exist!') 
        raise IOError('Configuration file does not exist!')

    with open(conf, 'r') as c:
        conf_dict = yaml.load(c)

    logger = logging.getLogger(__name__)
    utils.setup_logging()

    # add annotations
    annotations = {}
    # download the dbox_content from aspera
    try:
        subprocess.check_output(['ascp', '-QTl','100m','--ignore-host-key','--mode=recv','--host='+os.environ['ASCP_EGA_HOST'],'--user='+os.environ['ASCP_EGA_USER'], 'dbox_content', '.'])
        logger.info('Download dbox_content from aspera server!')
    except Exception, err:
        logger.error(str(err))
        sys.exit(1)
    # add staged ega_file_id
    with open('dbox_content', 'r') as f:
        for l in f:
            if l.endswith('md5'): continue
            fid = l.split('/')[-1].split('.')[0]
            if not annotations.get('staged'): annotations['staged'] = set()
            annotations['staged'].add(fid)

    # update the ega_xml audit repo
    try:
        origWD = os.getcwd()
        os.chdir(os.path.join(conf_dict.get('ega_audit_base_path'), conf_dict.get('ega_audit').get('file_path')))
        subprocess.check_output("git checkout master", shell=True)
        subprocess.check_output("git pull", shell=True)
        os.chdir(origWD)
        logger.info('Update the ega_xml audit repo')
    except Exception, err:
        logger.error(str(err))
        sys.exit(1)    


    # get the fid which are to be staged by EGA
    to_stage_file_pattern = os.path.join(conf_dict.get('ega_audit_base_path'), conf_dict.get('ega_operation').get('file_path'), '*-*', 'to_stage_*.tsv')
    files = glob.glob(to_stage_file_pattern)
    for fname in files:
        with open(fname, 'r') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for l in reader:
                 if not annotations.get('to_stage'): annotations['to_stage'] = set()
                 annotations['to_stage'].add(l.get('ega_file_id'))   

    # git pull the job repo
    repo_list = glob.glob(os.path.join(conf_dict.get('ega_job_base_path'), conf_dict.get('ega_job').get('job_repo')))
    for repo in repo_list:
        try:
            origWD = os.getcwd()
            os.chdir(repo)
            subprocess.check_output("git checkout master", shell=True)
            subprocess.check_output("git pull", shell=True)
            os.chdir(origWD)
            logger.info('Update the job repos')
        except Exception, err:
            logger.error(str(err))
            sys.exit(1)

        # get the file list for transferring completed
        job_pattern = os.path.join(repo, conf_dict.get('ega_job').get('job_completed'))
        files = glob.glob(job_pattern)
        for fname in files:
            with open(fname, 'r') as f:
                job_dict = json.loads(f.read())
                for ega_file in job_dict.get('files'):
                    fid = ega_file.get('ega_file_id')
                    if not annotations.get('completed'): annotations['completed'] = set()
                    annotations['completed'].add(fid)

        # get the file list for transferring job already queued
        for s in ['completed', 'running', 'failed', 'queued']:
            job_pattern = os.path.join(repo, conf_dict.get('ega_job').get('job_'+s))
            files = glob.glob(job_pattern)
            for fname in files:
                with open(fname, 'r') as f:
                    job_dict = json.loads(f.read())
                    for ega_file in job_dict.get('files'):
                        fid = ega_file.get('ega_file_id')
                        if not annotations.get('generated'): annotations['generated'] = set()
                        annotations['generated'].add(fid)

    if task == 'stage':
        logger.info('Generate list of files to be staged to server!')
        audit.generate_files_to_stage(conf_dict, annotations, project, seq_strategy)
    elif task == 'remove':
        logger.info('Generate list of files to be removed from server!')
        audit.generate_files_to_remove(conf_dict, annotations)
    elif task == 'job':
        logger.info('Generate jobs!')
        job.generate(conf_dict, annotations, project, seq_strategy)
    else:
        pass


if __name__ == "__main__":
    main()
    
