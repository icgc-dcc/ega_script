#!/usr/bin/env python

import os
import yaml
import csv
import json
import sys
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
import glob
from collections import OrderedDict
import subprocess
import shutil
import requests
import logging
import re

id_service_token = os.environ.get('ICGC_TOKEN')
logger = logging.getLogger('Job_generator')

def main(argv=None):

    parser = ArgumentParser(description="EGA transfer",
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
    conf = args.conf if args.conf else 'conf.yaml'
    task = args.task
    project = args.project
    project = list(project) if project else []
    seq_strategy = args.seq_strategy
    seq_strategy = list(seq_strategy) if seq_strategy else []
    
    if not os.path.isfile(conf): raise IOError('Configuration file does not exist!')

    with open(conf, 'r') as c:
        conf_dict = yaml.load(c)

    # add logger
    ch = logging.StreamHandler()
    logger.setLevel(logging.INFO)
    ch.setLevel(logging.ERROR)

    log_file = re.sub(r'\.py$', '.log', os.path.basename(__file__))
    # delete old log first if exists
    if os.path.isfile(log_file): os.remove(log_file)

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)


    # add annotations
    annotations = {}
    # download the dbox_content from aspera
    with open('flist.txt', 'w') as f:
        f.write('dbox_content\n')

    if os.path.isfile('dbox_content'): os.remove('dbox_content')

    try:
        subprocess.check_output(['ascp', '-QTl','100m','--file-list=flist.txt', '--ignore-host-key','--mode=recv','--host='+os.environ['ASCP_EGA_HOST'],'--user='+os.environ['ASCP_EGA_USER'],'.'])
    except Exception, err:
        logger(str(err))
        sys.exit(1)
    # add staged ega_file_id
    with open('dbox_content', 'r') as f:
        for l in f:
            if l.endswith('md5'): continue
            fid = l.split('/')[2].split('.')[0]
            if not annotations.get('staged'): annotations['staged'] = set()
            annotations['staged'].add(fid)

    # update the ega_xml audit repo
    try:
        origWD = os.getcwd()
        os.chdir(conf_dict.get('ega_audit').get('file_path'))
        subprocess.check_output("git checkout master", shell=True)
        subprocess.check_output("git pull", shell=True)
        os.chdir(origWD)
    except Exception, err:
        logger(str(err))
        sys.exit(1)    


    # get the fid which are to be staged by EGA
    to_stage_file_pattern = os.path.join(conf_dict.get('ega_operation').get('file_path'), '*-*', 'to_stage_*.tsv')
    files = glob.glob(to_stage_file_pattern)
    for fname in files:
        with open(fname, 'r') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for l in reader:
                 if not annotations.get('to_stage'): annotations['to_stage'] = set()
                 annotations['to_stage'].add(l.get('ega_file_id'))   

    # git pull the job repo
    try:
        origWD = os.getcwd()
        os.chdir(conf_dict.get('ega_job').get('job_repo'))
        subprocess.check_output("git checkout master", shell=True)
        subprocess.check_output("git pull", shell=True)
        os.chdir(origWD)
    except Exception, err:
        logger(str(err))
        sys.exit(1)

    # get the file list for transferring completed
    job_pattern = os.path.join(conf_dict.get('ega_job').get('job_repo'), conf_dict.get('ega_job').get('job_completed'))
    files = glob.glob(job_pattern)
    for fname in files:
        with open(fname, 'r') as f:
            job_dict = json.loads(f.read())
            for ega_file in job_dict.get('files'):
                fid = ega_file.get('ega_file_id')
                if not annotations.get('completed'): annotations['completed'] = set()
                annotations['completed'].add(fid)

    # get the file list for transferring job already queued
    for s in ['completed', 'running', 'failed', 'queued', 'backlog']:
        job_pattern = os.path.join(conf_dict.get('ega_job').get('job_repo'), conf_dict.get('ega_job').get('job_'+s))
        files = glob.glob(job_pattern)
        for fname in files:
            with open(fname, 'r') as f:
                job_dict = json.loads(f.read())
                for ega_file in job_dict.get('files'):
                    fid = ega_file.get('ega_file_id')
                    if not annotations.get('generated'): annotations['generated'] = set()
                    annotations['generated'].add(fid)

    if task == 'stage':
        generate_files_to_stage(conf_dict, annotations, seq_strategy)
    elif task == 'job':
        generate_ega_job(conf_dict, annotations, project, seq_strategy)
    else:
        pass

def generate_ega_job(conf_dict, annotations, project, seq_strategy):
    # audit path
    file_path = conf_dict.get('ega_audit').get('file_path')
    file_version = conf_dict.get('ega_audit').get('file_version')
    file_pattern = conf_dict.get('ega_audit').get('file_pattern')

    job_fields = conf_dict.get('ega_job').get('job').get('job_fields')
    mapping = conf_dict.get('ega_job').get('job').get('mapping')
    job_path = conf_dict.get('ega_job').get('job_folder')

    files = glob.glob(os.path.join(file_path, file_version, file_pattern))
    for fname in files:
        project_code = fname.split('/')[-2]
        # skip the project if not in the list of project
        if project and not project_code in project: continue

        ega_job = {}
        ega_file_ids = set()
        with open(fname) as f:
            reader = csv.DictReader(f, delimiter='\t')
            for l in reader:
                # skip the files not staged
                if not l.get('EGA File Accession') in annotations.get('staged').union(annotations.get('to_stage')): continue
                # skip the files generated
                if l.get('EGA File Accession') in annotations.get('generated'): continue
                # skip the files which does not belong to given library strategy
                if seq_strategy and not l.get('ICGC Submitted Sequencing Strategy') in seq_strategy: continue
 
                # skip the record if it has the same fid as the previous ones
                if l.get('EGA File Accession') in ega_file_ids: 
                    logger.warning('%s has more than one records in the dataset.', l.get('EGA File Accession'))
                    continue

                if l.get('EGA Analysis Accession'):
                    bundle_id = l.get('EGA Analysis Accession')
                elif l.get('EGA Run Accession'):
                    bundle_id = l.get('EGA Run Accession')
                else:
                    continue

                if not ega_job.get(bundle_id): 
                    ega_job[bundle_id] = {}
                    for field in job_fields:
                        value = l.get(mapping.get(field), None) if mapping.get(field) else None 
                        ega_job[bundle_id].update({field: value if value else None})
                    ega_job[bundle_id].update({
                        'bundle_id': bundle_id,
                        'bundle_type': 'analysis' if bundle_id.startswith('EGAZ') else 'run',
                        'ega_metadata_repo': 'https://raw.githubusercontent.com/icgc-dcc/ega-file-transfer/master/ega_xml/'+file_version,
                        'ega_metadata_file_name': 'bundle.'+bundle_id+'.xml',
                        'submitter': project_code
                    })
                        
                    ega_job[bundle_id]['files'] = []
                    
                ega_file = {}
                raw_file_name = l.get('EGA Raw Sequence Filename').split('/')[-1].rstrip('.gpg').replace('#', '_')
                ega_file.update({
                    'ega_file_id': l.get('EGA File Accession'),
                    'file_name': '.'.join([l.get('Unencrypted Checksum'), raw_file_name]),
                    'file_md5sum': l.get('Unencrypted Checksum'),
                    'idx_file_name': '.'.join([l.get('Unencrypted Checksum'), raw_file_name, 'bai']) if l.get('EGA Analysis Accession') else None
                })

                ega_job[bundle_id]['files'].append(ega_file)
                ega_file_ids.add(l.get('EGA File Accession'))


        for bundle_id, job in ega_job.iteritems():
            #update job by generating the object_id
            job.update({'ega_metadata_object_id': generate_object_id('bundle.'+bundle_id+'.xml', bundle_id, job.get('project_code'))})
            for job_file in job.get('files'):
                job_file.update({
                    'object_id': generate_object_id(job_file['file_name'], bundle_id, job.get('project_code')),
                    'idx_object_id': generate_object_id(job_file['idx_file_name'], bundle_id, job.get('project_code')) if job_file.get('idx_file_name') else None
                    })

            # write the job json
            job_name = '.'.join(['job', bundle_id, job.get('project_code'), job.get('submitter_sample_id'), job.get('ega_sample_id'), 'json'])
            with open(os.path.join(job_path, job_name), 'w') as f:
                f.write(json.dumps(job, indent=4, sort_keys=True))


def generate_files_to_stage(conf_dict, annotations, seq_strategy):
    # audit path
    file_path = conf_dict.get('ega_audit').get('file_path')
    file_version = conf_dict.get('ega_audit').get('file_version')
    file_pattern = conf_dict.get('ega_audit').get('file_pattern')
    # output path
    output_path = conf_dict.get('ega_operation').get('file_path')
    to_stage_type = conf_dict.get('ega_operation').get('to_stage').get('type')
    mapping = conf_dict.get('ega_operation').get('to_stage').get('mapping')

    # generate the files need to be staged
    files = glob.glob(os.path.join(file_path, file_version, file_pattern))
    for fname in files:
        project_code = fname.split('/')[-2]
        output_file_path = os.path.join(output_path, project_code)
        if os.path.exists(output_file_path): shutil.rmtree(output_file_path)
        os.makedirs(output_file_path)

        for t in to_stage_type:
            output_file_name = conf_dict.get('ega_operation').get('to_stage').get(t).get('file_name')
            output_fields = conf_dict.get('ega_operation').get('to_stage').get(t).get('fields')
            key = conf_dict.get('ega_operation').get('to_stage').get(t).get('key')
            require = conf_dict.get('ega_operation').get('to_stage').get(t).get('require')

            ega_file = {}
            with open(fname) as f:
                reader = csv.DictReader(f, delimiter='\t')
                for l in reader:
                    if not l.get(require[1]): continue
                    if not l.get(key[1]): continue
                    if seq_strategy and not l.get('ICGC Submitted Sequencing Strategy') in seq_strategy: continue
                    if not ega_file.get(l.get(key[1])): ega_file[l.get(key[1])] = OrderedDict()
                    for field in output_fields:
                        if not ega_file[l.get(key[1])].get(field): ega_file[l.get(key[1])][field] = set()
                        ega_file[l.get(key[1])][field].add(l.get(mapping.get(field), None))

                ega_file_list = []
                for fid, fvalue in ega_file.iteritems():
                    # skip the files already staged on ASPERA
                    if fid in annotations.get('staged'): continue 
                    # skip the files already in job completed:
                    if fid in annotations.get('completed'): continue

                    # QC the data
                    # skip the file if encrypted md5 = unencrypted md5
                    if fvalue['file_md5sum'] == fvalue['encrypted_file_md5sum']: 
                        logger.warning('%s:%s has the same file_md5sum and encrypted_file_md5sum: %s', project_code, fid, fvalue['file_md5sum'])
                        continue
                    skip=False
                    for h in output_fields:
                        # skip the file if there is any id inconsistent
                        if not h in ['dataset_id'] and len(fvalue[h]) > 1:
                            logger.warning('%s has the id inconsistent: %s', fid, h) 
                            skip=True
                    if skip: continue

                    ega_file_list.append(fvalue)
            

            # write to the file
            if not ega_file_list: continue
            with open(os.path.join(output_file_path, output_file_name), 'w') as o:
                o.write('\t'.join(output_fields) + '\n')
                for l in ega_file_list:
                    line = get_line(l)
                    o.write('\t'.join(line) + '\n')

    to_remove_file_name = os.path.join(conf_dict.get('ega_operation').get('file_path'), conf_dict.get('ega_operation').get('to_remove'))
    # generate the files to be removed
    with open(to_remove_file_name, 'w') as f:
        for fid in annotations.get('completed'):
            try:
                ret = subprocess.check_output(['grep', fid, 'dbox_content'])
                f.write(ret)
            except subprocess.CalledProcessError:
                continue      
                    

def get_line(obj):
    line = []
    for k, v in obj.iteritems():
        if isinstance(v, list):
            line.append(','.join(v))
        elif isinstance(v, set):
            line.append(','.join(list(v)))
        elif v is None:
            line.append('')
        else:
            line.append(str(v))
    return line


def generate_object_id(filename, gnos_id, project_code):
    global id_service_token
    url = 'https://meta.icgc.org/entities'
    # try get request first
    r = requests.get(url + '?gnosId=' + gnos_id + '&fileName=' + filename,
                       headers={'Content-Type': 'application/json'})
    if not r or not r.ok:
        logger.warning('GET request unable to access metadata service: {}'.format(url))
        return ''
    elif r.json().get('totalElements') == 1:
        logger.info('GET request got the id')
        return r.json().get('content')[0].get('id')
    elif r.json().get('totalElements') > 1:
        logger.warning('GET request to metadata service return multiple matches for gnos_id: {} and filename: {}'
                          .format(gnos_id, filename))
        return ''
    elif id_service_token:  # no match then try post to create
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + id_service_token
        }
        body = {
            "gnosId": gnos_id,
            "fileName": filename,
            "projectCode": project_code,
            "access": "controlled"
        }
        r = requests.post(url, data=json.dumps(body), headers=headers)
        if not r or not r.ok:
            logger.warning('POST request failed')
            return ''
        return r.json().get('id')
    else:
        logger.info('No luck, generate FAKE ID')
        return ''


if __name__ == "__main__":
    main()



    
