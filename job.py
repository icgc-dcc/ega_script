import os
import csv
import json
import glob
import utils
import logging

logger = logging.getLogger(__name__)

def generate(conf_dict, annotations, project, seq_strategy):
    # audit path
    ega_audit_base_path = conf_dict.get('ega_audit_base_path')
    file_path = conf_dict.get('ega_audit').get('file_path')
    file_version = conf_dict.get('ega_audit').get('file_version')
    file_pattern = conf_dict.get('ega_audit').get('file_pattern')

    ega_job_base_path = conf_dict.get('ega_job_base_path')
    job_fields = conf_dict.get('ega_job').get('job').get('job_fields')
    mapping = conf_dict.get('ega_job').get('job').get('mapping')
    job_path = conf_dict.get('ega_job').get('job_folder')
    print os.path.join(ega_audit_base_path, file_path, file_version, file_pattern)

    files = glob.glob(os.path.join(ega_audit_base_path, file_path, file_version, file_pattern))
    print files
    for fname in files:
        project_code = fname.split('/')[-2]
        # skip the project if not in the list of project
        if project and not project_code in project: continue

        ega_job = {}
        ega_file_ids = set()
        with open(fname) as f:
            reader = csv.DictReader(f, delimiter='\t')
            for l in reader:
                # skip the files which do not belong to given library strategy
                if seq_strategy and not l.get('ICGC Submitted Sequencing Strategy') in seq_strategy: continue

                # skip the files which do not have EGA File Accession
                if not l.get('EGA File Accession'):
                    logger.warning('Donor %s::%s with sample: %s is missing EGA File Accession.', l.get('ICGC DCC Project Code'), l.get('ICGC Submitted Donor ID'), l.get('ICGC Submitted Sample ID'))
                    continue

                # # skip the files not staged or not to be staged
                # if not l.get('EGA File Accession') in annotations.get('staged').union(annotations.get('to_stage')): continue
                # skip the files not staged only for safty
                if not l.get('EGA File Accession') in annotations.get('staged'): 
                    logger.warning('%s::%s has not been staged.', project_code, l.get('EGA File Accession'))
                    continue
 
                # skip the record if it has the same fid as the previous ones
                if l.get('EGA File Accession') in ega_file_ids: 
                    logger.warning('File %s::%s has more than one records in the audit report version %s.', project_code, l.get('EGA File Accession'), file_version)
                    continue

                # skip the files generated
                if l.get('EGA File Accession') in annotations.get('generated'): 
                    logger.warning('The job for file %s::%s has already been generated.', project_code, l.get('EGA File Accession'))
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
                        'ega_metadata_file_name': 'bundle.'+bundle_id+'.xml'
                    })
                        
                    ega_job[bundle_id]['files'] = []
                    
                ega_file = {}
                raw_file_name = l.get('EGA Raw Sequence Filename').split('/')[-1].rstrip('.gpg').replace('#', '_')
                ega_file.update({
                    'ega_file_id': l.get('EGA File Accession'),
                    'file_name': '.'.join([l.get('Unencrypted Checksum'), raw_file_name]),
                    'file_md5sum': l.get('Unencrypted Checksum'),
                    'file_size': l.get('File Size', None),
                    'idx_file_name': '.'.join([l.get('Unencrypted Checksum'), raw_file_name, 'bai']) if l.get('EGA Analysis Accession') else None
                })

                ega_job[bundle_id]['files'].append(ega_file)
                ega_file_ids.add(l.get('EGA File Accession'))


        for bundle_id, job in ega_job.iteritems():
            #update job by generating the object_id
            job.update({'ega_metadata_object_id': utils.generate_object_id('bundle.'+bundle_id+'.xml', bundle_id, job.get('project_code'))})
            for job_file in job.get('files'):
                job_file.update({
                    'object_id': utils.generate_object_id(job_file['file_name'], bundle_id, job.get('project_code')),
                    'idx_object_id': utils.generate_object_id(job_file['idx_file_name'], bundle_id, job.get('project_code')) if job_file.get('idx_file_name') else None
                    })

            # write the job json
            job_name = '.'.join(['job', bundle_id, job.get('project_code'), job.get('submitter_sample_id'), job.get('ega_sample_id'), 'json'])
            with open(os.path.join(ega_job_base_path, job_path, job_name), 'w') as f:
                f.write(json.dumps(job, indent=4, sort_keys=True))


