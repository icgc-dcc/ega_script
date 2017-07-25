# ega_script

The tool is used to:
* Generate the jobs for ega-file-transfer-to-collab
* Audit and report the files to be staged to or removed from ega Aspera server

## Getting Started
The tool needs to talk to two different kinds of git repository to gather the information to realize the above tasks. 
* The ega auditing git repository(https://github.com/icgc-dcc/ega-file-transfer.git)
* Job repositories which are tracking ega-file-transfer-to-collab job status(e.g., http://142.1.177.124/jt-hub/ega-file-transfer-to-collab-jtracker)

### Prerequisites
Before you can run the tool, you need to configure the tool. The configuration file locates `ega_script/conf/conf.yaml`. You may need to change the following two places accordingly.
```
ega_audit_base_path: "../ega-file-transfer"
ega_job_base_path: ".."
```

The above default configuration will assume:
* All the job repos which are set up for ega-file-transfer-to-collab, e.g., `ega-file-transfer-to-collab-jtracker`(http://142.1.177.124/jt-hub/ega-file-transfer-to-collab-jtracker), are `git clone` to the same folder as the tool script
* The ega auditing git repository: `ega-file-transfer`(https://github.com/icgc-dcc/ega-file-transfer) is also `git clone` to the same folder as the tool script
```
├── ega-file-transfer
├── ega-file-transfer-to-collab-2-jtracker
├── ega-file-transfer-to-collab-3-jtracker
├── ega-file-transfer-to-collab-4-jtracker
├── ega-file-transfer-to-collab-5-jtracker
├── ega-file-transfer-to-collab-jtracker
└── ega_script

```
Since ega auditing git repository is version controlled, before we can run the tool to generate the jobs and report `to_stage` or `to_remove` files, we also need to set the version of the ega auditing reports in the `conf/conf.yaml`, e.g.,
```
file_version: "v20170630"
```

### Installing

Get the source script of the tool
```
git clone git@github.com:lindaxiang/ega_script.git
```
Then you can run `./main.py -h` to get the usage of the tool
```
usage: main.py [-h] [-c CONF] -t TASK [-p [PROJECT [PROJECT ...]]]
               [-s [SEQ_STRATEGY [SEQ_STRATEGY ...]]]

EGA-file-to-colllab job generator and auditor

optional arguments:
  -h, --help            show this help message and exit
  -c CONF, --setting CONF
                        Specify ega setting file
  -t TASK, --task TASK  Specify the task
  -p [PROJECT [PROJECT ...]], --project [PROJECT [PROJECT ...]]
                        Specify the project
  -s [SEQ_STRATEGY [SEQ_STRATEGY ...]], --seq_strategy [SEQ_STRATEGY [SEQ_STRATEGY ...]]
                        Specify the sequencing strategy
```

## Running the tool to generate the jobs 
For example generating jobs for `RNA-Seq` data of project `CLLE-ES`, do this: 
```
cd ega_script
./main.py -t job -p CLLE-ES -s RNA-Seq
```
* If no `project` is specified, the tool will generate the eligible jobs for all the projects which have auditing reports available.
* If no `seq_strategy` is specified, the tool will generate the eligible jobs for all kinds of seq_strategy which are included in the related auditing reports. 
* The generated jobs locates in `job_state.backlog` of one of the job repositories which is defined in the `conf/conf.yaml`, you can change the `job folder` if needed:
```
job_folder: "ega-file-transfer-to-collab-2-jtracker/ega-file-transfer-to-collab.0.6.jtracker/job_state.backlog"
```

## Running the tool to generate the `to_stage` files
In order to get the list of files which are to be staged to Aspera server by EGA, do this:
```
cd ega_script
./main.py -t stage
```
You can specify the `project` and `seq_strategy` in order to get the list of files which are only for given sequence trategies and belong to given projects.
The tool will generate `to_stage_*.tsv` files under each project. For example:
```
ega_operation/
├── BRCA-KR
│   └── to_stage_run.tsv
├── CLLE-ES
│   └── to_stage_run.tsv
├── LICA-FR
│   └── to_stage_run.tsv
├── MALY-DE
│   └── to_stage_analysis.tsv
├── OV-AU
│   └── to_stage_analysis.tsv
├── PACA-AU
│   ├── to_stage_analysis.tsv
│   └── to_stage_run.tsv
├── PAEN-AU
│   └── to_stage_analysis.tsv
└── to_remove.tsv
```

## Running tool to generate the `to_remove` files
In order to list all files which can be removed from Aspera server by EGA, do this:
```
cd ega_script
./main.py -t remove
```
The tool will generate `to_remove.txt` file locating at: `ega-file-transfer/ega_operation/to_remove.tsv`

## Log information
When using the tool to generate the jobs or report the `to_stage` or `to_remove` files, the tool did many QC checks based on the auditing reports, the QC results are logged into the `*.log` files locates:
```
ega_script/log/
├── error.log
├── info.log
└── warn.log
```
Here are some sample log messages:
```
2017-07-25 15:12:45,689 - audit.stage - WARNING - LICA-FR::EGAF00000483937 has the same file_md5sum and encrypted_file_md5sum: set(['772febc5f8fea25a9b09e43dd51e43bd'])
2017-07-25 15:12:45,690 - audit.stage - WARNING - LICA-FR::EGAF00000483938 has the same file_md5sum and encrypted_file_md5sum: set(['170588f8a583c2d4fee882fdfcb6133b'])
2017-07-25 15:12:45,690 - audit.stage - WARNING - LICA-FR::EGAF00000483899 has the same file_md5sum and encrypted_file_md5sum: set(['83aed772452945dc994bcfad7edebc3a'])
2017-07-25 15:12:49,248 - audit.stage - WARNING - MALY-DE::EGAF00001592148 has the id inconsistent: ega_analysis_id in audit report version v20170630
2017-07-25 15:12:49,248 - audit.stage - WARNING - MALY-DE::EGAF00001592148 has the id inconsistent: file_name in audit report version v20170630
2017-07-25 15:12:49,248 - audit.stage - WARNING - MALY-DE::EGAF00001592148 has the id inconsistent: encrypted_file_md5sum in audit report version v20170630
```

## Authors

* **Linda Xiang** - *Initial work* 




