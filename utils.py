import requests
import logging
import os
import yaml
import logging

logger = logging.getLogger(__name__)


def setup_logging(
    default_path='logging.yaml',
    default_level=logging.INFO,
    env_key='LOG_CFG'
):
    """Setup logging configuration

    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


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
    id_service_token = os.environ.get('ICGC_TOKEN')
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
