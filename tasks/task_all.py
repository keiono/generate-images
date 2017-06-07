import luigi
import requests
import json
import logging
import binascii
import glob
import os

# Image generator service URL
BASE = 'http://169.228.38.217:5000/'
NDEX_URL = 'http://public.ndexbio.org/v2'

ID_LIST_FILE = 'id_list.json'


# This is an independent workflow running as a separate process in Luigi.


class RunTask(luigi.Task):

    def output(self):
        return luigi.LocalTarget('report.txt')

    def run(self):
        with open(ID_LIST_FILE, 'r') as f:
            data = json.load(f)

        # List of NDEx IDs
        ids = data['ids']

        # Credential is optional

        uid = ""
        pw = ""
        if 'credential' in data:
            uid = data['credential']['id']
            pw = data['credential']['password']

        logging.warn('Start2 ***************')
        logging.warn(ids)

        for ndex_id in ids:
            yield CacheImage(ndex_id, uid, pw)

        # Consolidate results
        logging.warn('Writing report ***************')
        report_files = glob.glob('/app/report*.txt')

        image_url_list = []
        with self.output().open('w') as f:
            for fl in report_files:
                with open(fl) as report:
                    rep = report.read()
                    image_url_list.append(rep)
                    os.remove(fl)

            json.dump(image_url_list, f)

        logging.warn('------------ Finished2 -----------------')


class GetNetworkFile(luigi.Task):

    network_id = luigi.Parameter()
    id = luigi.Parameter()
    pw = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(str(self.network_id) + '.json')

    def run(self):

        with self.output().open('w') as f:

            # Check optional parameter
            s = requests.session()

            if self.id is not "" and self.pw is not "":
                s.auth = (self.id, self.pw)
                res = s.get(NDEX_URL + NDEX_AUTH)

                if res.status_code != 200:
                    raise ValueError('Invalid credential given.')

            network_url = NDEX_URL + 'network/' + self.network_id + '/asCX'

            response = s.get(network_url, stream=True)

            # Error check: 401 may be given
            if response.status_code != 200:
                logging.warn('!!!!!!!!!!!!!!!!!!!!!!!Error getting CX network: CODE ' + str(response.status_code))
                raise RuntimeError("Could not fetch CX network file.")

            for block in response.iter_content(1024):
                f.write(block.decode('utf-8'))




class GenerateImage(luigi.Task):

    network_id = luigi.Parameter()
    id = luigi.Parameter()
    pw = luigi.Parameter()

    def requires(self):
        return {
            'cx': ConvertID(self.network_id, self.id, self.pw)
        }

    def output(self):
        return luigi.LocalTarget("graph_image_" + self.network_id + ".svg")

    def run(self):
        with self.output().open('w') as f:
            svg_image_url = BASE + 'image/svg'
            cx_file = self.input()['cx'].open('r')
            data = json.loads(cx_file.read())
            res = requests.post(svg_image_url, json=data)
            cx_file.close()

            f.write(res.content.decode('utf-8'))

        # Remove input file
        os.remove(str(self.network_id) + '.json')
        os.remove(str(self.network_id) + '.mapped.json')
        logging.warn('%%%%%%%%%%% Done: ' + str(self.network_id))



class GenerateIdList(luigi.Task):

    def output(self):
        return luigi.LocalTarget("uuid.txt")

    def run(self):
        with self.output().open('w') as f:
            ids = self._fetch()
            f.write(json.dumps(ids))

    def _fetch(self):
        # Get all available ID
        q = {
            'searchString': '*'
        }

        searchUrl = "http://public.ndexbio.org/v2/search/network?start=0&size=100000"
        res = requests.post(searchUrl, json=q)
        result = res.json()
        networks = result['networks']

        NODE_COUNT_MAX = 10000
        EDGE_COUNT_MAX = 10000

        newMap = list(map(lambda y: y['externalId'], filter(lambda x: x['nodeCount'] < NODE_COUNT_MAX and x['edgeCount'] < EDGE_COUNT_MAX, networks)))

        return newMap


if __name__ == '__main__':
    luigi.run(['GenerateIdList', '--workers', '5'])
