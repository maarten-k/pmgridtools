import os
import requests


class dcacheapy:
    """ """

    def __init__(self):
        """

        :return:
        """
        self.cert = os.environ["X509_USER_PROXY"]
        self.capath = "/etc/grid-security/certificates/"
        self.session = requests.Session()
        self.session.verify = "/etc/grid-security/certificates/"
        self.session.cert = self.cert
        self.timeout = 20
        self.api = "https://dcacheview.grid.surfsara.nl:22882/api/v1"

    def adler32(self, url):
        """

        :param url:
        :return:
        """
        headers = {
            "Want-Digest": "ADLER32",
        }

        responds = self.session.head(url, headers=headers, timeout=self.timeout)
        if responds.status_code == 200:
            try:
                adler32 = responds.headers["Digest"].split("=")[1]
            except KeyError as e:
                print(responds.headers)
                raise KeyError from e
        elif responds.status_code == 404:
            raise FileNotFoundError(f"Could not found {url}")

        return adler32

    def stage(self, pnfs, lifetime=3):
        if pnfs.__class__ == "str":
            pnfs = [pnfs]
        # curl --capath /etc/grid-security/certificates --cert /home/projectmine-mkooyman/.proxy --cacert /home/projectmine-mkooyman/.proxy -H 'accept: application/json' --fail --silent --show-error --ipv4 -H 'content-type: application/json' -X POST https://dcacheview.grid.surfsara.nl:22882/api/v1/bulk-requests -d '{"activity": "PIN", "arguments": {"lifetime": "7", "lifetimeUnit":"DAYS"}, "target": ["//pnfs/grid.sara.nl/data/lsgrid/Project_MinE/Realignment/hg38/Netherlands/Tape/NovaSeqHartwig/HW0174512325/HW0174512325.final-gatk.cram","//pnfs/grid.sara.nl/data/lsgrid/Project_MinE/Realignment/hg38/Netherlands/Tape/NovaSeqHartwig/HW0174512325/HW0174512325.final-gatk.cram.crai"], "expand_directories": "TARGETS"}' --dump-header -
        headers = {"accept": "application/json", "content-type": "application/json"}
        data = {
            "activity": "PIN",
            "arguments": {"lifetime": lifetime, "lifetimeUnit": "HOURS"},
            "target": pnfs,
            "expand_directories": "TARGETS",
        }
        url = f"{self.api}/bulk-requests"
        response = self.session.post(
            url,
            json=data,
            headers=headers,
        )
        response.raise_for_status()

    def locality(self, pnfs):
        """

        :param self:
        :param url:fileLocality
        :return: ONLINE ,NEARLINE or ONLINE_AND_NEARLINE
        """
        params = {"locality": "true"}
        headers = {"accept": "application/json"}
        url = f"{self.api}/namespace/{pnfs}"
        # Make the GET request
        response = self.session.get(url, params=params, headers=headers)

        # Handle the response
        if response.ok:
            return response.json()["fileLocality"]
        else:

            raise (f"fubar {response}")

    # def access_latency(self, url):
    #     """

    #     :param self:
    #     :param url:
    #     :return: "NEARLINE" or "ONLINE"
    #     """
    #     return self.extract_locality_and_access_latencty(
    #         '<?xml version="1.0"?><a:propfind xmlns:a="DAV:"><a:prop><srm:AccessLatency xmlns:srm="http://srm.lbl.gov/StorageResourceManager"/></a:prop></a:propfind>',
    #         url,fileLocality
    #         "{http://srm.lbl.gov/StorageResourceManager}AccessLatency",
    #     )

    def md5sum(self, url):
        """

        :param self:
        :param url:
        :return:

        """
        raise NotImplementedError

    def remove(self, url):
        """

        :param self:
        :param url:
        :return:
        """

        response = self.session.request("DELETE", url, timeout=self.timeout)
        print(response)
        # execute(cmd)

    def move(self, urlfrom, urlto):
        """

        :param self:
        :param urlfrom:
        :param urlto:
        :return:
        """
        raise NotImplementedError

    def upload(self, file, url):
        """

        :param self:
        :param file:
        :param url:
        :return:
        """
        raise NotImplementedError

    def download(self, url, localfile):
        """

        :param self:
        :param url:
        :param localfile:
        :return:
        """

        with self.session.get(url, stream=True, timeout=self.timeout) as d:
            d.raise_for_status()
            with open(localfile, "wb") as lf:
                for chunk in d.iter_content(chunk_size=4194304):
                    if chunk:
                        lf.write(chunk)
        return localfile

    def size(self, pnfs):
        """
        Get file size
        """
        url = f"{self.api}/namespace/{pnfs}"
        # headers = {"accept": "application/json", "content-type": "application/json"}
        response = self.session.get(url, timeout=self.timeout)
        if response.status_code == 200:
            return int(response.json()["size"])
        elif response.status_code == 404:
            raise ValueError(f"file not found: {url}")
        elif response.status_code == 403:
            raise PermissionError(f"resonds code {response.status_code} : {url}")
        else:
            raise ValueError(
                f"resonds code not a default value (expected 200 or 404){response.status_code} : {url}"
            )

    def cat(self, url):
        """

        :param self:
        :param url:
        :return:

        """
        return self.session.get(url, timeout=self.timeout).content

    def exists(self, url):
        response = self.session.request("HEAD", url, timeout=self.timeout)
        if response.status_code == 200:
            return True
        elif response.status_code == 404:
            return False
        elif response.status_code == 403:
            raise PermissionError(f"resonds code {response.status_code} : {url}")
        else:
            raise ValueError(
                f"resonds code not a default value (expected 200 or 404){response.status_code} {url}"
            )

    def _get_head(self, url):
        return self.session.request("HEAD", url, timeout=self.timeout)
