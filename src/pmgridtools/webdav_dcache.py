import os
import xml.etree.ElementTree as ET
import re

import requests



class WebDav:
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

    def locality(self, url):
        """

        :param self:
        :param url:
        :return: ONLINE ,NEARLINE or ONLINE_AND_NEARLINE
        """
        return self.extract_locality_and_access_latencty(
            '<?xml version="1.0"?><a:propfind xmlns:a="DAV:"><a:prop><srm:FileLocality xmlns:srm="http://srm.lbl.gov/StorageResourceManager"/></a:prop></a:propfind>',
            url,
            "{http://srm.lbl.gov/StorageResourceManager}FileLocality",
        )

    def access_latency(self, url):
        """

        :param self:
        :param url:
        :return: "NEARLINE" or "ONLINE"
        """
        return self.extract_locality_and_access_latencty(
            '<?xml version="1.0"?><a:propfind xmlns:a="DAV:"><a:prop><srm:AccessLatency xmlns:srm="http://srm.lbl.gov/StorageResourceManager"/></a:prop></a:propfind>',
            url,
            "{http://srm.lbl.gov/StorageResourceManager}AccessLatency",
        )

    # TODO Rename this here and in `locality` and `access_latency`
    def extract_locality_and_access_latencty(self, xml_in, url, extract_element):
        """
        @param arg
        """
        upd = xml_in
        responds = self.session.request("PROPFIND", url, data=upd, timeout=self.timeout)
        try:
            root = ET.fromstring(responds.content)
        except ET.ParseError as e:
            if not self.exists(url):
                raise FileNotFoundError(f"Could not found {url}") from e
            else:
                raise ET.ParseError from e
        return [elem.text for elem in root.iter(extract_element)][0]

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

    def size(self, url):
        """
        Get file size
        """
        response = self.session.request("HEAD", url, timeout=self.timeout)
        if response.status_code == 200:
            return int(response.headers["Content-Length"])
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
