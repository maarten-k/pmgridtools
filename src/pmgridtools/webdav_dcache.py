import os
import xml.etree.ElementTree as ET

import requests


class WebDav:
    """WebDAV client for dCache operations."""

    def __init__(self) -> None:
        """
        Initialize WebDAV client.
        """
        self.cert: str = os.environ["X509_USER_PROXY"]
        self.capath: str = "/etc/grid-security/certificates/"
        self.session: requests.Session = requests.Session()
        self.session.verify = "/etc/grid-security/certificates/"
        self.session.cert = self.cert
        self.timeout: int = 20

    def adler32(self, url: str) -> str:
        """
        Get ADLER32 checksum of a file.

        :param url: File URL
        :return: ADLER32 checksum string
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

    def locality(self, url: str) -> str:
        """
        Get file locality information.

        :param url: File URL
        :return: File locality (ONLINE, NEARLINE, or ONLINE_AND_NEARLINE)
        """
        return self._extract_locality_and_access_latency(
            '<?xml version="1.0"?><a:propfind xmlns:a="DAV:">'
            '<a:prop><srm:FileLocality xmlns:srm="http://srm.lbl.gov/StorageResourceManager"/>'
            "</a:prop></a:propfind>",
            url,
            "{http://srm.lbl.gov/StorageResourceManager}FileLocality",
        )

    def access_latency(self, url: str) -> str:
        """
        Get file access latency information.

        :param url: File URL
        :return: Access latency ("NEARLINE" or "ONLINE")
        """
        return self._extract_locality_and_access_latency(
            '<?xml version="1.0"?><a:propfind xmlns:a="DAV:">'
            '<a:prop><srm:AccessLatency xmlns:srm="http://srm.lbl.gov/StorageResourceManager"/>'
            "</a:prop></a:propfind>",
            url,
            "{http://srm.lbl.gov/StorageResourceManager}AccessLatency",
        )

    def _extract_locality_and_access_latency(
        self, xml_in: str, url: str, extract_element: str
    ) -> str:
        """
        Extract locality or access latency from XML response.

        :param xml_in: XML request body
        :param url: File URL
        :param extract_element: XML element to extract
        :return: Extracted value
        """
        upd: str = xml_in
        responds: requests.Response = self.session.request(
            "PROPFIND", url, data=upd, timeout=self.timeout
        )
        try:
            root = ET.fromstring(responds.content)
        except ET.ParseError as e:
            if not self.exists(url):
                raise FileNotFoundError(f"Could not found {url}") from e
            else:
                raise ET.ParseError from e
        return [elem.text for elem in root.iter(extract_element)][0]

    def md5sum(self, url: str) -> str:
        """
        Get MD5 checksum of a file.

        :param url: File URL
        :return: MD5 checksum string
        """
        raise NotImplementedError

    def remove(self, url: str) -> None:
        """
        Remove a file.

        :param url: File URL
        """
        response: requests.Response = self.session.request(
            "DELETE", url, timeout=self.timeout
        )
        print(response)

    def move(self, urlfrom: str, urlto: str) -> None:
        """
        Move a file.

        :param urlfrom: Source URL
        :param urlto: Destination URL
        """
        raise NotImplementedError

    def upload(self, file: str, url: str) -> None:
        """
        Upload a file.

        :param file: Local file path
        :param url: Destination URL
        """
        raise NotImplementedError

    def download(self, url: str, localfile: str) -> str:
        """
        Download a file.

        :param url: Source URL
        :param localfile: Local file path
        :return: Local file path
        """

        with self.session.get(url, stream=True, timeout=self.timeout) as d:
            d.raise_for_status()
            with open(localfile, "wb") as lf:
                for chunk in d.iter_content(chunk_size=4194304):
                    if chunk:
                        lf.write(chunk)
        return localfile

    def size(self, url: str) -> int:
        """
        Get file size.

        :param url: File URL
        :return: File size in bytes
        """
        response: requests.Response = self.session.request(
            "HEAD", url, timeout=self.timeout
        )
        if response.status_code == 200:
            return int(response.headers["Content-Length"])
        elif response.status_code == 404:
            raise ValueError(f"file not found: {url}")
        elif response.status_code == 403:
            raise PermissionError(f"response code {response.status_code} : {url}")
        else:
            raise ValueError(
                f"response code not a default value (expected 200 or 404){response.status_code} : {url}"
            )

    def cat(self, url: str) -> bytes:
        """
        Get file content.

        :param url: File URL
        :return: File content as bytes
        """
        return self.session.get(url, timeout=self.timeout).content

    def exists(self, url: str) -> bool:
        """
        Check if file exists.

        :param url: File URL
        :return: True if file exists, False otherwise
        """
        response: requests.Response = self.session.request(
            "HEAD", url, timeout=self.timeout
        )
        if response.status_code == 200:
            return True
        elif response.status_code == 404:
            return False
        elif response.status_code == 403:
            raise PermissionError(f"response code {response.status_code} : {url}")
        else:
            raise ValueError(
                f"response code not a default value (expected 200 or 404){response.status_code} {url}"
            )

    def _get_head(self, url: str) -> requests.Response:
        """
        Get HEAD response for a URL.

        :param url: File URL
        :return: Response object
        """
        return self.session.request("HEAD", url, timeout=self.timeout)
