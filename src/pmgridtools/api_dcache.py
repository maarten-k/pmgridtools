import os
from typing import Any, Dict, List, Union

import requests


class dcacheapy:
    """dCache API wrapper for file operations."""

    def __init__(self) -> None:
        """
        Initialize dCache API client.
        """
        self.cert: str = os.environ["X509_USER_PROXY"]
        self.capath: str = "/etc/grid-security/certificates/"
        self.session: requests.Session = requests.Session()
        self.session.verify = "/etc/grid-security/certificates/"
        self.session.cert = self.cert
        self.timeout: int = 20
        self.api: str = "https://dcacheview.grid.surfsara.nl:22882/api/v1"

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

    def stage(self, pnfs: Union[str, List[str]], lifetime: int = 3) -> None:
        """
        Stage files from tape to disk.

        :param pnfs: File path or list of file paths
        :param lifetime: Lifetime in hours
        """
        if isinstance(pnfs, str):
            pnfs = [pnfs]

        headers: Dict[str, str] = {
            "accept": "application/json",
            "content-type": "application/json",
        }
        data: Dict[str, Any] = {
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

    def locality(self, pnfs: str) -> str:
        """
        Get file locality information.

        :param pnfs: File path
        :return: File locality (ONLINE, NEARLINE, or ONLINE_AND_NEARLINE)
        """
        params: Dict[str, str] = {"locality": "true"}
        headers: Dict[str, str] = {"accept": "application/json"}
        url: str = f"{self.api}/namespace/{pnfs}"
        # Make the GET request
        response: requests.Response = self.session.get(
            url, params=params, headers=headers
        )

        # Handle the response
        if response.ok:
            return response.json()["fileLocality"]
        else:
            raise RuntimeError(f"API request failed: {response}")

    # def access_latency(self, url):
    #     """

    #     :param self:
    #     :param url:
    #     :return: "NEARLINE" or "ONLINE"
    #     """
    #     return self.extract_locality_and_access_latencty(
    #         '<?xml version="1.0"?><a:propfind xmlns:a="DAV:">
    # <a:prop><srm:AccessLatency xmlns:srm="http://srm.lbl.gov/StorageResourceManager"/>
    # </a:prop></a:propfind>',
    #         url,fileLocality
    #         "{http://srm.lbl.gov/StorageResourceManager}AccessLatency",
    #     )

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

    def size(self, pnfs: str) -> int:
        """
        Get file size.

        :param pnfs: File path
        :return: File size in bytes
        """
        url: str = f"{self.api}/namespace/{pnfs}"
        response: requests.Response = self.session.get(url, timeout=self.timeout)
        if response.status_code == 200:
            return int(response.json()["size"])
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
