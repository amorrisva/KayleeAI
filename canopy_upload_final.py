#!/usr/bin/env python3
"""
CanopyRouter -- Canopy sync daemon API client.

Uploads files to Canopy via the local sync daemon REST API.
Handles authentication, folder creation, duplicate replacement,
and connection recovery.
"""

import base64
import hashlib
import json
import logging
import os
import re
import sys
import time

import requests

logger = logging.getLogger("canopy_upload")

GATEWAY_MOUNT = os.path.join(
    os.environ.get("LOCALAPPDATA", ""),
    "canopy", "Sync Dist", "gateway_shell", "mount.json"
)


class CanopyUploader:
    def __init__(self):
        self._folder_cache = set()
        self._load_mount()

    def _load_mount(self):
        """Read daemon URL and token from mount.json."""
        if not os.path.isfile(GATEWAY_MOUNT):
            raise FileNotFoundError(
                f"Canopy Drive is not installed or the service is not running.\n"
                f"Expected: {GATEWAY_MOUNT}"
            )
        with open(GATEWAY_MOUNT, "r") as f:
            mounts = json.load(f)
        self.base_url = mounts[0]["gateway.url"]
        self.token = mounts[0]["gateway.auth.access.token"]
        self.session = requests.Session()
        self.session.headers.update({
            "Gateway-Agent": "B542-R0fc7c08",
            "Authorization": f"Bearer {self.token}",
        })

    def _reconnect(self):
        """Re-read mount.json and re-authenticate (port may have changed)."""
        logger.info("Reconnecting -- re-reading mount.json")
        try:
            self._load_mount()
            return self.authenticate()
        except Exception as e:
            logger.error(f"Reconnect failed: {e}")
            return False

    def authenticate(self):
        url = f"{self.base_url}/v2/gateway_auth"
        try:
            resp = self.session.post(url, json={
                "gateway.auth.access.token": self.token,
            }, timeout=10)
        except requests.ConnectionError:
            logger.warning("Connection refused during auth -- daemon may have restarted")
            return self._reconnect() if not hasattr(self, "_reconnecting") else False
        if resp.ok:
            data = resp.json()
            self.token = data.get("gateway.auth.access.token", self.token)
            self.session.headers["Authorization"] = f"Bearer {self.token}"
            return True
        logger.error(f"Auth failed: HTTP {resp.status_code}")
        return False

    def _api_call(self, method, url, retries=2, **kwargs):
        """Make an API call with retry on connection errors."""
        kwargs.setdefault("timeout", 30)
        for attempt in range(retries + 1):
            try:
                resp = self.session.request(method, url, **kwargs)
                if resp.status_code == 401:
                    if self.authenticate():
                        continue
                    return resp
                return resp
            except (requests.ConnectionError, requests.Timeout) as e:
                if attempt < retries:
                    logger.warning(f"Connection error (attempt {attempt + 1}): {e}")
                    time.sleep(2)
                    if isinstance(e, requests.ConnectionError):
                        self._reconnect()
                else:
                    raise
        return None

    def list_folder(self, remote_path):
        """List contents of a remote folder. Returns list of items or None."""
        encoded = base64.b64encode(remote_path.encode()).decode()
        url = f"{self.base_url}/v2/gateway_metadata_children/{encoded}"
        try:
            resp = self._api_call("GET", url)
            if resp and resp.ok:
                return resp.json()
        except Exception as e:
            logger.debug(f"list_folder({remote_path}): {e}")
        return None

    def folder_exists(self, remote_path):
        if remote_path in self._folder_cache:
            return True
        encoded = base64.b64encode(remote_path.encode()).decode()
        url = f"{self.base_url}/v2/gateway_metadata_children/{encoded}"
        try:
            resp = self._api_call("GET", url)
            if resp and resp.status_code == 200:
                self._folder_cache.add(remote_path)
                return True
        except Exception:
            pass
        return False

    def create_folder(self, parent_path, folder_name):
        encoded = base64.b64encode(parent_path.encode()).decode()
        url = f"{self.base_url}/v2/gateway_metadata_folder/{encoded}"
        try:
            resp = self._api_call("POST", url, json={
                "gateway.metadata.name": folder_name,
            })
            if resp and resp.status_code == 200:
                full_path = f"{parent_path.rstrip('/')}/{folder_name}"
                self._folder_cache.add(full_path)
                return True
        except Exception as e:
            logger.error(f"create_folder({parent_path}/{folder_name}): {e}")
        return False

    def ensure_folder(self, remote_path):
        if self.folder_exists(remote_path):
            return True

        parts = remote_path.strip("/").split("/")
        create_from = 0

        for i in range(len(parts)):
            test_path = "/" + "/".join(parts[:i + 1])
            if self.folder_exists(test_path):
                create_from = i + 1
            else:
                break

        for i in range(create_from, len(parts)):
            parent = "/" + "/".join(parts[:i]) if i > 0 else "/"
            if not self.create_folder(parent, parts[i]):
                return False

        return self.folder_exists(remote_path)

    def delete_file(self, remote_path, file_id):
        """Delete a file from Canopy by its metadata ID."""
        url = f"{self.base_url}/v2/gateway_metadata/{file_id}"
        try:
            resp = self._api_call("DELETE", url)
            if resp and resp.status_code == 200:
                logger.info(f"Deleted: {remote_path}")
                return True
            logger.warning(f"Delete failed: HTTP {resp.status_code if resp else '?'}")
        except Exception as e:
            logger.warning(f"Delete failed: {e}")
        return False

    def find_existing_file(self, remote_folder, filename):
        """Check if a file already exists in the remote folder.

        Returns (file_id, existing_name) or (None, None).
        """
        items = self.list_folder(remote_folder)
        if not items:
            return None, None
        for item in items:
            if item.get("gateway.metadata.name") == filename:
                return item.get("gateway.metadata.id"), filename
        return None, None

    def upload_file(self, local_path, remote_folder, replace_existing=True):
        """Upload a file to a Canopy remote folder.

        If replace_existing is True and the file already exists,
        deletes the old version first so there's never a (1) duplicate.

        Returns (success: bool, message: str)
        """
        filename = os.path.basename(local_path)

        try:
            with open(local_path, "rb") as f:
                file_data = f.read()
        except (OSError, PermissionError) as e:
            return False, f"Cannot read file: {e}"

        file_size = len(file_data)
        file_sha256 = hashlib.sha256(file_data).hexdigest()
        mtime_ms = int(os.path.getmtime(local_path) * 1000)

        # Delete existing file if present (prevents duplicates)
        if replace_existing:
            existing_id, existing_name = self.find_existing_file(remote_folder, filename)
            if existing_id:
                self.delete_file(remote_folder, existing_id)
                logger.info(f"Replaced existing: {filename}")

        encoded = base64.b64encode(remote_folder.encode()).decode()
        url = f"{self.base_url}/v2/gateway_metadata_file/{encoded}"

        metadata = {
            "gateway.metadata.name": filename,
            "gateway.metadata.file.size": file_size,
            "gateway.metadata.file.sha256": file_sha256,
            "gateway.metadata.modified": mtime_ms,
        }

        try:
            resp = self._api_call(
                "POST", url,
                data=file_data,
                headers={
                    "Content-Type": "application/octet-stream",
                    "X-Gateway-Upload": json.dumps(metadata),
                },
                timeout=120,
            )
        except (requests.ConnectionError, requests.Timeout) as e:
            return False, f"Connection error: {e}"
        except Exception as e:
            return False, f"Upload error: {e}"

        if not resp:
            return False, "No response from server"

        if resp.status_code == 200:
            resp_data = resp.json()
            uploaded_name = resp_data.get("gateway.metadata.name", filename)
            if "(1)" in uploaded_name or "(2)" in uploaded_name:
                return True, f"OK (version: {uploaded_name})"
            return True, "OK"
        elif resp.status_code == 403:
            if self.ensure_folder(remote_folder):
                try:
                    resp = self._api_call(
                        "POST", url,
                        data=file_data,
                        headers={
                            "Content-Type": "application/octet-stream",
                            "X-Gateway-Upload": json.dumps(metadata),
                        },
                        timeout=120,
                    )
                    if resp and resp.status_code == 200:
                        return True, "OK (folder created)"
                except Exception as e:
                    return False, f"Upload after folder creation failed: {e}"
            return False, "HTTP 403: Could not create folder"
        else:
            reason = resp.headers.get("X-Reason", resp.text[:100])
            return False, f"HTTP {resp.status_code}: {reason}"
