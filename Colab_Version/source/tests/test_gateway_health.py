"""Focused checks for score-based gateway health tracking."""

import tempfile
import unittest
from pathlib import Path
import sys
import types

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
if "curl_cffi" not in sys.modules:
    fake_curl = types.ModuleType("curl_cffi")
    fake_curl.requests = types.SimpleNamespace(Session=object)
    sys.modules["curl_cffi"] = fake_curl

from app.network_manager import NetworkManager


class GatewayHealthTests(unittest.TestCase):
    def test_status_scores_and_failure_metadata(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            manager = NetworkManager(Path(temporary_directory))
            gateway = "https://library.lol/main/ashrae.pdf"
            manager.gateway_result(gateway, 206, completed=True)
            manager.gateway_result(gateway, 500)
            manager.gateway_result(gateway, 504)

            health = manager._gateway_health[gateway]
            self.assertEqual(manager.gateway_score(gateway), -40)
            self.assertEqual(health["gateway_url"], gateway)
            self.assertEqual(health["success_count"], 1)
            self.assertEqual(health["failure_count"], 2)
            self.assertIsNotNone(health["last_failure"])
            manager.close()
