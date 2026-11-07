"""Unit tests for the Feature-Store Drift Checks & AWS/Tableau Retail Mart Validator."""

from __future__ import annotations

import unittest
from pathlib import Path

from feature_store_drift_and_tableau_validator import run_feature_store_and_tableau_validation


class TestFeatureStoreAndTableauGates(unittest.TestCase):
    def test_feature_store_and_tableau_validation(self) -> None:
        root = Path(__file__).resolve().parent.parent
        report = run_feature_store_and_tableau_validation(root)
        self.assertTrue(report["all_passed"])
        self.assertEqual(report["retail_accounts_validated"], 4372)
        self.assertEqual(report["quality_checks_passed"], 9)
        self.assertAlmostEqual(report["tableau_reporting_latency_reduction_pct"], 62.4, places=1)
        self.assertLess(report["feature_store_psi"], 0.05)


if __name__ == "__main__":
    unittest.main()
