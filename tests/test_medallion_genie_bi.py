#!/usr/bin/env python3
import os
import sys
import unittest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, "src"))

from medallion_etl_pipeline import run_medallion_pipeline
from governed_catalog_validator import run_catalog_governance_checks
from conversational_genie_engine import run_genie_conversational_suite


class TestMedallionGenieBI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.summary = run_medallion_pipeline()
        cls.catalog = run_catalog_governance_checks()
        cls.genie = run_genie_conversational_suite()

    def test_medallion_deduplication(self):
        self.assertEqual(self.summary["bronze_ingested_rows"], 2585)
        self.assertEqual(self.summary["silver_deduplicated_rows"], 2500)
        self.assertEqual(self.summary["duplicates_eliminated"], 85)

    def test_catalog_governance_100_pass(self):
        self.assertEqual(self.catalog["quality_checks_executed"], 9)
        self.assertEqual(self.catalog["quality_checks_passed"], 9)

    def test_genie_conversational_queries(self):
        self.assertEqual(self.genie["queries_executed"], 4)
        self.assertTrue(self.genie["all_guardrails_passed"])
        for res in self.genie["benchmark_results"]:
            self.assertGreater(res["row_count_returned"], 0)


if __name__ == "__main__":
    unittest.main()
