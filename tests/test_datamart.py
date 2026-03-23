from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from typing import cast

from datamart.db import get_connection, initialize_database
from datamart.qa import answer_question
from datamart.semantic import SemanticQuery, run_semantic_query
from datamart.simulator import DatamartSimulator, SimulationConfig
from datamart.warehouse import build_star_schema

ROOT_DIR = Path(__file__).resolve().parents[1]
HAS_SEMANTIC_DEPS = (
    importlib.util.find_spec("ibis") is not None and importlib.util.find_spec("yaml") is not None
)


class DatamartSmokeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "datamart-test.db"
        self.connection = get_connection(self.db_path)
        initialize_database(self.connection)

    def tearDown(self) -> None:
        self.connection.close()
        self.temp_dir.cleanup()

    def _build_demo(self, days: int = 10, seed: int = 7) -> None:
        config = SimulationConfig(days=days, seed=seed)
        DatamartSimulator(self.connection, config).run()
        build_star_schema(self.connection, config.start_date, config.days)

    def test_simulation_builds_multi_fact_warehouse(self) -> None:
        self._build_demo(days=10)

        self.assertGreater(
            self.connection.execute("SELECT COUNT(*) AS count FROM customers").fetchone()["count"],
            0,
        )
        self.assertGreater(
            self.connection.execute("SELECT COUNT(*) AS count FROM products").fetchone()["count"],
            0,
        )
        self.assertGreater(
            self.connection.execute("SELECT COUNT(*) AS count FROM fact_order_items").fetchone()[
                "count"
            ],
            0,
        )
        self.assertGreater(
            self.connection.execute("SELECT COUNT(*) AS count FROM fact_payments").fetchone()[
                "count"
            ],
            0,
        )
        self.assertGreater(
            self.connection.execute(
                "SELECT COUNT(*) AS count FROM fact_support_tickets"
            ).fetchone()["count"],
            0,
        )
        self.assertGreater(
            self.connection.execute("SELECT COUNT(*) AS count FROM fact_daily_kpis").fetchone()[
                "count"
            ],
            0,
        )
        self.assertGreater(
            self.connection.execute("SELECT COUNT(*) AS count FROM fact_weekly_kpis").fetchone()[
                "count"
            ],
            0,
        )
        self.assertGreater(
            self.connection.execute("SELECT COUNT(*) AS count FROM fact_monthly_kpis").fetchone()[
                "count"
            ],
            0,
        )

    def test_daily_weekly_monthly_net_revenue_aligns(self) -> None:
        self._build_demo(days=14)

        daily_value = self.connection.execute(
            """
            SELECT ROUND(SUM(f.kpi_value), 2) AS value
            FROM fact_daily_kpis f
            JOIN dim_kpi k ON k.kpi_key = f.kpi_key
            JOIN dim_country co ON co.country_key = f.country_key
            JOIN dim_segment s ON s.segment_key = f.segment_key
            JOIN dim_category c ON c.category_key = f.category_key
            JOIN dim_channel ch ON ch.channel_key = f.channel_key
            JOIN dim_payment_method pm ON pm.payment_method_key = f.payment_method_key
            JOIN dim_ticket_type tt ON tt.ticket_type_key = f.ticket_type_key
            WHERE k.kpi_name = 'net_revenue'
              AND co.country_code = 'all'
              AND s.segment_name = 'all'
              AND c.category_name = 'all'
              AND ch.channel_name = 'all'
              AND pm.payment_method_name = 'all'
              AND tt.ticket_type_name = 'all'
            """
        ).fetchone()["value"]

        weekly_value = self.connection.execute(
            """
            SELECT ROUND(SUM(f.kpi_value), 2) AS value
            FROM fact_weekly_kpis f
            JOIN dim_kpi k ON k.kpi_key = f.kpi_key
            JOIN dim_country co ON co.country_key = f.country_key
            JOIN dim_segment s ON s.segment_key = f.segment_key
            JOIN dim_category c ON c.category_key = f.category_key
            JOIN dim_channel ch ON ch.channel_key = f.channel_key
            JOIN dim_payment_method pm ON pm.payment_method_key = f.payment_method_key
            JOIN dim_ticket_type tt ON tt.ticket_type_key = f.ticket_type_key
            WHERE k.kpi_name = 'net_revenue'
              AND co.country_code = 'all'
              AND s.segment_name = 'all'
              AND c.category_name = 'all'
              AND ch.channel_name = 'all'
              AND pm.payment_method_name = 'all'
              AND tt.ticket_type_name = 'all'
            """
        ).fetchone()["value"]

        monthly_value = self.connection.execute(
            """
            SELECT ROUND(SUM(f.kpi_value), 2) AS value
            FROM fact_monthly_kpis f
            JOIN dim_kpi k ON k.kpi_key = f.kpi_key
            JOIN dim_country co ON co.country_key = f.country_key
            JOIN dim_segment s ON s.segment_key = f.segment_key
            JOIN dim_category c ON c.category_key = f.category_key
            JOIN dim_channel ch ON ch.channel_key = f.channel_key
            JOIN dim_payment_method pm ON pm.payment_method_key = f.payment_method_key
            JOIN dim_ticket_type tt ON tt.ticket_type_key = f.ticket_type_key
            WHERE k.kpi_name = 'net_revenue'
              AND co.country_code = 'all'
              AND s.segment_name = 'all'
              AND c.category_name = 'all'
              AND ch.channel_name = 'all'
              AND pm.payment_method_name = 'all'
              AND tt.ticket_type_name = 'all'
            """
        ).fetchone()["value"]

        self.assertEqual(daily_value, weekly_value)
        self.assertEqual(daily_value, monthly_value)

    def test_question_answers_return_expected_shapes(self) -> None:
        self._build_demo(days=14)

        payments_answer = answer_question(self.connection, "Show payments by method yesterday")
        tickets_answer = answer_question(self.connection, "Show tickets by type yesterday")
        trend_answer = answer_question(
            self.connection, "Show daily new customers for the last 14 days"
        )
        best_selling_answer = answer_question(self.connection, "Show best selling products top 5")
        comparison_answer = answer_question(self.connection, "Show week vs week revenue")

        self.assertIn("payment_method_name=", payments_answer)
        self.assertIn("ticket_type_name=", tickets_answer)
        self.assertIn("kpi_date=", trend_answer)
        self.assertIn("value=", trend_answer)
        self.assertIn("product_name=", best_selling_answer)
        self.assertIn("items_sold=", best_selling_answer)
        self.assertIn("current_value=", comparison_answer)
        self.assertIn("previous_value=", comparison_answer)

    def test_unknown_question_returns_guidance(self) -> None:
        self._build_demo(days=10)

        answer = answer_question(
            self.connection, "Explain customer happiness trend by superhero tier"
        )

        self.assertIn("Question not recognized.", answer)
        self.assertIn("What was net revenue in the last 7 days?", answer)

    @unittest.skipUnless(HAS_SEMANTIC_DEPS, "Ibis and PyYAML are required for semantic tests.")
    def test_semantic_query_returns_business_friendly_result(self) -> None:
        self._build_demo(days=14)

        rows = run_semantic_query(
            self.connection,
            SemanticQuery(
                metric="payments_collected",
                dimensions=("payment_method",),
                day_offset=1,
                aggregate_over_time=True,
            ),
        )

        self.assertGreater(len(rows), 0)
        self.assertIn("payment_method_name", rows[0])
        self.assertIn("value", rows[0])

    @unittest.skipUnless(HAS_SEMANTIC_DEPS, "Ibis and PyYAML are required for semantic tests.")
    def test_semantic_query_supports_ranked_product_results(self) -> None:
        self._build_demo(days=14)

        rows = run_semantic_query(
            self.connection,
            SemanticQuery(
                metric="items_sold",
                dimensions=("product",),
                trailing_days=14,
                aggregate_over_time=True,
                order_by_metric="items_sold",
                order_direction="desc",
                limit=5,
            ),
        )

        self.assertEqual(len(rows), 5)
        self.assertIn("product_name", rows[0])
        self.assertIn("value", rows[0])
        first_value = cast(float, rows[0]["value"])
        last_value = cast(float, rows[-1]["value"])
        self.assertGreaterEqual(first_value, last_value)

    @unittest.skipUnless(HAS_SEMANTIC_DEPS, "Ibis and PyYAML are required for semantic tests.")
    def test_semantic_query_supports_previous_period_comparison(self) -> None:
        self._build_demo(days=21)

        rows = run_semantic_query(
            self.connection,
            SemanticQuery(
                metric="net_revenue",
                grain="week",
                compare_to="previous_period",
            ),
        )

        self.assertEqual(len(rows), 1)
        self.assertIn("current_period_start", rows[0])
        self.assertIn("previous_period_start", rows[0])
        self.assertIn("current_value", rows[0])
        self.assertIn("previous_value", rows[0])
        self.assertIn("delta_value", rows[0])


class SemanticArtifactTest(unittest.TestCase):
    def test_semantic_files_exist_and_contain_expected_sections(self) -> None:
        semantic_model = (ROOT_DIR / "semantic_model.yaml").read_text()
        semantic_guide = (ROOT_DIR / "AGENT_SQL_SEMANTIC_GUIDE.md").read_text()
        pyproject = (ROOT_DIR / "pyproject.toml").read_text()

        self.assertIn("metrics:", semantic_model)
        self.assertIn("dimensions:", semantic_model)
        self.assertIn("net_revenue:", semantic_model)
        self.assertIn("payment_method:", semantic_model)
        self.assertIn("product:", semantic_model)

        self.assertIn("Semantic workflow", semantic_guide)
        self.assertIn("Metric-to-source guidance", semantic_guide)
        self.assertIn("SQL prompt template", semantic_guide)
        self.assertIn("ibis-framework[sqlite]", pyproject)
        self.assertIn("PyYAML", pyproject)
        self.assertIn("order_by_metric", (ROOT_DIR / "AGENT_SEMANTIC_QUERY_SPEC.md").read_text())
        self.assertIn("compare_to", (ROOT_DIR / "AGENT_SEMANTIC_QUERY_SPEC.md").read_text())


if __name__ == "__main__":
    unittest.main()
