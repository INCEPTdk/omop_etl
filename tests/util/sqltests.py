import unittest

from etl.util.sql import clean_sql


class CleanSqlUnitTests(unittest.TestCase):
    def test_decorator_no_args(self):
        @clean_sql
        def _dummy() -> str:
            return """
        SELECT *
        FROM mytable
        WHERE mytable.x = 89;
        """

        self.assertEqual(
            _dummy(), "SELECT * FROM mytable WHERE mytable.x = 89;"
        )

    def test_decorator_with_quotes(self):
        @clean_sql
        def _dummy() -> str:
            return """
        SELECT *
        FROM mytable
        WHERE mytable.x = '89';
        """

        self.assertEqual(
            _dummy(), "SELECT * FROM mytable WHERE mytable.x = '89';"
        )

    def test_decorator_with_like(self):
        @clean_sql
        def _dummy() -> str:
            return """
        SELECT *
        FROM mytable
        WHERE mytable.x like '%89%'
        GROUP BY mytable.y;
        """

        self.assertEqual(
            _dummy(),
            "SELECT * FROM mytable WHERE mytable.x like '%89%' GROUP BY mytable.y;",
        )

    def test_decorator_messy(self):
        @clean_sql
        def _dummy() -> str:
            return """
    select           row_number()  OVER (ORDER by per.person_id) as procedure_occurrence_id,


               per.person_id         as person_id,
               coalesce  (lk.concept_id::int    ,       0) as procedure_concept_id,
               NULL as provider_id,
                    p.fallid::float::int as visit_occurrence_id,

               NULL as visit_detail,
                   p.lstid as procedure_source_value,
 NULL      as    procedure_source_concept_id,
               NULL as modifier_source_value
        from partial_procedure_occurrence p
                 join person per
                      on p.persnr = per.person_source_value
                 left join lstid_lookup lk
                        on

                        p.lstid::int
                           =

                           lk.lstid::int
        ;
        """

        expected = "select row_number() OVER (ORDER by per.person_id) as procedure_occurrence_id, "
        expected += "per.person_id as person_id, coalesce (lk.concept_id::int , 0) as procedure_concept_id, "
        expected += "NULL as provider_id, "
        expected += "p.fallid::float::int as visit_occurrence_id, "
        expected += "NULL as visit_detail, "
        expected += "p.lstid as procedure_source_value, "
        expected += "NULL as procedure_source_concept_id, "
        expected += "NULL as modifier_source_value "
        expected += "from partial_procedure_occurrence p "
        expected += "join person per on p.persnr = per.person_source_value left join lstid_lookup lk on "
        expected += "p.lstid::int = lk.lstid::int ;"

        self.assertEqual(_dummy(), expected)

    def test_decorator_with_args(self):
        @clean_sql
        def _dummy(a: str, b: str) -> str:
            return f"""
        SELECT *
        FROM {a}
        WHERE {a}.x = {b};
        """

        self.assertEqual(
            _dummy("mytable", str(89)),
            "SELECT * FROM mytable WHERE mytable.x = 89;",
        )


__all__ = ["CleanSqlUnitTests"]
