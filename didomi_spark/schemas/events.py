from typing import Dict, Any


class EventSchemas:
    """Centralize schemas used in :obj:`~didomi_spark.jobs.events.EventJob`."""

    @property
    def raw_event(self) -> Dict[str, Any]:
        schema = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "string", "nullable": True, "metadata": {}},
                {"name": "datetime", "type": "string", "nullable": True, "metadata": {}},
                {"name": "type", "type": "string", "nullable": True, "metadata": {}},
                {"name": "domain", "type": "string", "nullable": True, "metadata": {}},
                {
                    "name": "user",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {"name": "id", "type": "string", "nullable": True, "metadata": {}},
                            {"name": "country", "type": "string", "nullable": True, "metadata": {}},
                            {"name": "token", "type": "string", "nullable": True, "metadata": {}},
                        ],
                    },
                    "nullable": True,
                    "metadata": {},
                },
                {"name": "datehour", "type": "string", "nullable": True, "metadata": {}},
            ],
        }
        return schema

    @property
    def event(self) -> Dict[str, Any]:
        schema = {
            "type": "struct",
            "fields": [
                {"name": "event_id", "type": "string", "nullable": True, "metadata": {}},
                {"name": "event_type", "type": "string", "nullable": True, "metadata": {}},
                {"name": "domain", "type": "string", "nullable": True, "metadata": {}},
                {"name": "datehour", "type": "string", "nullable": True, "metadata": {}},
                {"name": "user_id", "type": "string", "nullable": True, "metadata": {}},
                {"name": "user_country", "type": "string", "nullable": True, "metadata": {}},
                {"name": "user_consent", "type": "integer", "nullable": False, "metadata": {}},
            ],
        }
        return schema

    @property
    def token(self) -> str:
        return "vendors struct<enabled: array<string>, disabled: array<string>>, purposes struct<enabled: array<string>, disabled: array<string>>"

    @property
    def events_metrics(self) -> Dict[str, Any]:
        schema = {
            "type": "struct",
            "fields": [
                {"name": "datehour", "type": "string", "nullable": True, "metadata": {}},
                {"name": "domain", "type": "string", "nullable": True, "metadata": {}},
                {"name": "country", "type": "string", "nullable": True, "metadata": {}},
                {"name": "pageviews", "type": "long", "nullable": True, "metadata": {}},
                {"name": "consents_asked", "type": "long", "nullable": True, "metadata": {}},
                {"name": "consents_given", "type": "long", "nullable": True, "metadata": {}},
                {"name": "pageviews_with_consent", "type": "long", "nullable": True, "metadata": {}},
                {"name": "consents_asked_with_consent", "type": "long", "nullable": True, "metadata": {}},
                {"name": "consents_given_with_consent", "type": "long", "nullable": True, "metadata": {}},
                {"name": "avg_pageviews_per_user", "type": "double", "nullable": False, "metadata": {}},
            ],
        }
        return schema
