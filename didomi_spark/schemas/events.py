from typing import Dict, Any


class EventSchemas:
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
                {"name": "id", "type": "string", "nullable": True, "metadata": {}},
                {"name": "type", "type": "string", "nullable": True, "metadata": {}},
                {"name": "domain", "type": "string", "nullable": True, "metadata": {}},
                {"name": "datehour", "type": "string", "nullable": True, "metadata": {}},
                {"name": "user_id", "type": "string", "nullable": True, "metadata": {}},
                {"name": "user_country", "type": "string", "nullable": True, "metadata": {}},
                {"name": "user_consent", "type": "string", "nullable": True, "metadata": {}},
            ],
        }
        return schema

    @property
    def token(self) -> str:
        return "vendors STRUCT<enabled: ARRAY<STRING>, disabled: ARRAY<STRING>>, purposes STRUCT<enabled: ARRAY<STRING>, disabled: ARRAY<STRING>>"
