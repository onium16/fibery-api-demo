# Set constants data for Fibery

SUPPORTED_FIELD_TYPES = {
    'text': 'fibery/text',
    'int': 'fibery/int',
    'uuid': 'fibery/uuid',
    'date-time': 'fibery/date-time',
    'float': 'fibery/decimal',  # Updated mapping
    'boolean': 'fibery/bool'
}

FIBERY_FIELD_GENERAL =  [{
                      "fibery/name": "fibery/id",
                      "fibery/type": "fibery/uuid",
                      "fibery/meta": {
                        "fibery/secured?": False,
                        "fibery/id?": True,
                        "fibery/readonly?": True
                      }
                    },
                    {
                      "fibery/name": "fibery/public-id",
                      "fibery/type": "fibery/text",
                      "fibery/meta": {
                        "fibery/secured?": False,
                        "fibery/public-id?": True,
                        "fibery/readonly?": True
                      }
                    },
                    {
                      "fibery/name": "fibery/creation-date",
                      "fibery/type": "fibery/date-time",
                      "fibery/meta": {
                        "fibery/secured?": False,
                        "fibery/creation-date?": True,
                        "fibery/readonly?": True,
                        "fibery/default-value": "$now"
                      }
                    },
                    {
                      "fibery/name": "fibery/modification-date",
                      "fibery/type": "fibery/date-time",
                      "fibery/meta": {
                        "fibery/modification-date?": True,
                        "fibery/required?": True,
                        "fibery/readonly?": True,
                        "fibery/default-value": "$now",
                        "fibery/secured?": False
                      }
                    }]
