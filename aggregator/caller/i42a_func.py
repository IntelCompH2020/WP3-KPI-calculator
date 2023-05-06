from utils import uf


def i42a_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "ESG data": {"$exists": True},
            }
        },
        {
            "$addFields": {
                "TurnoverNumeric": {"$ifNull": [{"$toDouble": "$Turnover"}, 0]},
                "NumberOfESGdata": {"$size": "$ESG data"},
                "Year": "$ESG data.Year",
            }
        },
        {"$match": {"NumberOfESGdata": {"$ne": 0}, "Turnover": {"$ne": None}}},
        {
            "$group": {
                "_id": field,
                "count": {
                    "$push": {
                        "company_name": "$company_name",
                        "TurnoverNumeric": "$TurnoverNumeric",
                        "Year": "$Year",
                    }
                },
            }
        },
    ]


def ind_caller(enco, results):
    results["i42a"] = {}
    results["i42a"]["sv00"] = uf.top_companies(enco, ["all"], i42a_aggregation, 10)
    return results
