from utils import uf


def i42_aggregation(field, extra_aggr_param):
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
                    }
                },
            }
        },
    ]


def ind_caller(enco, results):
    results["i42"] = {}
    results["i42"]["sv00"] = uf.top_companies(enco, ["all"], i42_aggregation, 10)
    return results
