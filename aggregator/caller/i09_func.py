from utils import uf
import copy


def i09_aggregation(field, extra_aggr_param):
    return extra_aggr_param + [{"$group": {"_id": "$" + field, "count": {"$sum": 1}}}]


def i09_aggregation_per_year(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$group": {"_id": ["$appln_filing_year", "$" + field], "count": {"$sum": 1}}}
    ]


def i09_aggregation_per_year_nace(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$match": {"nace": {"$exists": True, "$not": {"$size": 0}}}},
        {
            "$group": {
                "_id": ["$appln_filing_year", "$nace.nace2_code"],
                "count": {"$sum": 1},
            }
        },
    ]


def i09_aggregation_per_year_cpc(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$match": {"cpc_labels": {"$exists": True, "$not": {"$size": 0}}}},
        {
            "$group": {
                "_id": [
                    "$appln_filing_year",
                    "$cpc_labels.code",
                ],
                "count": {"$sum": 1},
            }
        },
    ]

def i09_aggregation_per_year_nace(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$match": {"nace": {"$exists": True, "$not": {"$size": 0}}}},
        {
            "$group": {
                "_id": "$nace.nace2_code",
                "count": {"$sum": 1},
            }
        },
    ]
def i09_aggregation_per_year_cpc(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$match": {"cpc_labels": {"$exists": True, "$not": {"$size": 0}}}},
        {
            "$group": {
                "_id": 
                    "$cpc_labels.code",
                "count": {"$sum": 1},
            }
        },
    ]

def ind_caller(pat, results, logging, extra_aggr_param=[], working_path=""):
    results["i09"] = {}
    
    try:
        results["i09"]["sv01"] = uf.secondary_view(
            pat, "appln_filing_year", i09_aggregation, copy.deepcopy(extra_aggr_param)
        )
    except Exception as e:
        results["i09"]["sv01"] = None
        print(f"Error calculating i09[sv01]: {str(e)}")
        logging.error(f"Error calculating i09[sv01]: {str(e)}")
    
    try:
        results["i09"]["sv02"] = uf.secondary_view_per_year(
            pat, "topic", i09_aggregation_per_year, copy.deepcopy(extra_aggr_param), first_year=2000
        )
    except Exception as e:
        results["i09"]["sv02"] = None
        print(f"Error calculating i09[sv01]: {str(e)}")
        logging.error(f"Error calculating i09[sv02]: {str(e)}")

    try:
        results["i09"]["sv03"] = uf.secondary_view_per_year(
            pat, "category", i09_aggregation_per_year, copy.deepcopy(extra_aggr_param), first_year=2000
        )
    except Exception as e:
        results["i09"]["sv03"] = None
        print(f"Error calculating i09[sv01]: {str(e)}")
        logging.error(f"Error calculating i09[sv03]: {str(e)}")

    try:
        results["i09"]["sv06"] = uf.inner_secondary_view_per_year(
            pat, "participant.name", i09_aggregation_per_year, copy.deepcopy(extra_aggr_param), first_year=2000
        )
    except Exception as e:
        results["i09"]["sv06"] = None
        print(f"Error calculating i09[sv01]: {str(e)}")
        logging.error(f"Error calculating i09[sv06]: {str(e)}")

    try:
        results["i09"]["sv07"] = uf.inner_secondary_view_per_year_nace_cpc(
            pat, ["nace.nace2_code", "nace.description"], i09_aggregation_per_year_nace, copy.deepcopy(extra_aggr_param), first_year=2000)
    except Exception as e:
        results["i09"]["sv07"] = None
        logging.error(f"Error calculating i09[sv07]: {str(e)}")

    try:
        results["i09"]["sv08"] = uf.inner_secondary_view_per_year(
            pat, "participant.sector", i09_aggregation_per_year, copy.deepcopy(extra_aggr_param), first_year=2000
        )
    except Exception as e:
        results["i09"]["sv08"] = None
        print(f"Error calculating i09[sv01]: {str(e)}")
        logging.error(f"Error calculating i09[sv08]: {str(e)}")

    try:
        full_set = uf.inner_secondary_view_per_year(
            pat, "participant.country", i09_aggregation_per_year, copy.deepcopy(extra_aggr_param), first_year=2000
        )
        results["i09"]["sv09"] = {}
        for k in full_set.keys():
            if k in uf.eu_members_code:
                results["i09"]["sv09"][
                    uf.eu_members[uf.eu_members_code.index(k)]
                ] = full_set[k]
    except Exception as e:
        results["i09"]["sv09"] = None
        print(f"Error calculating i09[sv01]: {str(e)}")
        logging.error(f"Error calculating i09[sv09]: {str(e)}")

    try:
        results["i09"]["sv13"] = uf.inner_secondary_view_per_year_nace_cpc(
            pat, ["cpc_labels.code", "cpc_labels.description"], i09_aggregation_per_year_cpc, copy.deepcopy(extra_aggr_param), first_year=2000)
    except Exception as e:
        results["i09"]["sv13"] = None
        print(f"Error calculating i09[sv01]: {str(e)}")
        logging.error(f"Error calculating i09[sv13]: {str(e)}")

    return results
