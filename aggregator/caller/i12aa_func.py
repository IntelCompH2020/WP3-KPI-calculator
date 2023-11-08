from utils import uf

def i12a_aggregation(field, extra_aggr_param):
    if field == "all":
        return extra_aggr_param + [{"$group": {"_id": None, "count": {"$sum": 1}}}]
    else:
        return extra_aggr_param + [
            {"$group": {"_id": "$" + field, "count": {"$sum": 1}}}
        ]
    

def i12a_aggregation_nace(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$match": {"nace": {"$exists": True, "$not": {"$size": 0}}}},
        {
            "$group": {
                "_id": ["$nace.nace2_code", "$nace.description"],
                "count": {"$sum": 1},
            }
        },
    ]


def i12a_aggregation_cpc(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$match": {"cpc_labels": {"$exists": True, "$not": {"$size": 0}}}},
        {
            "$group": {
                "_id": ["$cpc_labels.code", "$cpc_labels.description"],
                "count": {"$sum": 1},
            }
        }
    ]


def i12a_aggregation_forward(field, extra_aggr_param):
    if field == "all":
        return extra_aggr_param + [
            {"$match": {"citations": {"$exists": True, "$not": {"$size": 0}}}},
            {"$group": {"_id": None, "count": {"$sum": "$citations.forward"}}},
        ]
    else:
        return extra_aggr_param + [
            {"$match": {"citations": {"$exists": True, "$not": {"$size": 0}}}},
            {"$group": {"_id": "$" + field, "count": {"$sum": "$citations.forward"}}},
        ]


def i12a_aggregation_forward_nace(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "nace": {"$exists": True, "$not": {"$size": 0}},
                "citations": {"$exists": True, "$not": {"$size": 0}},
            }
        },
        {
            "$group": {
                "_id": ["$nace.nace2_code", "$nace.description"],
                "count": {"$sum": "$citations.forward"},
            }
        },
    ]


def i12a_aggregation_forward_cpc(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "cpc_labels": {"$exists": True, "$not": {"$size": 0}},
                "citations": {"$exists": True, "$not": {"$size": 0}},
            }
        },
        {
            "$group": {
                "_id": ["$cpc_labels.code", "$cpc_labels.description"],
                "count": {"$sum": "$citations.forward"},
            }
        },
    ]


def i12a_aggregation_backward(field, extra_aggr_param):
    if field == "all":
        return extra_aggr_param + [
            {"$match": {"citations": {"$exists": True, "$not": {"$size": 0}}}},
            {"$group": {"_id": None, "count": {"$sum": "$citations.backward"}}},
        ]
    else:
        return extra_aggr_param + [
            {"$match": {"citations": {"$exists": True, "$not": {"$size": 0}}}},
            {"$group": {"_id": "$" + field, "count": {"$sum": "$citations.backward"}}},
        ]


def i12a_aggregation_backward_nace(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "nace": {"$exists": True, "$not": {"$size": 0}},
                "citations": {"$exists": True, "$not": {"$size": 0}},
            }
        },
        {
            "$group": {
                "_id": ["$nace.nace2_code", "$nace.description"],
                "count": {"$sum": "$citations.backward"},
            }
        },
    ]


def i12a_aggregation_backward_ipc(field, extra_aggr_param):
    return extra_aggr_param + [
        {
            "$match": {
                "cpc_labels": {"$exists": True, "$not": {"$size": 0}},
                "citations": {"$exists": True, "$not": {"$size": 0}},
            }
        },
        {
            "$group": {
                "_id": ["$cpc_labels.code", "$cpc_labels.description"],
                "count": {"$sum": "$citations.backward"},
            }
        },
    ]



def ind_caller(pat, results, logging, extra_aggr_param=[], working_path=""):
    results["i12a"] = {}

    # try:
    #     numerator_FC = uf.secondary_view(
    #         pat, "all", i12a_aggregation_forward, extra_aggr_param
    #     ).pop(None)
    #     numerator_BC = uf.secondary_view(
    #         pat, "all", i12a_aggregation_backward, extra_aggr_param
    #     ).pop(None)
    #     denominator = uf.secondary_view(
    #         pat, "all", i12a_aggregation, extra_aggr_param
    #     ).pop(None)

    #     average_FC = numerator_FC/denominator
    #     average_BC = numerator_BC/denominator

    #     results = {"i12a": {"sv00": {'adopter': {},
    #                                 'enablers': {},
    #                                 'pioneers': {},
    #                                 'mavericks': {},
    #                                 'unclassified': {}}}}
    #     params = extra_aggr_param[0]['$match']
    #     params['citations'] = {"$exists": True, "$not": {"$size": 0}}

    #     # Initializing count for each category for year k
    #     for category in results["i12a"]["sv00"].keys():
    #         results["i12a"]["sv00"][category] = 0

    #     for doc in pat.find(params):
    #         FC = doc["citations"]["forward"]
    #         BC = doc["citations"]["backward"]
            
    #         # Classify based on FC, BC, average_FC, and average_BC
    #         if average_FC >= FC and average_BC <= BC:
    #             category = 'adopter'
    #         elif average_FC <= FC and average_BC < BC:
    #             category = 'enablers'
    #         elif average_FC < FC and average_BC >= BC:
    #             category = 'pioneers'
    #         elif average_FC > FC and average_BC > BC:
    #             category = 'mavericks'
    #         else:
    #             category = 'unclassified'
            
    #         # Incrementing count
    #         results["i12a"]["sv00"][category]["average_citations"] += 1
    #     print(results)
            
    # except Exception as e:
    #     results["i12a"]["sv00"] = None
    #     logging.error(f"Error calculating i12a[sv00]: {str(e)}")

    try:
        numerator_FC = uf.secondary_view(
            pat, "appln_filing_year", i12a_aggregation_forward, extra_aggr_param
        )
        numerator_BC = uf.secondary_view(
            pat, "appln_filing_year", i12a_aggregation_backward, extra_aggr_param
        )
        denominator = uf.secondary_view(
            pat, "appln_filing_year", i12a_aggregation, extra_aggr_param
        )
        # Remove keys where the denominator is zero
        denominator = {k: v for k, v in denominator.items() if v != 0}
        average_FC = {}
        average_BC = {}
        results = {"i12a": {"sv01": {'adopter': {},
                                    'enablers': {},
                                    'pioneers': {},
                                    'mavericks': {},
                                    'unclassified': {}}}}
        params = extra_aggr_param[0]['$match']
        params['citations'] = {"$exists": True, "$not": {"$size": 0}}

        for k in numerator_FC.keys():
            # If key is not present in the denominator, skip the calculation
            if k not in denominator:
                continue
            params["appln_filing_year"] = k
            average_FC[k] = numerator_FC[k] / denominator[k]
            average_BC[k] = numerator_BC[k] / denominator[k]

            # Initializing count for each category for year k
            for category in results["i12a"]["sv01"].keys():
                results["i12a"]["sv01"][category][k] = 0
                
            for doc in pat.find(params):
                FC = doc["citations"]["forward"]
                BC = doc["citations"]["backward"]
                
                # Classify based on FC, BC, average_FC, and average_BC
                if average_FC[k] >= FC and average_BC[k] <= BC:
                    category = 'adopter'
                elif average_FC[k] <= FC and average_BC[k] < BC:
                    category = 'enablers'
                elif average_FC[k] < FC and average_BC[k] >= BC:
                    category = 'pioneers'
                elif average_FC[k] > FC and average_BC[k] > BC:
                    category = 'mavericks'
                else:
                    category = 'unclassified'
                
                # Incrementing count
                results["i12a"]["sv01"][category][k] += 1
                
    except Exception as e:
        results["i12a"]["sv01"] = None
        print(f"Error calculating i12a_sv01: {str(e)}")


    # try:
    #     numerator_FC = uf.secondary_view(
    #         pat, "topic", i12a_aggregation_forward, extra_aggr_param
    #     )
    #     numerator_BC = uf.secondary_view(
    #         pat, "topic", i12a_aggregation_backward, extra_aggr_param
    #     )
    #     denominator = uf.secondary_view(
    #         pat, "topic", i12a_aggregation, extra_aggr_param
    #     )
    #     # Remove keys where the denominator is zero
    #     denominator = {k: v for k, v in denominator.items() if v != 0}
    #     average_FC = {}
    #     average_BC = {}
    #     results = {"i12a": {"sv02": {'adopter': {},
    #                                 'enablers': {},
    #                                 'pioneers': {},
    #                                 'mavericks': {},
    #                                 'unclassified': {}}}}
    #     params = extra_aggr_param[0]['$match']
    #     params['citations'] = {"$exists": True, "$not": {"$size": 0}}
        
    #     for k in numerator_FC.keys():
    #         # If key is not present in the denominator, skip the calculation
    #         if k not in denominator:
    #             continue
    #         params["topic"] = k
    #         average_FC[k] = numerator_FC[k] / denominator[k]
    #         average_BC[k] = numerator_BC[k] / denominator[k]
            
    #         # Initializing count for each category for year k
    #         for category in results["i12a"]["sv02"].keys():
    #             results["i12a"]["sv02"][category][k] = 0
            
    #         for doc in pat.find(params):
    #             FC = doc["citations"]["forward"]
    #             BC = doc["citations"]["backward"]
                
    #             # Classify based on FC, BC, average_FC, and average_BC
    #             if average_FC[k] >= FC and average_BC[k] <= BC:
    #                 category = 'adopter'
    #             elif average_FC[k] <= FC and average_BC[k] < BC:
    #                 category = 'enablers'
    #             elif average_FC[k] < FC and average_BC[k] >= BC:
    #                 category = 'pioneers'
    #             elif average_FC[k] > FC and average_BC[k] > BC:
    #                 category = 'mavericks'
    #             else:
    #                 category = 'unclassified'
                
    #             # Incrementing count
    #             results["i12a"]["sv02"][category][k] += 1
    #     print(results)
    # except Exception as e:
    #     results["i12a"]["sv02"] = None
    #     print(f"Error calculating i12a[sv02]: {str(e)}")

    # try:
    #     numerator_FC = uf.inner_secondary_view(
    #         pat, "participant.name", i12a_aggregation_forward, extra_aggr_param
    #     )
    #     numerator_BC = uf.inner_secondary_view(
    #         pat, "participant.name", i12a_aggregation_backward, extra_aggr_param
    #     )
    #     denominator = uf.inner_secondary_view(
    #         pat, "participant.name", i12a_aggregation, extra_aggr_param
    #     )
    #     # Remove keys where the denominator is zero
    #     denominator = {k: v for k, v in denominator.items() if v != 0}
    #     average_FC = {}
    #     average_BC = {}
    #     results = {"i12a": {"sv06": {'adopter': {},
    #                                 'enablers': {},
    #                                 'pioneers': {},
    #                                 'mavericks': {},
    #                                 'unclassified': {}}}}
    #     params = extra_aggr_param[0]['$match']
    #     params['citations'] = {"$exists": True, "$not": {"$size": 0}}
        
    #     for k in numerator_FC.keys():
    #         # If key is not present in the denominator, skip the calculation
    #         if k not in denominator:
    #             continue
    #         params["participant"]["name"] = k
    #         average_FC[k] = numerator_FC[k] / denominator[k]
    #         average_BC[k] = numerator_BC[k] / denominator[k]
            
    #         # Initializing count for each category for year k
    #         for category in results["i12a"]["sv06"].keys():
    #             results["i12a"]["sv06"][category][k] = 0
            
    #         for doc in pat.find(params):
    #             FC = doc["citations"]["forward"]
    #             BC = doc["citations"]["backward"]
                
    #             # Classify based on FC, BC, average_FC, and average_BC
    #             if average_FC[k] >= FC and average_BC[k] <= BC:
    #                 category = 'adopter'
    #             elif average_FC[k] <= FC and average_BC[k] < BC:
    #                 category = 'enablers'
    #             elif average_FC[k] < FC and average_BC[k] >= BC:
    #                 category = 'pioneers'
    #             elif average_FC[k] > FC and average_BC[k] > BC:
    #                 category = 'mavericks'
    #             else:
    #                 category = 'unclassified'
                
    #             # Incrementing count
    #             results["i12a"]["sv06"][category][k] += 1
    #     print(results)
    # except Exception as e:
    #     results["i12a"]["sv06"] = None
    #     print(f"Error calculating i12a[sv06]: {str(e)}")

    # try:
    #     numerator_FC = uf.inner_secondary_view_nace_cpc(
    #         pat, "nace.nace2_code", i12a_aggregation_forward_nace, extra_aggr_param
    #     )
    #     numerator_BC = uf.inner_secondary_view_nace_cpc(
    #         pat, "nace.nace2_code", i12a_aggregation_backward_nace, extra_aggr_param
    #     )
    #     denominator = uf.inner_secondary_view_nace_cpc(
    #         pat, "nace.nace2_code", i12a_aggregation_nace, extra_aggr_param
    #     )
    #     # Remove keys where the denominator is zero
    #     denominator = {k: v for k, v in denominator.items() if v != 0}
    #     average_FC = {}
    #     average_BC = {}
    #     results = {"i12a": {"sv07": {'adopter': {},
    #                                 'enablers': {},
    #                                 'pioneers': {},
    #                                 'mavericks': {},
    #                                 'unclassified': {}}}}
    #     params = extra_aggr_param[0]['$match']
    #     params['citations'] = {"$exists": True, "$not": {"$size": 0}}
    #     params['nace'] = {"$exists": True, "$not": {"$size": 0}}
        
    #     for k in numerator_FC.keys():
    #         # If key is not present in the denominator, skip the calculation
    #         if k not in denominator:
    #             continue
    #         params["nace.nace2_code"] = k
    #         average_FC[k] = numerator_FC[k] / denominator[k]
    #         average_BC[k] = numerator_BC[k] / denominator[k]
            
    #         # Initializing count for each category for year k
    #         for category in results["i12a"]["sv07"].keys():
    #             results["i12a"]["sv07"][category][k] = 0
            
    #         for doc in pat.find(params):
    #             FC = doc["citations"]["forward"]
    #             BC = doc["citations"]["backward"]
                
    #             # Classify based on FC, BC, average_FC, and average_BC
    #             if average_FC[k] >= FC and average_BC[k] <= BC:
    #                 category = 'adopter'
    #             elif average_FC[k] <= FC and average_BC[k] < BC:
    #                 category = 'enablers'
    #             elif average_FC[k] < FC and average_BC[k] >= BC:
    #                 category = 'pioneers'
    #             elif average_FC[k] > FC and average_BC[k] > BC:
    #                 category = 'mavericks'
    #             else:
    #                 category = 'unclassified'
                
    #             # Incrementing count
    #             results["i12a"]["sv07"][category][k] += 1
    #     print(results)
    # except Exception as e:
    #     results["i12a"]["sv07"] = None
    #     print(f"Error calculating i12a[sv07]: {str(e)}")

    # try:
    #     numerator_FC = uf.inner_secondary_view(
    #         pat, "participant.sector", i12a_aggregation_forward, extra_aggr_param
    #     )
    #     numerator_BC = uf.inner_secondary_view(
    #         pat, "participant.sector", i12a_aggregation_backward, extra_aggr_param
    #     )
    #     denominator = uf.inner_secondary_view(
    #         pat, "participant.sector", i12a_aggregation, extra_aggr_param
    #     )
    #     # Remove keys where the denominator is zero
    #     denominator = {k: v for k, v in denominator.items() if v != 0}
    #     average_FC = {}
    #     average_BC = {}
    #     results = {"i12a": {"sv08": {'adopter': {},
    #                                 'enablers': {},
    #                                 'pioneers': {},
    #                                 'mavericks': {},
    #                                 'unclassified': {}}}}
    #     params = extra_aggr_param[0]['$match']
    #     params['citations'] = {"$exists": True, "$not": {"$size": 0}}

    #     for k in numerator_FC.keys():
    #         # If key is not present in the denominator, skip the calculation
    #         if k not in denominator:
    #             continue
    #         params["participant.sector"] = k
    #         average_FC[k] = numerator_FC[k] / denominator[k]
    #         average_BC[k] = numerator_BC[k] / denominator[k]
            
    #         # Initializing count for each category for year k
    #         for category in results["i12a"]["sv08"].keys():
    #             results["i12a"]["sv08"][category][k] = 0
            
    #         for doc in pat.find(params):
    #             FC = doc["citations"]["forward"]
    #             BC = doc["citations"]["backward"]
                
    #             # Classify based on FC, BC, average_FC, and average_BC
    #             if average_FC[k] >= FC and average_BC[k] <= BC:
    #                 category = 'adopter'
    #             elif average_FC[k] <= FC and average_BC[k] < BC:
    #                 category = 'enablers'
    #             elif average_FC[k] < FC and average_BC[k] >= BC:
    #                 category = 'pioneers'
    #             elif average_FC[k] > FC and average_BC[k] > BC:
    #                 category = 'mavericks'
    #             else:
    #                 category = 'unclassified'
                
    #             # Incrementing count
    #             results["i12a"]["sv08"][category][k] += 1
    #     print(results)
    # except Exception as e:
    #     results["i12a"]["sv08"] = None
    #     print(f"Error calculating i12a[sv08]: {str(e)}")

    # try:
    #     numerator_FC = uf.inner_secondary_view(
    #         pat, "participant.country", i12a_aggregation_forward, extra_aggr_param
    #     )
    #     numerator_BC = uf.inner_secondary_view(
    #         pat, "participant.country", i12a_aggregation_backward, extra_aggr_param
    #     )
    #     denominator = uf.inner_secondary_view(
    #         pat, "participant.country", i12a_aggregation, extra_aggr_param
    #     )
    #     # Remove keys where the denominator is zero
    #     denominator = {k: v for k, v in denominator.items() if v != 0}
    #     average_FC = {}
    #     average_BC = {}
    #     results = {"i12a": {"sv09": {'adopter': {},
    #                                 'enablers': {},
    #                                 'pioneers': {},
    #                                 'mavericks': {},
    #                                 'unclassified': {}}}}
    #     params = extra_aggr_param[0]['$match']
    #     params['citations'] = {"$exists": True, "$not": {"$size": 0}}
        
    #     for k in numerator_FC.keys():
    #         # If key is not present in the denominator, skip the calculation
    #         if k not in denominator:
    #             continue
    #         params["participant.country"] = k
    #         average_FC[k] = numerator_FC[k] / denominator[k]
    #         average_BC[k] = numerator_BC[k] / denominator[k]
            
    #         # Initializing count for each category for year k
    #         for category in results["i12a"]["sv09"].keys():
    #             results["i12a"]["sv09"][category][k] = 0
            
    #         for doc in pat.find(params):
    #             FC = doc["citations"]["forward"]
    #             BC = doc["citations"]["backward"]
                
    #             # Classify based on FC, BC, average_FC, and average_BC
    #             if average_FC[k] >= FC and average_BC[k] <= BC:
    #                 category = 'adopter'
    #             elif average_FC[k] <= FC and average_BC[k] < BC:
    #                 category = 'enablers'
    #             elif average_FC[k] < FC and average_BC[k] >= BC:
    #                 category = 'pioneers'
    #             elif average_FC[k] > FC and average_BC[k] > BC:
    #                 category = 'mavericks'
    #             else:
    #                 category = 'unclassified'
                
    #             # Incrementing count
    #             results["i12a"]["sv09"][category][k] += 1
    #     print(results)
    # except Exception as e:
    #     results["i12a"]["sv09"] = None
    #     print(f"Error calculating i12a[sv09]: {str(e)}")

    # try:
    #     numerator_FC = uf.inner_secondary_view_nace_cpc(
    #         pat, "ipc.ipc_class", i12a_aggregation_forward_cpc, extra_aggr_param
    #     )
    #     numerator_BC = uf.inner_secondary_view_nace_cpc(
    #         pat, "ipc.ipc_class", i12a_aggregation_backward_ipc, extra_aggr_param
    #     )
    #     denominator = uf.inner_secondary_view_nace_cpc(
    #         pat, "ipc.ipc_class", i12a_aggregation_cpc, extra_aggr_param
    #     )
    #     # Remove keys where the denominator is zero
    #     denominator = {k: v for k, v in denominator.items() if v != 0}
    #     average_FC = {}
    #     average_BC = {}
    #     results = {"i12a": {"sv13": {'adopter': {},
    #                                 'enablers': {},
    #                                 'pioneers': {},
    #                                 'mavericks': {},
    #                                 'unclassified': {}}}}
    #     params = extra_aggr_param[0]['$match']
    #     params['citations'] = {"$exists": True, "$not": {"$size": 0}}
    #     params['ipc'] = {"$exists": True, "$not": {"$size": 0}}
        
    #     for k in numerator_FC.keys():
    #         # If key is not present in the denominator, skip the calculation
    #         if k not in denominator:
    #             continue
    #         params["ipc.ipc_class"] = k
    #         average_FC[k] = numerator_FC[k] / denominator[k]
    #         average_BC[k] = numerator_BC[k] / denominator[k]
            
    #         # Initializing count for each category for year k
    #         for category in results["i12a"]["sv13"].keys():
    #             results["i12a"]["sv13"][category][k] = 0
            
    #         for doc in pat.find(params):
    #             FC = doc["citations"]["forward"]
    #             BC = doc["citations"]["backward"]
                
    #             # Classify based on FC, BC, average_FC, and average_BC
    #             if average_FC[k] >= FC and average_BC[k] <= BC:
    #                 category = 'adopter'
    #             elif average_FC[k] <= FC and average_BC[k] < BC:
    #                 category = 'enablers'
    #             elif average_FC[k] < FC and average_BC[k] >= BC:
    #                 category = 'pioneers'
    #             elif average_FC[k] > FC and average_BC[k] > BC:
    #                 category = 'mavericks'
    #             else:
    #                 category = 'unclassified'
                
    #             # Incrementing count
    #             results["i12a"]["sv13"][category][k] += 1
    #     print(results)
    # except Exception as e:
    #     results["i12a"]["sv13"] = None
    #     print(f"Error calculating i12a[sv13]: {str(e)}")

    return results