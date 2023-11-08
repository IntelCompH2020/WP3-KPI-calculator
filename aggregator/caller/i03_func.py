from utils import uf

def i03_aggregation_per_year(field, extra_aggr_param):
    return extra_aggr_param + [
        {"$group": {"_id": ["$pub_year", "$" + field], "count": {"$sum": 1}}}
        # { "$unwind": "$topic" },  
        # { "$group": { "_id": {"topic": "$topic", "year": "$pub_year"}, "count": { "$sum": 1 } } },
        # { "$project": { "topic": "$_id.topic", "year": "$_id.year", "count": 1, "_id": 0 } }
    ]
def ind_caller(sci, results, logging, extra_aggr_param=[], working_path=""):
    results["i03"] = {}

    try:
        res = uf.inner_secondary_view_per_year_science(
            sci,
            "topic",
            i03_aggregation_per_year,
            extra_aggr_param,
            first_year=2014,
            final_year=2021,
        )
        
        new_res = {}
        for key, sub_dict in res.items():
            new_sub_dict = {k: v for k, v in sub_dict.items() if v != 0}
            new_res[key] = new_sub_dict
            
        total = {}
        for topic in new_res.keys():
            for i in new_res[topic].keys():
                if i not in total.keys():
                    total[i] = new_res[topic][i]
                else:
                    total[i] += new_res[topic][i]

        for topic in new_res.keys():
            for i in new_res[topic].keys():
                new_res[topic][i] /= total[i]

        results["i03"]["sv02"] = new_res

    except Exception as e:
        results["i03"]["sv02"] = None
        print(f"Error calculating sv02: {str(e)}")

    return results
