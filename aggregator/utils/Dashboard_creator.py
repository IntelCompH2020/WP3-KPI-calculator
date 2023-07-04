from pathlib import Path
import json


def create_json(job_id, job, indicators):
    script_dir = Path(__file__).resolve().parent.parent
    with open(script_dir.joinpath("config/mapping.json")) as j:
        ind_to_graph = json.loads(j.read())

    chartGroups = []
    for indid, svids in indicators.items():
        charts = []
        for svid in svids:
            try:
                with open(script_dir.joinpath("graphs/" + f"{indid}_{svid}.json")) as j:
                    charts.append(json.loads(j.read()))
            except Exception as e:
                print(f"{str(e)}")

        chartGroups.append({"name": f"{ind_to_graph[indid]}", "charts": charts})

    data = {
        "id": job_id,
        "tabs": [
            {
                "name": job,
                "chartGroups": chartGroups,
            }
        ],
    }

    data_str = json.dumps(data)

    return data_str
