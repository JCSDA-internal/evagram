import { useEffect, useState } from "react";
import axios from "axios";
import Plot from "./Plot.js";

function PlotList({ owner, experiment, group, observation, variable }) {
    const [plots, setPlots] = useState([]);

    useEffect(() => {
        if (
            owner !== "" &&
            experiment !== "" &&
            group !== "" &&
            observation !== "" &&
            variable !== ""
        ) {
            axios
                .get("http://localhost:8000/api/get-plots-by-field/", {
                    params: {
                        owner_id: owner,
                        experiment_id: experiment,
                        group_id: group,
                        observation_id: observation,
                        variable_id: variable,
                    },
                })
                .then((response) => {
                    setPlots(response.data);
                });
        }
    }, [owner, experiment, group, observation, variable]);
    return (
        <div>
            {plots.map((plot) => (
                <Plot key={plot.plot_id} div={plot.div} script={plot.script} />
            ))}
        </div>
    );
}

export default PlotList;
