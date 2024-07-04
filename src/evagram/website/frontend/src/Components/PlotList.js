import React from "react";
import { useEffect, useState } from "react";
import axios from "axios";
import Plot from "./Plot.js";

/*
The PlotList component interacts with the backend to request for Bokeh plot data in a list format
using the attributes provided below as request parameters. The component gives the ability to render
multiple plots by performing a map on the plot list from the backend to render multiple Plot components.
*/
function PlotList({
  owner,
  experiment,
  observation,
  variableName,
  channel,
  group,
}) {
  const [plots, setPlots] = useState([]);

  useEffect(() => {
    if (
      owner !== "" &&
      experiment !== "" &&
      observation !== "" &&
      variableName !== "" &&
      channel !== "" &&
      group !== ""
    ) {
      axios
        .get("http://localhost:8000/api/get-plots-by-field/", {
          params: {
            owner_id: owner,
            experiment_id: experiment,
            observation_id: observation,
            variable_name: variableName,
            channel: channel,
            group_id: group,
          },
        })
        .then((response) => {
          setPlots(response.data);
        });
    }
  }, [owner, experiment, observation, variableName, channel, group]);
  return (
    <div>
      {plots.map((plot) => (
        <Plot key={plot.plot_id} div={plot.div} script={plot.script} />
      ))}
    </div>
  );
}

export default React.memo(PlotList);
