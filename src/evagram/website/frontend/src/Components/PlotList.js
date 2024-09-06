import React from "react";
import { useEffect, useState } from "react";
import axios from "axios";

//icons and static images
import plot1 from "./assets/plot1.png";
import plot2 from "./assets/plot2.png";
import plot3 from "./assets/plot3.png";
import { RiArrowRightSLine, RiArrowLeftSLine } from "react-icons/ri";
import Plot from "./Plot";

/*
The PlotList component interacts with the backend to request for Bokeh plot data in a list format
using the attributes provided below as request parameters. The component gives the ability to render
multiple plots by performing a map on the plot list from the backend to render multiple Plot components.
*/
function PlotList({
  owner,
  experiment,
  cycleTime,
  reader,
  observation,
  variableName,
  channel,
  group,
  plotType,
}) {
  const [plots, setPlots] = useState([]);

  // array of static carousel images

  const imageList = [
    {
      id: 1,
      src: plot1,
      alt: "Image 1",
    },
    {
      id: 2,
      src: plot2,
      alt: "Image 2",
    },
    {
      id: 3,
      src: plot3,
      alt: "Image 3",
    },
  ];

  // Carousel Logic

  // index for carousel images
  const [currentIndex, setCurrentIndex] = useState(0);

  //logic for slider arrows

  //logic for previous slide button
  const prevSlide = () => {
    const isFirstPlot = currentIndex === 0;
    const newIndex = isFirstPlot ? plots.length - 1 : currentIndex - 1;
    setCurrentIndex(newIndex);
  };
  //logic for next slide button
  const nextSlide = () => {
    const isLastPlot = currentIndex === plots.length - 1;
    const newIndex = isLastPlot ? 0 : currentIndex + 1;
    setCurrentIndex(newIndex);
  };

  useEffect(() => {
    if (
      owner !== "" &&
      experiment !== "" &&
      cycleTime !== "" &&
      reader !== "" &&
      observation !== "" &&
      variableName !== "" &&
      channel !== "" &&
      group !== "" &&
      plotType !== ""
    ) {
      axios
        .get("http://localhost:8000/api/get-plots-by-field/", {
          params: {
            owner_id: owner,
            experiment_id: experiment,
            cycle_time: cycleTime,
            reader_id: reader,
            observation_id: observation,
            variable_name: variableName,
            channel: channel,
            group_id: group,
            plot_type: plotType,
          },
        })
        .then((response) => {
          setPlots(response.data);
        });
    }
  }, [
    owner,
    experiment,
    cycleTime,
    reader,
    observation,
    variableName,
    channel,
    group,
    plotType,
  ]);
  return (
    // carousel menu

    
      <div id="carousel" className=" flex w-fit flex-col ">
        {/* <div id="plot-header" className="p-5">
          <header className="font-heading font-bold text-black text-2xl text-center ">
            Plot View
          </header>
        </div> */}
        {plots.length > 0 && (
          <div
            id="image-container"
            className="w-full flex flex-col m-9 items-center p-5"
          >
            <div className="duration-300">
              <Plot
                div={plots[currentIndex].div}
                script={plots[currentIndex].script}
              ></Plot>
            </div>

            <div id="arrow-container" className="flex flex-row p-5 space-x-4">
              <button
                className="text-2xl p-2 rounded-full bg-primary-blue/20 text-black cursor-pointer hover:bg-secondary-blue
            transition ease-in-out duration-300"
                onClick={prevSlide}
              >
                <RiArrowLeftSLine
                  size={40}
                  className="hover:text-white transition ease-in-out duration-300"
                />
              </button>
              <div
                className="right-5 text-2xl p-2 rounded-full bg-primary-blue/20 text-black cursor-pointer hover:bg-secondary-blue
            transition ease-in-out duration-300"
                onClick={nextSlide}
              >
                <RiArrowRightSLine
                  size={40}
                  className="hover:text-white transition ease-in-out duration-300"
                />
              </div>
            </div>
            <div id="diagnostics-text">
              <header className="font-body font-medium text-base">
                Showing {currentIndex + 1} out of {plots.length} diagnostics
              </header>
            </div>
            <div id="scroll-text" className="p-2">
              <header className="font-body italic font-medium text-sm">
                Click on arrow buttons to scroll through diagnostics
              </header>
            </div>
          </div>
        )}
      </div>
    
  );
}

export default React.memo(PlotList);
