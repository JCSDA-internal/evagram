import { useState, useEffect, useRef } from "react";
import axios from "axios";
import PlotList from "./PlotList.js";
import DropdownList from "./DropdownList.js";
import VariableDropdownList from "./VariableDropdownList.js";

import { IoMdInformationCircleOutline } from "react-icons/io";

/*
The PlotMenu component contains all the dropdown menus the user interacts with to query
for diagnostics stored in the Evagram database. The interaction works in a cascading
order: the user must select an option in the dropdown from top to bottom. The user can
leave dropdown menus empty or selected to broaden or narrow the diagnostics results.
The component also contains the PlotList component that renders the Bokeh plots by the
constraints.
*/
function PlotMenu() {
  // const dummyOwners = [{key: 1, keyValue: 2  }];

  const [owners, setOwners] = useState([]);
  const [experiments, setExperiments] = useState([]);
  const [cycleTimes, setCycleTimes] = useState([]);
  const [readers, setReaders] = useState([]);
  const [observations, setObservations] = useState([]);
  const [variablesMap, setVariablesMap] = useState(new Map());
  const [groups, setGroups] = useState([]);
  const [plotTypes, setPlotTypes] = useState([]);
  const [selectedOwner, setSelectedOwner] = useState("");
  const [selectedExperiment, setSelectedExperiment] = useState("");
  const [selectedCycleTime, setSelectedCycleTime] = useState("");
  const [selectedReader, setSelectedReader] = useState("");
  const [selectedObservation, setSelectedObservation] = useState("");
  const [selectedVariableName, setSelectedVariableName] = useState("");
  const [selectedChannel, setSelectedChannel] = useState("null");
  const [selectedGroup, setSelectedGroup] = useState("");
  const [selectedPlotType, setSelectedPlotType] = useState("");

  const [observationAlias, setObservationAlias] = useState("Observation");
  const [variableAlias, setVariableAlias] = useState("Variable");
  const [groupAlias, setGroupAlias] = useState("Group");

  const [toggleChannel, setToggleChannel] = useState(false);
  // State to control visibility of expand-dropdowns
  const [isExpandDropdownsVisible, setExpandDropdownsVisible] = useState(false);

  const didMount = useRef(false);

  useEffect(() => {
    axios
      .get("http://localhost:8000/api/initial-load/")
      .then((response) => {
        setOwners(response.data["owners"]);
        setExperiments(response.data["experiments"]);
        didMount.current = true;
      })
      .catch((error) => console.log(error));
  }, []);

  // need another function to open second menu

  const submitForm = (e) => {
    setSelectedOwner(document.getElementById("user_menu").value);
    setSelectedExperiment(document.getElementById("experiment_menu").value);
    setSelectedCycleTime(document.getElementById("cycle_time_menu").value);
    setSelectedReader(document.getElementById("reader_menu").value);
    setSelectedObservation(document.getElementById("observation_menu").value);
    setSelectedVariableName(document.getElementById("variable_menu").value);
    setSelectedChannel(document.getElementById("channel_menu").value);
    setSelectedGroup(document.getElementById("group_menu").value);
    setSelectedPlotType(document.getElementById("plot_type_menu").value);
  };

  const updateOptionsByUser = (e) => {
    setExperiments([]);
    setCycleTimes([]);
    setReaders([]);
    setObservations([]);
    setVariablesMap(new Map());
    setToggleChannel(false);
    setGroups([]);
    setPlotTypes([]);
    if (e.target.value !== "null") {
      axios
        .get("http://localhost:8000/api/update-user-option/", {
          params: {
            owner_id: e.target.value,
          },
        })
        .then((response) => {
          setExperiments(response.data["experiments"]);
        })
        .catch((error) => console.log(error));
    }
  };

  const updateOptionsByExperiment = (e) => {
    setCycleTimes([]);
    setReaders([]);
    setObservations([]);
    setVariablesMap(new Map());
    setToggleChannel(false);
    setGroups([]);
    setPlotTypes([]);
    if (e.target.value !== "null") {
      axios
        .get("http://localhost:8000/api/update-experiment-option/", {
          params: {
            experiment_id: e.target.value,
          },
        })
        .then((response) => {
          setCycleTimes(response.data["cycle_times"]);
        })
        .catch((error) => console.log(error));
    }
  };

  const updateOptionsByCycleTime = (e) => {
    setReaders([]);
    setObservations([]);
    setVariablesMap(new Map());
    setToggleChannel(false);
    setGroups([]);
    setPlotTypes([]);
    if (e.target.value !== "null") {
      axios
        .get("http://localhost:8000/api/update-cycle-time-option/", {
          params: {
            experiment_id: document.getElementById("experiment_menu").value,
            cycle_time: e.target.value,
          },
        })
        .then((response) => {
          setReaders(response.data["readers"]);
        })
        .catch((error) => console.log(error));
    }
  };

  const updateOptionsByReader = (e) => {
    setObservations([]);
    setVariablesMap(new Map());
    setToggleChannel(false);
    setGroups([]);
    setPlotTypes([]);
    updateReaderAliases(e.target.value);
    setExpandDropdownsVisible(true); // Show expand-dropdowns when a reader is selected
    if (e.target.value !== "null") {
      axios
        .get("http://localhost:8000/api/update-reader-option/", {
          params: {
            experiment_id: document.getElementById("experiment_menu").value,
            cycle_time: document.getElementById("cycle_time_menu").value,
            reader_id: e.target.value,
          },
        })
        .then((response) => {
          setObservations(response.data["observations"]);
        })
        .catch((error) => console.log(error));
    }
  };

  const updateOptionsByObservation = (e) => {
    setVariablesMap(new Map());
    setToggleChannel(false);
    setGroups([]);
    setPlotTypes([]);
    if (e.target.value !== "null") {
      axios
        .get("http://localhost:8000/api/update-observation-option/", {
          params: {
            experiment_id: document.getElementById("experiment_menu").value,
            cycle_time: document.getElementById("cycle_time_menu").value,
            reader_id: document.getElementById("reader_menu").value,
            observation_id: e.target.value,
          },
        })
        .then((response) => {
          setVariablesMap(response.data["variablesMap"]);
        })
        .catch((error) => console.log(error));
    }
  };

  const updateOptionsByVariableName = (e) => {
    setToggleChannel(false);
    setGroups([]);
    setPlotTypes([]);
    if (e.target.value !== "null") {
      setToggleChannel(true);
      var channel = "null";
      if (variablesMap[e.target.value][0] !== null) {
        channel = variablesMap[e.target.value][0];
      }
      axios
        .get("http://localhost:8000/api/update-variable-option/", {
          params: {
            experiment_id: document.getElementById("experiment_menu").value,
            cycle_time: document.getElementById("cycle_time_menu").value,
            reader_id: document.getElementById("reader_menu").value,
            observation_id: document.getElementById("observation_menu").value,
            variable_name: e.target.value,
            channel: channel,
          },
        })
        .then((response) => {
          setGroups(response.data["groups"]);
        })
        .catch((error) => console.log(error));
    }
  };

  const updateOptionsByChannel = (e) => {
    setGroups([]);
    setPlotTypes([]);
    if (e.target.value !== "null") {
      axios
        .get("http://localhost:8000/api/update-variable-option/", {
          params: {
            experiment_id: document.getElementById("experiment_menu").value,
            cycle_time: document.getElementById("cycle_time_menu").value,
            reader_id: document.getElementById("reader_menu").value,
            observation_id: document.getElementById("observation_menu").value,
            variable_name: document.getElementById("variable_menu").value,
            channel: e.target.value,
          },
        })
        .then((response) => {
          setGroups(response.data["groups"]);
        })
        .catch((error) => console.log(error));
    }
  };

  const updateOptionsByGroup = (e) => {
    setPlotTypes([]);
    if (e.target.value !== "null") {
      axios
        .get("http://localhost:8000/api/update-group-option/", {
          params: {
            experiment_id: document.getElementById("experiment_menu").value,
            cycle_time: document.getElementById("cycle_time_menu").value,
            reader_id: document.getElementById("reader_menu").value,
            observation_id: document.getElementById("observation_menu").value,
            variable_name: document.getElementById("variable_menu").value,
            channel: document.getElementById("channel_menu").value,
            group_id: e.target.value,
          },
        })
        .then((response) => {
          setPlotTypes(response.data["plot_types"]);
        })
        .catch((error) => console.log(error));
    }
  };

  const updateReaderAliases = (reader_id) => {
    if (reader_id === "null") {
      setObservationAlias("Observation");
      setVariableAlias("Variable");
      setGroupAlias("Group");
    } else {
      axios
        .get("http://localhost:8000/api/get-reader-aliases/", {
          params: {
            reader_id: reader_id,
          },
        })
        .then((response) => {
          setObservationAlias(response.data["observation_name"]);
          setVariableAlias(response.data["variable_name"]);
          setGroupAlias(response.data["group_name"]);
        })
        .catch((error) => console.log(error));
    }
  };

  return (
    // <div id="experiment-header">

    // </div>
    <div id="main-div" className="min-h-screen  flex flex-col">
      <div
        id="header"
        className="flex justify-center  items-center p-5 bg-white shadow-lg relative"
      >
        <header className="font-heading font-bold text-black text-2xl flex items-center">
          Experiment View
        </header>
        <div className="relative group">
          <IoMdInformationCircleOutline className="ml-2 text-2xl cursor-pointer" />
          <div className="absolute left-1/2 transform -translate-x-1/2 mt-2 hidden group-hover:flex flex-row items-center">
            <div className="bg-white w-[376px] shadow-lg text-black font-medium font-body text-sm p-2 rounded-lg">
              Welcome to Experiment View.
              <p className="mb-4 font-body">
                Here you can set your attributes of the experiment you are
                examining, and also bookmark this page to come back to.
              </p>
              <p>
                Pick your fields from the drop downs and the left, and hit
                submit to see the generated plots.
              </p>
            </div>
          </div>
        </div>
      </div>

      <div
        id="outer-container"
        className="mt-5 mb-5 flex flex-col justify-center items-center lg:flex-row "
      >
        <div
          id="menu_container"
          className="mt-5  flex flex-col w-96 justify-center items-center "
        >
          <div id="experiment-details-label" className="p-3">
            <header className="font-heading font-bold text-black text-2xl text-center ">
              Experiment Details
            </header>

            <div id="mandatory-label" className="">
              <header className="font-heading italic font-medium text-black text-sm text-center mt-1 ">
                Fields marked with * are mandatory
              </header>
            </div>
          </div>

          <div
            id="dropdown-container"
            className="mt-3 p-6 w-fit mb-3 flex rounded-md space-x-4 flex-row  bg-white shadow-lg "
          >
            <div id="required-dropdowns" className="w-fit flex flex-col">
              <label className="font-body font-bold text-black">User *</label>
              <DropdownList
                id="user_menu"
                updateOptionCallback={updateOptionsByUser}
                objects={owners}
                nullable={false}
              />
              <label className="font-body font-bold text-black">
                Experiment *
              </label>
              <DropdownList
                id="experiment_menu"
                updateOptionCallback={updateOptionsByExperiment}
                objects={experiments}
              />
              <label className="font-body font-bold text-black">
                Cycle Time *
              </label>
              <DropdownList
                id="cycle_time_menu"
                updateOptionCallback={updateOptionsByCycleTime}
                objects={cycleTimes}
              />
              <label className="font-body font-bold text-black">Reader *</label>
              <DropdownList
                id="reader_menu"
                updateOptionCallback={updateOptionsByReader}
                objects={readers}
              />
            </div>

            {/* static expanded drop down  */}

            {/* Conditionally render expanded drop down */}
            {isExpandDropdownsVisible && (
              <div id="expand-dropdowns" className="w-fit flex flex-col">
                <label className="font-body font-bold text-black">
                  {observationAlias}:
                </label>
                <DropdownList
                  id="observation_menu"
                  updateOptionCallback={updateOptionsByObservation}
                  objects={observations}
                />
                <label className="font-body font-bold text-black">
                  {variableAlias}:
                </label>
                <VariableDropdownList
                  id="variable_menu"
                  updateOptionsByVariableName={updateOptionsByVariableName}
                  updateOptionsByChannel={updateOptionsByChannel}
                  variablesMap={variablesMap}
                  toggleChannel={toggleChannel}
                />
                <label className="font-body font-bold text-black">
                  {groupAlias}:
                </label>
                <DropdownList
                  id="group_menu"
                  updateOptionCallback={updateOptionsByGroup}
                  objects={groups}
                />
                <label className="font-body font-bold text-black">
                  Plot Types:
                </label>
                <DropdownList id="plot_type_menu" objects={plotTypes} />
              </div>
            )}
          </div>

          <button
            className="submitBtn m-5 w-48 h-10 font-heading text-white shadow-lg rounded-md font-bold bg-primary-blue hover:bg-[#17507E] 
          transition ease-in-out duration-300"
            type="submit"
            onClick={(e) => submitForm(e)}
          >
            Submit
          </button>

          <button
            className="m-5 w-48 h-14 font-heading text-white shadow-lg rounded-md font-bold bg-secondary-blue
           hover:bg-[#071C2C] 
          transition ease-in-out duration-300"
          >
            View Another Experiment
          </button>
        </div>

        <PlotList
          owner={selectedOwner}
          experiment={selectedExperiment}
          cycleTime={selectedCycleTime}
          reader={selectedReader}
          observation={selectedObservation}
          variableName={selectedVariableName}
          channel={selectedChannel}
          group={selectedGroup}
          plotType={selectedPlotType}
        />
      </div>
    </div>
  );
}

export default PlotMenu;
