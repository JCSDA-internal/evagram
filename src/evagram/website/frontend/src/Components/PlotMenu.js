import styles from "../styles/PlotMenu.module.css";
import { useState, useEffect, useRef } from "react";
import axios from "axios";
import PlotList from "./PlotList.js";

function PlotMenu() {
    const [owners, setOwners] = useState([]);
    const [groups, setGroups] = useState([]);
    const [experiments, setExperiments] = useState([]);
    const [observations, setObservations] = useState([]);
    const [variables, setVariables] = useState([]);
    const [currentOwner, setCurrentOwner] = useState("");
    const [currentExperiment, setCurrentExperiment] = useState("");
    const [currentGroup, setCurrentGroup] = useState("");
    const [currentObservation, setCurrentObservation] = useState("");
    const [currentVariable, setCurrentVariable] = useState("");

    const didMount = useRef(false);

    useEffect(() => {
        axios
            .get("http://localhost:8000/api/initial-load/")
            .then((response) => {
                setOwners(response.data["owners"]);
                setExperiments(response.data["experiments"]);
                // setGroups(response.data["groups"]);
                // setObservations(response.data["observations"]);
                // setVariables(response.data["variables"]);
                didMount.current = true;
            })
            .catch((error) => console.log(error));
    }, []);

    const submitForm = (e) => {
        setCurrentOwner(document.getElementById("user_menu").value);
        setCurrentExperiment(document.getElementById("experiment_menu").value);
        setCurrentGroup(document.getElementById("group_menu").value);
        setCurrentObservation(
            document.getElementById("observation_menu").value
        );
        setCurrentVariable(document.getElementById("variable_menu").value);
    };

    const updateOptionsByUser = (e) => {
        // setCurrentExperiment(""); // sets state to empty until all data is fetched
        setExperiments([]);
        setObservations([]);
        setVariables([]);
        setGroups([]);
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
    };

    const updateOptionsByExperiment = (e) => {
        if (e.target.value === "placeholder") {
            setObservations([]);
            setVariables([]);
            setGroups([]);
        } else {
            //setCurrentGroup("");
            axios
                .get("http://localhost:8000/api/update-experiment-option/", {
                    params: {
                        experiment_id: e.target.value,
                    },
                })
                .then((response) => {
                    setObservations(response.data["observations"]);
                })
                .catch((error) => console.log(error));
        }
    };

    const updateOptionsByObservation = (e) => {
        if (e.target.value === "placeholder") {
            setVariables([]);
            setGroups([]);
        } else {
            //setCurrentVariable("");
            axios
                .get("http://localhost:8000/api/update-observation-option/", {
                    params: {
                        observation_id: e.target.value,
                    },
                })
                .then((response) => {
                    setVariables(response.data["variables"]);
                })
                .catch((error) => console.log(error));
        }
    };

    const updateOptionsByVariable = (e) => {
        if (e.target.value === "placeholder") {
            setGroups([]);
        } else {
            //setCurrentGroup("");
            axios
                .get("http://localhost:8000/api/update-variable-option/", {
                    params: {
                        variable_id: e.target.value,
                    },
                })
                .then((response) => {
                    setGroups(response.data["groups"]);
                })
                .catch((error) => console.log(error));
        }
    };

    return (
        <div id="menu_container" className={styles.menu_container}>
            <div className={styles.dropdown_container}>
                <label htmlFor="user_menu">User:</label>
                <select id="user_menu" onChange={(e) => updateOptionsByUser(e)}>
                    {owners.map((owner) =>
                        owner.owner_id === currentOwner ? (
                            <option
                                key={owner.owner_id}
                                value={owner.owner_id}
                                selected
                            >
                                {owner.first_name + " " + owner.last_name}
                            </option>
                        ) : (
                            <option key={owner.owner_id} value={owner.owner_id}>
                                {owner.first_name + " " + owner.last_name}
                            </option>
                        )
                    )}
                </select>
                <label htmlFor="experiment_menu">Experiment:</label>
                <select
                    id="experiment_menu"
                    onChange={(e) => updateOptionsByExperiment(e)}
                >
                    <option value="placeholder">-- select an option --</option>
                    {experiments.map((experiment) =>
                        experiment.experiment_id === currentExperiment ? (
                            <option
                                key={experiment.experiment_id}
                                value={experiment.experiment_id}
                                selected
                            >
                                {experiment.experiment_name}
                            </option>
                        ) : (
                            <option
                                key={experiment.experiment_id}
                                value={experiment.experiment_id}
                            >
                                {experiment.experiment_name}
                            </option>
                        )
                    )}
                </select>
                <label htmlFor="observation_menu">Observation:</label>
                <select
                    id="observation_menu"
                    onChange={(e) => updateOptionsByObservation(e)}
                >
                    <option value="placeholder">-- select an option --</option>
                    {observations.map((observation) =>
                        observation.observation_id === currentObservation ? (
                            <option
                                key={observation.observation_id}
                                value={observation.observation_id}
                                selected
                            >
                                {observation.observation_name}
                            </option>
                        ) : (
                            <option
                                key={observation.observation_id}
                                value={observation.observation_id}
                            >
                                {observation.observation_name}
                            </option>
                        )
                    )}
                </select>
                <label htmlFor="variable_menu">Variable:</label>
                <select
                    id="variable_menu"
                    onChange={(e) => updateOptionsByVariable(e)}
                >
                    <option value="placeholder">-- select an option --</option>
                    {variables.map((variable) =>
                        variable.variable_id === currentVariable ? (
                            variable.channel ? (
                                <option
                                    key={variable.variable_id}
                                    value={variable.variable_id}
                                    selected
                                >
                                    {variable.variable_name + variable.channel}
                                </option>
                            ) : (
                                <option
                                    key={variable.variable_id}
                                    value={variable.variable_id}
                                    selected
                                >
                                    {variable.variable_name}
                                </option>
                            )
                        ) : variable.channel ? (
                            <option
                                key={variable.variable_id}
                                value={variable.variable_id}
                            >
                                {variable.variable_name + variable.channel}
                            </option>
                        ) : (
                            <option
                                key={variable.variable_id}
                                value={variable.variable_id}
                            >
                                {variable.variable_name}
                            </option>
                        )
                    )}
                </select>
                <label htmlFor="group_menu">Group:</label>
                <select id="group_menu">
                    <option value="placeholder">-- select an option --</option>
                    {groups.map((group) =>
                        group.group_id === currentGroup ? (
                            <option
                                key={group.group_id}
                                value={group.group_id}
                                selected
                            >
                                {group.group_name}
                            </option>
                        ) : (
                            <option key={group.group_id} value={group.group_id}>
                                {group.group_name}
                            </option>
                        )
                    )}
                </select>
                <button id="submit" onClick={(e) => submitForm(e)}>
                    Submit
                </button>
            </div>
            <div className={styles.plot}>
                <PlotList
                    owner={currentOwner}
                    experiment={currentExperiment}
                    group={currentGroup}
                    observation={currentObservation}
                    variable={currentVariable}
                />
            </div>
        </div>
    );
}

export default PlotMenu;
