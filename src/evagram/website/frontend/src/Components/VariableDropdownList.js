import React from "react";
import { useEffect, useState } from "react";
import styles from "../styles/VariableDropdownList.module.css";

function VariableDropdownList({
    id,
    updateOptionsByVariableName,
    updateOptionsByChannel,
    variablesMap,
    toggleChannel,
}) {
    const [variableName, setVariableName] = useState("--");

    // console.log("Variables Map", variablesMap, variableName);

    const handleChange = (e) => {
        updateOptionsByVariableName(e);
        toggleVariableName();
    };

    const toggleVariableName = () => {
        var variableMenu = document.getElementById(id);
        setVariableName(variableMenu.options[variableMenu.selectedIndex].text);
    };

    useEffect(() => {
        var variableMenu = document.getElementById(id);
        setVariableName(variableMenu.options[variableMenu.selectedIndex].text);
    }, [id, variablesMap]);

    return (
        <div className={styles.variable_dropdown}>
            <select id={id} onChange={(e) => handleChange(e)}>
                <option value="null">--</option>
                {Object.keys(variablesMap).length > 0
                    ? Object.keys(variablesMap).map((variable) => (
                          <option key={variable} value={variable}>
                              {variable}
                          </option>
                      ))
                    : null}
            </select>
            {toggleChannel && variablesMap[variableName][0] !== null ? (
                <>
                    <div className={styles.variable_dropdown}>
                        {/* <label>Channel:</label> */}
                        <select
                            id="channel_menu"
                            onChange={updateOptionsByChannel}
                        >
                            {variablesMap[variableName].map((channel) => (
                                <option key={channel} value={channel}>
                                    {channel}
                                </option>
                            ))}
                        </select>
                    </div>
                </>
            ) : (
                <input type="hidden" id="channel_menu" value={"null"} />
            )}
        </div>
    );
}

export default React.memo(VariableDropdownList);
