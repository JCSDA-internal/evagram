import { useEffect, useState } from "react";
import styles from "../styles/VariableDropdownList.module.css";

function VariableDropdownList({
    id,
    updateOptionsByVariableName,
    updateOptionsByChannel,
    variablesMap,
}) {
    const [variableName, setVariableName] = useState("--");

    // console.log("Variables Map", variablesMap.size);

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
    }, [variablesMap]);

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
            {variableName !== "--" && Object.keys(variablesMap).length > 0 ? (
                <>
                    <div className={styles.variable_dropdown}>
                        <label>Channel:</label>
                        <select
                            id="channel_menu"
                            onChange={updateOptionsByChannel}
                        >
                            {variableName in variablesMap ? (
                                variablesMap[variableName].map((channel) => (
                                    <option key={channel} value={channel}>
                                        {channel}
                                    </option>
                                ))
                            ) : (
                                <input
                                    type="hidden"
                                    id="channel_menu"
                                    value={"null"}
                                />
                            )}
                        </select>
                    </div>
                </>
            ) : (
                <input type="hidden" id="channel_menu" value={"null"} />
            )}
        </div>
    );
}

export default VariableDropdownList;
