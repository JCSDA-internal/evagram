import React from "react";
import { useEffect, useState } from "react";
import styles from "../styles/VariableDropdownList.module.css";

/*
The VariableDropdownList is similar to the DropdownList component but is used exclusively for the
variable dropdown menu. The component contains two dropdown menus: one for variable name and the 
other for channel. The user must select a variable name before a channel and not all variable names
come with a channel.
*/
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

  // updates the variableName state variable to the variable name selected in the variable menu dropdown
  const toggleVariableName = () => {
    var variableMenu = document.getElementById(id);
    setVariableName(variableMenu.options[variableMenu.selectedIndex].text);
  };

  useEffect(() => {
    toggleVariableName();
  }, [id, variablesMap, toggleVariableName]);

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
            <select id="channel_menu" onChange={updateOptionsByChannel}>
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
