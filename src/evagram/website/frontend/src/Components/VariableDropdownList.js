import React from "react";
import { useEffect, useState } from "react";

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
    <div className="flex flex-col">
      {/* styling for variable drop down here */}
      <select
        id={id}
        onChange={(e) => handleChange(e)}
        className="bg-[#cccfd3] bg-opacity-100 m-1.5 flex w-36 rounded-md"
      >
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
          {/* styling for variable drop down here */}
          <div>
            <label className="font-body font-bold text-black">Channel:</label>
            <select
              className="bg-[#cccfd3] bg-opacity-100 shadow-lg m-1.5 flex w-36 rounded-md"
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
