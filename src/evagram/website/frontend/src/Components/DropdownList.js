import React from "react";

/* 
DropdownList has three properties:
- id: DOM Element identifier
- updateOptionCallback: function that updates PlotMenu dropdowns when option is selected
- objects: options displayed in the dropdown with three attributes: key, value, and content
*/
function DropdownList({ id, updateOptionCallback, objects, nullable = true }) {
  return (
    <select
      id={id}
      onChange={updateOptionCallback}
      className="bg-[#cccfd3] w-36 bg-opacity-100 shadow-lg m-1.5 mb-1 rounded-md"
    >
      {nullable ? <option value="null">--</option> : null}

      {objects.map((object) => (
        <option key={object.key} value={object.value}>
          {object.content}
        </option>
      ))}
    </select>
  );
}

export default React.memo(DropdownList);
