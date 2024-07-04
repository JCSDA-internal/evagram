import React from "react";

/* 
DropdownList has three properties:
- id: DOM Element identifier
- updateOptionCallback: function that updates PlotMenu dropdowns when option is selected
- objects: options displayed in the dropdown with three attributes: key, value, and content
*/
function DropdownList({ id, updateOptionCallback, objects }) {
  return (
    <select id={id} onChange={updateOptionCallback}>
      <option value="null">--</option>
      {objects.map((object) => (
        <option key={object.key} value={object.value}>
          {object.content}
        </option>
      ))}
    </select>
  );
}

export default React.memo(DropdownList);
