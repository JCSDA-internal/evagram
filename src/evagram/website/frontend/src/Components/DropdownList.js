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

export default DropdownList;
