import { useEffect, useRef } from "react";
import * as DOMPurify from "dompurify";

function Plot({ div, script }) {
    const scriptComponent = document.createElement("script");
    const bokehContainerRef = useRef(null);

    useEffect(() => {
        if (!bokehContainerRef.current) return;
        const divContainer = bokehContainerRef.current;
        const divElement = document.createElement("div");
        const divContents = DOMPurify.sanitize(div);
        divElement.innerHTML = divContents;
        bokehContainerRef.current.appendChild(divElement);

        const scriptContents = DOMPurify.sanitize(
            script.replace(/(<([^>]+)>)/gi, "")
        );
        scriptComponent.innerHTML = scriptContents;
        document.body.appendChild(scriptComponent);

        return () => {
            divContainer.removeChild(divElement);
            document.body.removeChild(scriptComponent);
        };
    }, [div, script]);

    return <div ref={bokehContainerRef}></div>;
}

export default Plot;
