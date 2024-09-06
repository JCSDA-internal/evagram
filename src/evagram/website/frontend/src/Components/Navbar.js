import {FaBars, FaTimes} from "react-icons/fa";
import { ReactComponent as NasaLogo } from "./assets/nasa-logo.svg";
import { IoMdInformationCircleOutline } from "react-icons/io";


function Navbar() {
    return ( 
     
        <nav class="bg-primary-blue h-28 flex items-center w-full max-w-screen relative top-0 justify-between shadow-lg">
            <div class="max-w-screen flex items-center w-full justify-between">
                <div class="flex items-center space-x-4">
                   {/* nav bar header logo */}
                    <div id="nasa-logo" className="ml-2">
                            <a href="https://www.nasa.gov/" class="flex items-center space-x-3">
                                <NasaLogo/>
                            </a>
                        
                    </div>
                {/* nav bar header text */}
                    <div id="evagram-subheading text-wrap w-34">
                        <header class="text-2xl text-left font-heading font-bold text-white">Evagram</header>
                        <h2 class="text-md text-left font-heading font-mediun text-white">Global Modelling and Assimilation Office</h2>
                    </div>
                </div>
                
            {/* nav bar navigation buttons here */}
            <ul className="flex items-center space-x-4 mr-7">
                <li>
                    <a href="/#" className="bg-white text-black shadow-lg rounded px-2 py-1 text-md font-heading 
                    font-semibold w-24 hover:text-white hover:bg-black hover:bg-opacity-40 transition ease-in-out duration-300
                    active:bg-secondary-blue active:text-white">Home</a>
                </li>
                <li>
                    <a href="/#" className="bg-white text-black shadow-lg rounded px-2 py-1 text-md font-heading 
                    font-semibold w-24 hover:text-white hover:bg-black hover:bg-opacity-40 transition ease-in-out duration-300
                    active:bg-secondary-blue active:text-white">Experiment</a>
                </li>
                <li>
                    <a href="/#" className="bg-white text-black shadow-lg rounded px-2 py-1 text-md font-heading 
                    font-semibold w-24 hover:text-white hover:bg-black hover:bg-opacity-40 transition ease-in-out duration-300
                    active:bg-secondary-blue active:text-white">About</a>
                </li>
            </ul>


            </div>
          </nav> 
        
     );
}

export default Navbar;