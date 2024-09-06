import { ReactComponent as NasaLogo } from "./assets/nasa-logo.svg";
import GoddardLogo from "./assets/goddard-logo.png";


function Footer() {
    return ( 
<footer className="bg-footer text-white mt-5 p-6">
  {/* <div className="w-full flex flex-row items-center"> */}
    
    {/* Footer Logo / Brand */}
    <div className=" flex flex-row items-center justify-between">

        <div id="nasa-container" className="flex-grow-0">
            <div id="nasa-logo" className="p-4">
                <a href="https://www.nasa.gov/" class="flex items-center space-x-3">
                    <NasaLogo/>
                </a>
                            
            </div>

        </div>
        
        <div id="goddard-container" className="flex flex-grow ml-3 justify-center ">
            <div id="goddard-logo" className="">
                    <a href="" className="flex items-center space-x-3">
                        <img src={GoddardLogo} width={500} height={120} alt="Goddard Logo" /> {/* Using img tag for PNG */}
                    </a>           
                            
            </div>

            
        </div>
       <div id="footer-text-container" className="flex-grow-0 ml-9">
        <div id="footer-text">
                <ul className="list-disc text-sm text-left mb-5">
                    <li>
                        Privacy Policy & Important Notices
                    </li>
                    <li>
                        Contact Us
                    </li>
                    <li>
                       Page Last Updated: 8/8/2024
                    </li>
                </ul>

                <p className="text-left text-sm">
                    Nasa Official: Alexey Shiklomanov
                   

                </p>
                <p className="text-left text-sm">
                Web Curator: Akira Sewnath & Danya Baron 
                </p>
                <p className="text-left text-sm">
                Web Master: Brandon Zhu 
                </p>

            </div>

       </div>
        
    </div>
    
    {/* </div> */}
</footer>
 );
}

export default Footer;