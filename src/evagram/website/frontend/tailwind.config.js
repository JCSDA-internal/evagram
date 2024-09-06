/** @type {import('tailwindcss').Config} */

module.exports = {
  content: [
    "./src/**/*.{js,jsx,ts,tsx}",

  ],
  theme: {
    extend: {},
    colors: {
      'primary-blue': '#1C75BC',
      'secondary-blue': '#0E3453',
      'black': "#071013",
      'white': '#F4F7F9',
      'bg-blue': '#E0E7ED',
      'dropdown-gray':'#EDEEEF',
      'footer': '#434343'
    },
    fontFamily: {
      heading: ['Inter', 'sans-serif'],
      body: ['Public Sans', 'sans-serif']
      
    }
  },
  plugins: [],
}
