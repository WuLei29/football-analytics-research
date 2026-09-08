/** Tailwind v4 is a PostCSS plugin; there is no tailwind.config.ts.
 *  The design tokens live in CSS, in app/globals.css (@theme). */
const config = {
  plugins: {
    "@tailwindcss/postcss": {},
  },
};

export default config;
