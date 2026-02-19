import { createTheme } from '@mui/material/styles'

/**
 * Amadeus-like palette (safe approximation).
 * You can override from CSS variables later if you have a brand token system.
 */
const AMADEUS_BLUE = '#005EB8'  // primary
const AMADEUS_BLUE_DARK = '#003B73'
const AMADEUS_ACCENT = '#00A1DE'
const AMADEUS_GREY = '#F5F7FA'

const theme = createTheme({
  palette: {
    mode: 'light',
    primary: { main: AMADEUS_BLUE, dark: AMADEUS_BLUE_DARK },
    secondary: { main: AMADEUS_ACCENT },
    background: { default: AMADEUS_GREY }
  },
  typography: {
    fontFamily: 'Inter, system-ui, "Segoe UI", Roboto, Arial, sans-serif',
    h4: { fontWeight: 700 },
    h6: { fontWeight: 600 }
  },
  components: {
    MuiAppBar: {
      styleOverrides: { root: { boxShadow: 'none', borderBottom: '1px solid #E6ECF3' } }
    },
    MuiPaper: {
      styleOverrides: {
        root: { borderRadius: 10, border: '1px solid #E6ECF3' }
      }
    },
    MuiButton: {
      styleOverrides: { root: { textTransform: 'none', fontWeight: 600 } }
    }
  }
})

export default theme