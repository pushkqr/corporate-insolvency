from nicegui import ui

def apply_global_styles():
    # Force dark mode globally using NiceGUI's built-in tool
    ui.dark_mode().enable()
    
    # Inject Google Fonts and Global CSS variables for Trading Desk Dark Theme
    ui.add_head_html("""
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Roboto+Mono:wght@400;500&display=swap" rel="stylesheet">
        
        <style>
            :root {
                --terminal-bg: #0d0d0d;
                --card-bg: #1a1a1a;
                --card-border: #333333;
                --text-main: #f3f4f6;
                --text-muted: #9ca3af;
                --accent-primary: #2563eb;
                --accent-success: #10b981;
                --accent-danger: #ef4444;
            }

            body {
                font-family: 'Inter', sans-serif !important;
                background-color: var(--terminal-bg) !important;
                color: var(--text-main) !important;
                margin: 0;
                min-height: 100vh;
            }
            
            /* Utilitarian Cards */
            .trading-card {
                background-color: var(--card-bg) !important;
                border: 1px solid var(--card-border) !important;
                border-radius: 4px !important;
                box-shadow: none !important;
                transition: border-color 0.2s ease !important;
            }

            .trading-card:hover {
                border-color: #555555 !important;
            }

            /* Inputs */
            .trading-input .q-field__control {
                background-color: #000000 !important;
                border: 1px solid var(--card-border) !important;
                border-radius: 4px !important;
                box-shadow: none !important;
                padding: 0 12px !important;
            }

            .trading-input .q-field__control:hover {
                border-color: #555555 !important;
            }
            
            .trading-input .q-field__native {
                color: var(--text-main) !important;
                font-family: 'Roboto Mono', monospace !important;
            }

            /* Remove quasar's default field outline */
            .q-field--outlined .q-field__control:before,
            .q-field--outlined .q-field__control:after {
                border: none !important;
            }

            /* Typography */
            h1, h2, h3 {
                font-weight: 600 !important;
                color: #ffffff !important;
                letter-spacing: -0.01em;
                margin-bottom: 0.5em;
            }
            
            .mono-text {
                font-family: 'Roboto Mono', monospace !important;
            }

            /* Utilitarian Buttons */
            .q-btn {
                background-color: var(--accent-primary) !important;
                color: #ffffff !important;
                border-radius: 4px !important;
                font-weight: 500 !important;
                text-transform: uppercase !important;
                letter-spacing: 0.05em !important;
                border: none !important;
                box-shadow: none !important;
                transition: background-color 0.2s ease !important;
            }
            .q-btn:hover {
                background-color: #1d4ed8 !important;
            }
            
            /* Secondary Button Override */
            .btn-secondary {
                background-color: #374151 !important;
            }
            .btn-secondary:hover {
                background-color: #4b5563 !important;
            }

            /* Loader styling */
            #custom-loader {
                position: fixed; top: 0; left: 0; width: 100vw; height: 100vh;
                background: rgba(13, 13, 13, 0.9);
                z-index: 99999;
                display: none; flex-direction: column;
                justify-content: center; align-items: center;
            }
            
            #custom-loader .loader-container {
                background-color: var(--card-bg);
                border: 1px solid var(--card-border);
                padding: 30px 50px; border-radius: 4px;
                display: flex; flex-direction: column; align-items: center;
            }
            
            #custom-loader .loader {
                width: 40px; height: 40px;
                border: 3px solid #333333;
                border-top: 3px solid var(--accent-primary);
                border-radius: 50%;
                animation: spin 1s linear infinite; 
                margin-bottom: 20px;
            }
            
            #custom-loader .loader-text { 
                font-family: 'Roboto Mono', monospace;
                font-size: 1em; 
                color: var(--text-main); 
            }
            
            @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
        </style>
    """)

    # Inject the HTML for the loader directly into the body
    ui.add_body_html("""
        <div id="custom-loader">
            <div class="loader-container">
                <div class="loader"></div>
                <div class="loader-text">EXECUTING PIPELINE...</div>
            </div>
        </div>
    """)

def show_loader():
    ui.run_javascript("document.getElementById('custom-loader').style.display = 'flex';")

def hide_loader():
    ui.run_javascript("document.getElementById('custom-loader').style.display = 'none';")
